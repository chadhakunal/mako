#pragma once

#include "__dep__.h"
#include "constants.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declare Marshal and Marshallable
namespace rrr {
class Marshal;
class Marshallable;
}

namespace janus {

//=============================================================================
// Execution Status (simplified for Option D - no speculative execution)
//
// With agreement-before-execution:
// - Single-shard: INIT -> DIRECT -> COMPLETE
// - Multi-shard: INIT -> (wait for agreement) -> DIRECT -> COMPLETE
//
// We don't need SPEC, ROLLBACK, REPOSITIONED since we don't execute
// speculatively.
//=============================================================================
enum LuigiExecStatus {
  LUIGI_EXEC_INIT = 1,     // Not started yet
  LUIGI_EXEC_DIRECT = 2,   // Executing (agreement done for multi-shard)
  LUIGI_EXEC_COMPLETE = 3, // Execution finished
  LUIGI_EXEC_ABANDONED = 4 // Abandoned due to error
};

//=============================================================================
// Agreement Status (for multi-shard timestamp agreement)
// Aligned with Tiga's AGREE_STATUS enum from TigaMessage.h
//=============================================================================
enum LuigiAgreeStatus {
  LUIGI_AGREE_INIT = 1, // Not started (Tiga: AGREE_INIT)

  LUIGI_AGREE_PENDING = 8, // Agreement initiated, waiting for responses

  // When my proposed_ts != agreed_ts:
  LUIGI_AGREE_FLUSHING =
      2, // I used smaller ts -> reposition in queue (Tiga: AGREE_FLUSHING)

  // When my proposed_ts == agreed_ts but others differ:
  LUIGI_AGREE_CONFIRMING =
      3, // I used agreed_ts, waiting for others (Tiga: AGREE_CONFIRMING)

  LUIGI_AGREE_COMPLETE = 4, // All agreed, can commit (Tiga: AGREE_COMPLETE)

  // Debug/tracking states (like Tiga's AGREE_CHECK1/2/3)
  LUIGI_AGREE_CHECK1 = 5, // Single-shard fast path
  LUIGI_AGREE_CHECK2 = 6, // Multi-shard, all matched
  LUIGI_AGREE_CHECK3 = 7  // Multi-shard, completed via agreement
};

//=============================================================================
// Operation Types
//=============================================================================
constexpr uint8_t LUIGI_OP_READ = 0;
constexpr uint8_t LUIGI_OP_WRITE = 1;

//=============================================================================
// PHASE 4: Watermark Entry for Paxos Replication
// Simple Marshallable wrapper for watermark values
//=============================================================================
/**
 * WatermarkEntry: Minimal serializable struct for Paxos replication.
 * Contains just the watermark timestamp for durability.
 * This is much simpler than making the entire LuigiLogEntry Marshallable.
 */
struct WatermarkEntry {
  uint64_t timestamp_;     // Watermark timestamp
  uint32_t worker_id_;     // Which worker stream this belongs to
  txnid_t txn_id_;         // Transaction ID (for debugging)

  WatermarkEntry() : timestamp_(0), worker_id_(0), txn_id_(0) {}
  WatermarkEntry(uint64_t ts, uint32_t wid, txnid_t tid)
      : timestamp_(ts), worker_id_(wid), txn_id_(tid) {}
};

//=============================================================================
// LuigiOp: A single read or write operation within a transaction
//=============================================================================
struct LuigiOp {
  uint16_t table_id = 0; // Which table
  uint8_t op_type = 0;   // 0 = read, 1 = write
  std::string key;       // Key bytes
  std::string value;     // Value bytes (for writes) or result (for reads)
  bool executed = false; // Has this op been executed?
};

//=============================================================================
// LuigiLogEntry: Container for one transaction as it flows through Luigi
// Semantics aligned with Tiga's TigaLogEntry, with more intuitive naming
//=============================================================================
struct LuigiLogEntry {
  //--- Timestamps ---
  uint64_t proposed_ts_ = 0; // Proposed execution timestamp (send_time + bound)
  uint64_t agreed_ts_ =
      0; // Final agreed timestamp (after multi-shard agreement)
  uint64_t dequeue_ts_ =
      0; // For debug: timestamp when dequeued from priority queue

  //--- Worker ID (for per-worker replication streams) ---
  uint32_t worker_id_ = 0; // Which worker/client thread generated this txn

  //--- Transaction type (for stored-procedure style execution) ---
  uint32_t txn_type_ =
      0; // Transaction type (e.g., NEW_ORDER=0, PAYMENT=1, etc.)

  //--- Status flags (atomic for thread-safe reads) ---
  std::atomic<uint32_t> prev_agree_status_{
      LUIGI_AGREE_INIT}; // Previous agree status
  std::atomic<uint32_t> agree_status_{
      LUIGI_AGREE_INIT}; // Current agreement status
  std::atomic<uint32_t> exec_status_{LUIGI_EXEC_INIT}; // Execution status

  //--- Agreement tracking ---
  std::atomic<bool> ts_agreed_{false};   // All shards agreed on timestamp?
  std::atomic<bool> exec_agreed_{false}; // Execution outcome agreed?
  uint32_t requeue_count_ =
      0; // How many times re-queued (for Case 3 repositioning)

  //--- Transaction identity ---
  txnid_t tid_ = 0; // Unique transaction ID
  std::shared_ptr<Marshallable> cmd_ =
      nullptr; // Command payload (for deptran-style)

  //--- Operations (parsed from request) ---
  std::vector<LuigiOp> ops_; // Read and write operations (legacy)

  //--- TPC-C transaction parameters (Tiga-style) ---
  std::map<int32_t, std::string>
      working_set_; // Transaction parameters (e.g., w_id, d_id, items)

  //--- Callback to return result to coordinator ---
  std::function<void(int status, uint64_t commit_ts,
                     const std::vector<std::string> &read_results)>
      reply_cb_ = nullptr;
  std::atomic<bool> awaiting_reply_{false}; // Waiting for commit reply?

  //--- Keys touched by this txn ---
  std::vector<int32_t>
      local_keys_; // Keys on THIS shard (for conflict detection)
  // shard_id => keys touched on that shard (from coordinator)
  std::map<uint32_t, std::set<int32_t>> shard_to_keys_;

  //--- Timing info ---
  uint64_t send_time_ = 0; // When coordinator sent the txn
  uint32_t owd_ = 0;       // One-way delay (microseconds)
  uint32_t bound_ = 0;     // Bound parameter from coordinator

  //--- For multi-shard txns: which shards are involved ---
  std::set<uint32_t> involved_shards_; // All partitions this txn touches
  std::vector<uint32_t>
      remote_shards_; // Remote partitions (for leader agreement)

  //--- Log/Replication Related ---
  uint32_t log_id_ = 0;      // Only synced log entries have log_id
  uint32_t spec_log_id_ = 0; // Only speculatively executed entries have this

  //--- Reply status and mutex ---
  std::atomic<uint32_t> reply_status_{0};
  std::mutex reply_mutex_;

  //--- Result storage ---
  TxnOutput output_;
  std::vector<std::string> read_results_;

  //--- Constructor ---
  LuigiLogEntry(txnid_t tid = 0)
      : tid_(tid), proposed_ts_(0), agreed_ts_(0), dequeue_ts_(0), txn_type_(0),
        requeue_count_(0), prev_agree_status_(LUIGI_AGREE_INIT),
        agree_status_(LUIGI_AGREE_INIT), exec_status_(LUIGI_EXEC_INIT),
        ts_agreed_(false), exec_agreed_(false), awaiting_reply_(false),
        send_time_(0), owd_(0), bound_(0), log_id_(0), spec_log_id_(0),
        reply_status_(0) {}

  //--- Helper: Is this a multi-shard transaction? ---
  bool IsMultiShard() const { return shard_to_keys_.size() > 1; }

  //--- Helper: Get number of shards ---
  uint32_t NumShards() const { return shard_to_keys_.size(); }

  //--- Helper: Unique ID string ---
  std::string ID() const { return "|" + std::to_string(tid_) + "|"; }

  //--- Helper: Debug string ---
  std::string DebugString() const {
    return "LuigiEntry[tid=" + std::to_string(tid_) +
           ", proposed_ts=" + std::to_string(proposed_ts_) +
           ", agreed_ts=" + std::to_string(agreed_ts_) +
           ", ops=" + std::to_string(ops_.size()) +
           ", keys=" + std::to_string(local_keys_.size()) +
           ", shards=" + std::to_string(NumShards()) +
           ", agree=" + std::to_string(agree_status_.load()) +
           ", exec=" + std::to_string(exec_status_.load()) + "]";
  }
};

} // namespace janus
