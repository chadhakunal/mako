#pragma once

#include "../scheduler.h"
#include "../tx.h"
#include "../concurrentqueue.h"  // moodycamel lock-free queue (same as Tiga uses)
#include "luigi_entry.h"
#include "luigi_executor.h"

#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

// Forward declaration - the actual type is defined in benchmarks/abstract_ordered_index.h
class abstract_ordered_index;

namespace janus {

/**
 * SchedulerLuigi: Tiga-style timestamp-ordered execution for Mako.
 *
 * Key differences from vanilla Tiga:
 * - Only leaders receive txns (no follower fast-path)
 * - Replication happens AFTER execution (Mako's existing replication)
 * - We keep Mako's speculative execution style
 *
 * Flow:
 * 1. Coordinator calls LuigiDispatch() with (txn, send_time, bound)
 * 2. Entry goes into incoming_txn_queue_ (lock-free queue)
 * 3. HoldReleaseTd() picks it up, does conflict detection:
 *    - If txn.deadline > max(lastReleasedDeadline[key] for all keys): accept into priority_queue_
 *    - Else: update txn.deadline = max + 1 (leader can update timestamp)
 * 4. When current_time >= deadline, release from priority_queue_ to ready_txn_queue_
 * 5. ExecTd() executes and triggers timestamp agreement for multi-shard txns
 */
class SchedulerLuigi : public SchedulerClassic {
 public:
  SchedulerLuigi();
  virtual ~SchedulerLuigi();

  // Start background threads
  void Start();
  void Stop();

  // Entry point for a Luigi-style dispatch from raw request buffer.
  // This is called by server.cc's HandleLuigiDispatch.
  // Parses the request, creates a LuigiLogEntry, and enqueues it.
  void LuigiDispatchFromRequest(
      uint64_t txn_id,
      uint64_t send_time,
      uint32_t bound,
      const std::vector<LuigiOp>& ops,
      std::function<void(int status, uint64_t commit_ts, const std::vector<std::string>& read_results)> reply_cb);

  // Original entry point (kept for compatibility with deptran-style calls)
  void LuigiDispatch(txnid_t tx_id,
                     std::shared_ptr<Marshallable> cmd,
                     uint64_t send_time,
                     uint32_t bound,
                     const std::vector<int32_t>& local_keys,
                     std::function<void(const TxnOutput&)> reply_cb);

  // Requeue a txn after agreement determines it needs repositioning (Case 3)
  // This is called by the executor when AGREE_FLUSHING is needed
  void RequeueForReposition(std::shared_ptr<LuigiLogEntry> entry);

 protected:
  // Threads
  void HoldReleaseTd();
  void ExecTd();

  // Helpers
  uint64_t GetMicrosecondTimestamp();

  //==========================================================================
  // INCOMING TXN QUEUE (lock-free)
  // New txns from coordinator land here. HoldReleaseTd() consumes them.
  // Think of this as: "txns waiting to be checked for conflicts"
  //==========================================================================
  moodycamel::ConcurrentQueue<std::shared_ptr<LuigiLogEntry>> incoming_txn_queue_;

  //==========================================================================
  // PRIORITY QUEUE: ordered by (deadline, txid)
  // After conflict check, txns wait here until their deadline arrives.
  // ONLY accessed by HoldReleaseTd — no mutex needed.
  // Think of this as: "txns waiting for their turn to execute"
  //==========================================================================
  std::map<std::pair<uint64_t, txnid_t>, std::shared_ptr<LuigiLogEntry>> priority_queue_;

  //==========================================================================
  // Per-key last released deadline tracking (for conflict detection)
  // Key = application key (int32_t like Tiga), Value = last released timestamp
  // This combines rMap and wMap from the paper into one (simplified)
  //==========================================================================
  std::unordered_map<int32_t, uint64_t> last_released_deadlines_;

  //==========================================================================
  // READY TXN QUEUE (lock-free)
  // Txns whose deadline has passed go here. ExecTd() consumes them.
  // Think of this as: "txns ready to be executed"
  //==========================================================================
  moodycamel::ConcurrentQueue<std::shared_ptr<LuigiLogEntry>> ready_txn_queue_;

  //==========================================================================
  // Threads
  //==========================================================================
  std::thread* hold_thread_ = nullptr;
  std::thread* exec_thread_ = nullptr;
  std::atomic<bool> running_{false};

  //==========================================================================
  // Partition ID (for replication)
  //==========================================================================
  uint32_t partition_id_ = 0;

  //==========================================================================
  // Executor (handles actual read/write operations and replication)
  //==========================================================================
  LuigiExecutor executor_;

 public:
  void SetPartitionId(uint32_t par_id) { 
    partition_id_ = par_id; 
    executor_.SetPartitionId(par_id);
  }
  uint32_t GetPartitionId() const { return partition_id_; }
  
  // Set the database tables reference (passed to executor)
  void SetDbTables(std::map<int, abstract_ordered_index*>* tables) { 
    executor_.SetDbTables(tables); 
  }
};

} // namespace janus
