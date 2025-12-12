#pragma once

#include "deptran/__dep__.h"
#include "deptran/classic/scheduler.h"  // For SchedulerClassic base class
#include "deptran/tx.h"
#include "deptran/concurrentqueue.h"  // moodycamel lock-free queue
#include "deptran/rcc_rpc.h"          // For LuigiLeaderProxy

#include "luigi_entry.h"
#include "luigi_executor.h"

#include <atomic>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <functional>

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

  // Required override from SchedulerClassic - Luigi doesn't use row-level guards
  // since it uses timestamp ordering instead of locking
  virtual bool Guard(Tx &tx_box, mdb::Row *row, int col_id, bool write=true) override {
    // Luigi uses timestamp-based ordering, not row-level locking
    // Always return true (no guard needed)
    return true;
  }

  // Start background threads
  void Start();
  void Stop();

  // Entry point for a Luigi-style dispatch from raw request buffer.
  // This is called by server.cc's HandleLuigiDispatch.
  // Parses the request, creates a LuigiLogEntry, and enqueues it.
  // involved_shards: list of ALL shard IDs involved in this multi-shard transaction
  void LuigiDispatchFromRequest(
      uint64_t txn_id,
      uint64_t expected_time,
      const std::vector<LuigiOp>& ops,
      const std::vector<uint32_t>& involved_shards,
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

  //==========================================================================
  // LEADER AGREEMENT STATE (Tiga-style bidirectional broadcast)
  // 
  // Key insight: In Tiga, ALL involved leaders independently broadcast 
  // their proposals to all other involved shards. There's no single 
  // "coordinator". Each leader collects proposals and determines the
  // agreed timestamp when all proposals are received.
  //
  // Flow:
  // 1. Txn reaches execution at shard X
  // 2. Shard X broadcasts its proposal to all involved shards (fire-and-forget)
  // 3. When shard X receives proposals from all N involved shards:
  //    - agreed_ts = max(all proposals)
  //    - If my_ts == agreed_ts: Case 1 (all match) or Case 2 (I'm max)
  //    - If my_ts < agreed_ts: Case 3 (need to reposition)
  //==========================================================================
  
  /**
   * DeadlineQItem - Tiga-style per-transaction agreement state.
   * Tracks proposals from all involved shards for a transaction.
   */
  struct DeadlineQItem {
    static constexpr uint32_t MAX_SHARDS = 16;  // Max shards per txn
    
    std::shared_ptr<LuigiLogEntry> entry_;      // Back-pointer to txn entry
    uint32_t expected_count_ = 0;               // Total shards involved
    uint32_t item_count_ = 0;                   // Proposals received so far
    
    // Per-shard tracking (indexed by shard_id)
    uint64_t deadlines_[MAX_SHARDS] = {};       // Proposed timestamps
    uint32_t phases_[MAX_SHARDS] = {};          // Phase (1=propose, 2=confirm)
    bool received_[MAX_SHARDS] = {};            // Have we received from this shard?
    
    uint64_t agreed_deadline_ = 0;              // Computed max (when complete)
    
    DeadlineQItem() = default;
  };
  
  // Main agreement tracking map
  // Key: txn_id, Value: DeadlineQItem
  std::unordered_map<uint64_t, DeadlineQItem> deadline_queue_;
  std::mutex deadline_queue_mutex_;  // Protects deadline_queue_
  
  //==========================================================================
  // LEADER PROXIES (for RPC to other leaders)
  //==========================================================================
  std::map<uint32_t, LuigiLeaderProxy*> leader_proxies_;  // shard_id -> proxy
  std::mutex proxies_mutex_;
  
  //==========================================================================
  // PENDING TXN TRACKING (for async status check)
  // Tracks txn_ids from dispatch until completion callback.
  //==========================================================================
  std::unordered_set<uint64_t> pending_txns_;
  mutable std::mutex pending_txns_mutex_;

 public:
  void SetPartitionId(uint32_t par_id) { 
    partition_id_ = par_id; 
    executor_.SetPartitionId(par_id);
  }
  uint32_t GetPartitionId() const { return partition_id_; }
  uint32_t partition_id() const { return partition_id_; }  // alias
  
  /**
   * Check if a transaction is still pending (queued or executing).
   * Used by async status check to distinguish QUEUED vs NOT_FOUND.
   */
  bool HasPendingTxn(uint64_t txn_id) const;
  
  //==========================================================================
  // CALLBACKS FOR DB OPERATIONS (set by Mako's ShardReceiver)
  //==========================================================================
  
  void SetReadCallback(LuigiExecutor::ReadCallback cb) { 
    executor_.SetReadCallback(std::move(cb)); 
  }
  void SetWriteCallback(LuigiExecutor::WriteCallback cb) { 
    executor_.SetWriteCallback(std::move(cb)); 
  }
  void SetReplicationCallback(LuigiExecutor::ReplicationCallback cb) { 
    executor_.SetReplicationCallback(std::move(cb)); 
  }
  
  //==========================================================================
  // LEADER PROXY MANAGEMENT
  //==========================================================================
  
  // Add a proxy for communicating with another leader
  void AddLeaderProxy(uint32_t shard_id, LuigiLeaderProxy* proxy) {
    std::lock_guard<std::mutex> lock(proxies_mutex_);
    leader_proxies_[shard_id] = proxy;
  }
  
  // Get proxy for a specific shard (returns nullptr if not found)
  LuigiLeaderProxy* GetLeaderProxy(uint32_t shard_id) {
    std::lock_guard<std::mutex> lock(proxies_mutex_);
    auto it = leader_proxies_.find(shard_id);
    return (it != leader_proxies_.end()) ? it->second : nullptr;
  }
  
  //==========================================================================
  // AGREEMENT HANDLERS (Tiga-style bidirectional broadcast)
  //==========================================================================
  
  /**
   * Update the deadline record for a transaction (Tiga-style).
   * Called when:
   * 1. We initiate agreement for our own txn (with our proposal)
   * 2. We receive a remote proposal via RPC
   * 
   * When all proposals are received, determines the agreement case:
   * - Case 1: All proposals match -> AGREE_COMPLETE
   * - Case 2: My ts == max ts but others differ -> AGREE_CONFIRMING (wait)
   * - Case 3: My ts < max ts -> AGREE_FLUSHING (reposition)
   * 
   * @param tid Transaction ID
   * @param src_shard Source shard (who sent the proposal)
   * @param proposed_ts The proposed timestamp
   * @param phase 1=initial proposal, 2=reposition confirmation
   * @param entry (optional) The txn entry if this is our local txn
   */
  void UpdateDeadlineRecord(uint64_t tid, uint32_t src_shard, uint64_t proposed_ts, 
                            uint32_t phase, std::shared_ptr<LuigiLogEntry> entry = nullptr);
  
  /**
   * Handle a deadline proposal RPC from a remote leader.
   * This is the RPC handler called by LuigiLeaderServiceImpl.
   * 
   * @param tid Transaction ID
   * @param src_shard Source shard that sent the proposal
   * @param remote_ts Remote shard's proposed timestamp
   * @param phase 1=initial proposal, 2=reposition confirmation
   * @return Our proposed timestamp for this txn (0 if we don't have entry yet)
   */
  uint64_t HandleRemoteDeadlineProposal(uint64_t tid, uint32_t src_shard, uint64_t remote_ts, uint32_t phase);
  
  /**
   * Handle a confirmation from a remote leader (phase=2).
   * Implemented via HandleRemoteDeadlineProposal.
   */
  bool HandleRemoteDeadlineConfirm(uint64_t tid, uint32_t src_shard, uint64_t new_ts);
  
  /**
   * Initiate agreement for a multi-shard transaction (Tiga-style).
   * Called by executor when a multi-shard txn reaches head of queue.
   * 
   * This:
   * 1. Records our own proposal in deadline_queue_
   * 2. Broadcasts our proposal to all involved shards (fire-and-forget)
   * 3. Returns immediately (completion happens asynchronously)
   * 
   * @param entry The transaction entry
   */
  void InitiateAgreement(std::shared_ptr<LuigiLogEntry> entry);
  
  /**
   * Send Phase 2 confirmations after repositioning.
   * Called when we were in Case 3 and have repositioned.
   * 
   * @param entry The transaction entry (with updated proposed_ts)
   */
  void SendRepositionConfirmations(std::shared_ptr<LuigiLogEntry> entry);
};

} // namespace janus
