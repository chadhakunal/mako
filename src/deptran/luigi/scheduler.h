#pragma once

#include "deptran/__dep__.h"
#include "deptran/classic/scheduler.h" // For SchedulerClassic base class
#include "deptran/concurrentqueue.h"   // moodycamel lock-free queue
#include "deptran/paxos/server.h"      // For PaxosServer (Phase 4)
#include "deptran/tx.h"
#include "rrr/reactor/quorum_event.h" // For QuorumEvent (Phase 4)

// Fix macro conflict: deptran/constants.h defines SUCCESS as (0)
// but mako/lib/common.h uses SUCCESS as an enum value
#undef SUCCESS

#include "executor.h"
#include "luigi_entry.h"
#include "state_machine.h"

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
 *    - If txn.deadline > max(lastReleasedDeadline[key] for all keys): accept
 * into priority_queue_
 *    - Else: update txn.deadline = max + 1 (leader can update timestamp)
 * 4. When current_time >= deadline, release from priority_queue_ to
 * ready_txn_queue_
 * 5. ExecTd() executes and triggers timestamp agreement for multi-shard txns
 */
class SchedulerLuigi : public SchedulerClassic {
  // Helper to get all shard IDs except self for broadcasting
  std::vector<uint32_t> GetAllShardIdsExceptSelf() const;

public:
  SchedulerLuigi();
  virtual ~SchedulerLuigi();

  // Required override from SchedulerClassic - Luigi doesn't use row-level
  // guards since it uses timestamp ordering instead of locking
  virtual bool Guard(Tx &tx_box, mdb::Row *row, int col_id,
                     bool write = true) override {
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
  // involved_shards: list of ALL shard IDs involved in this multi-shard
  // transaction
  void LuigiDispatchFromRequest(
      uint64_t txn_id, uint64_t expected_time, const std::vector<LuigiOp> &ops,
      const std::vector<uint32_t> &involved_shards, uint32_t worker_id,
      std::function<void(int status, uint64_t commit_ts,
                         const std::vector<std::string> &read_results)>
          reply_cb);

  // Methods called by executor
  void Replicate(uint32_t worker_id, std::shared_ptr<LuigiLogEntry> entry);
  void InitiateAgreement(std::shared_ptr<LuigiLogEntry> entry);
  void SendRepositionConfirmations(std::shared_ptr<LuigiLogEntry> entry);

  // Methods called by server (RPC handlers)
  uint64_t HandleRemoteDeadlineProposal(uint64_t tid, uint32_t src_shard,
                                        uint64_t proposed_ts, uint32_t phase);
  bool HandleRemoteDeadlineConfirm(uint64_t tid, uint32_t src_shard,
                                   uint64_t new_ts);
  void HandleWatermarkExchange(uint32_t src_shard,
                               const std::vector<int64_t> &watermarks);
  uint32_t GetPartitionId() const { return shard_id_; }
  uint32_t partition_id() const { return shard_id_; }

  // Server initialization methods
  void SetWorkerCount(uint32_t count);
  void SetStateMachine(std::shared_ptr<LuigiStateMachine> sm);
  void EnableStateMachineMode(bool enable);
  void SetReplicationCallback(LuigiExecutor::ReplicationCallback cb);
  bool HasPendingTxn(uint64_t txn_id) const;
  void SetPartitionId(uint32_t shard_id);

  //==========================================================================
  // PHASE 4: PAXOS REPLICATION METHODS
  //==========================================================================

  /**
   * Initialize Paxos replication streams.
   * Creates one PaxosServer per worker for independent replication.
   */
  void InitializePaxosStreams();

  /**
   * Replay committed Paxos log entries on startup.
   * Restores watermarks from durable Paxos log.
   */
  void ReplayPaxosLog(uint32_t worker_id);

  /**
   * Checkpoint watermarks and truncate Paxos log.
   * Called periodically to prevent unbounded log growth.
   */
  void CheckpointWatermarks();

  /**
   * Append a log entry to the specified worker's stream.
   * Called by follower RPC handler when receiving Replicate request.
   */
  void AppendToLog(uint32_t worker_id, uint64_t slot_id, uint64_t txn_id,
                   uint64_t timestamp, const std::string &log_data);

  /**
   * Batch append log entries to worker's stream.
   * Called by BatchReplicate RPC. Returns last appended slot.
   */
  uint64_t BatchAppendToLog(uint32_t worker_id,
                            const std::vector<uint64_t> &slot_ids,
                            const std::vector<uint64_t> &txn_ids,
                            const std::vector<uint64_t> &timestamps,
                            const std::vector<std::string> &log_entries);

  // Requeue a txn after agreement determines it needs repositioning (Case 3)
  void RequeueForReposition(std::shared_ptr<LuigiLogEntry> entry);

protected:
  // Threads
  void HoldReleaseTd();
  void ExecTd();
  void WatermarkTd();

  // Helpers
  uint64_t GetMicrosecondTimestamp();

  //==========================================================================
  // INCOMING TXN QUEUE (lock-free)
  // New txns from coordinator land here. HoldReleaseTd() consumes them.
  // Think of this as: "txns waiting to be checked for conflicts"
  //==========================================================================
  moodycamel::ConcurrentQueue<std::shared_ptr<LuigiLogEntry>>
      incoming_txn_queue_;

  //==========================================================================
  // PRIORITY QUEUE: ordered by (deadline, worker_id, txid)
  // After conflict check, txns wait here until their deadline arrives.
  // NOW accessed by both HoldReleaseTd AND ExecTd (for re-enqueuing incomplete
  // agreements) Think of this as: "txns waiting for their turn to execute"
  //==========================================================================
  std::mutex priority_queue_mutex_; // Protects priority_queue_
  std::map<std::tuple<uint64_t, uint32_t, txnid_t>,
           std::shared_ptr<LuigiLogEntry>>
      priority_queue_;

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
  // Timestamp ordering enforcement
  // Track minimum timestamp of any transaction with pending agreement
  // to ensure later transactions don't execute before earlier ones
  //==========================================================================
  std::atomic<uint64_t> min_pending_timestamp_{UINT64_MAX};

  //==========================================================================
  // Threads
  //==========================================================================
  std::thread *hold_thread_ = nullptr;
  std::thread *exec_thread_ = nullptr;
  std::thread *watermark_thread_ = nullptr;
  std::atomic<bool> running_{false};

  //==========================================================================
  // Partition ID (for replication)
  //==========================================================================
  uint32_t shard_id_ = 0;

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
    static constexpr uint32_t MAX_SHARDS = 16; // Max shards per txn

    std::shared_ptr<LuigiLogEntry> entry_; // Back-pointer to txn entry
    uint32_t expected_count_ = 0;          // Total shards involved
    uint32_t item_count_ = 0;              // Proposals received so far

    // Per-shard tracking (indexed by shard_id)
    uint64_t deadlines_[MAX_SHARDS] = {}; // Proposed timestamps
    uint32_t phases_[MAX_SHARDS] = {};    // Phase (1=propose, 2=confirm)
    bool received_[MAX_SHARDS] = {};      // Have we received from this shard?

    uint64_t agreed_deadline_ = 0; // Computed max (when complete)

    DeadlineQItem() = default;
  };

  // Main agreement tracking map
  // Key: txn_id, Value: DeadlineQItem
  std::unordered_map<uint64_t, DeadlineQItem> deadline_queue_;
  std::mutex deadline_queue_mutex_; // Protects deadline_queue_

  //==========================================================================
  // WATERMARK MANAGEMENT (Tiga-style)
  //
  // Tracks execution progress per worker stream to ensure global consistency.
  // Coordinator only commits when agreed_ts <= Min(global_watermarks)
  //==========================================================================

  std::mutex watermark_mutex_;
  uint32_t worker_count_ = 1;

  // Local watermarks: watermarks_[worker_id] = last replicated timestamp
  std::vector<uint64_t> watermarks_;

  // Global watermarks: map<shard_id, vector<timestamp>>
  std::map<uint32_t, std::vector<uint64_t>> global_watermarks_;

  // Last time we broadcasted watermarks
  uint64_t last_watermark_broadcast_ = 0;

  //==========================================================================
  // PHASE 4: PAXOS REPLICATION
  // Per-worker Paxos streams for durable log replication with quorum
  //==========================================================================

  // Log entry for Paxos replication (Raft-style append)
  struct LogEntry {
    uint64_t slot_id;
    uint64_t txn_id;
    uint64_t timestamp;
    std::string log_data; // Serialized transaction
  };

  // Per-worker Paxos stream with log storage
  struct PaxosStream {
    uint64_t next_slot_ = 1;      // Next slot to use (leader)
    uint64_t committed_slot_ = 0; // Last committed slot
    std::vector<LogEntry> log_;   // Per-stream log storage

    void Append(const LogEntry &entry) {
      log_.push_back(entry);
      committed_slot_ = entry.slot_id;
    }
  };

  // One stream per worker (paxos_streams_[worker_id])
  std::vector<PaxosStream> paxos_streams_;

  // Number of replicas (from config, default 1 for single-replica)
  uint32_t num_replicas_ = 1;

  // Mutex for Paxos operations (separate from watermark_mutex_ to avoid
  // deadlocks)
  std::mutex paxos_mutex_;

  // Per-worker replication queues (ExecTd enqueues, ReplicateTd dequeues)
  std::vector<std::unique_ptr<
      moodycamel::ConcurrentQueue<std::shared_ptr<LuigiLogEntry>>>>
      replication_queues_;

  // Per-worker replication threads
  std::vector<std::thread *> replicate_threads_;

  // Replication thread function (one per worker)
  void ReplicateTd(uint32_t worker_id);

  // Internal replication logic (called by ReplicateTd)
  void DoReplicate(uint32_t worker_id, std::shared_ptr<LuigiLogEntry> entry);

  // Batched replication with async quorum handling
  void
  DoBatchReplicate(uint32_t worker_id,
                   const std::vector<std::shared_ptr<LuigiLogEntry>> &batch);

  // Follower site IDs for leader-to-follower replication
  // Set at startup for multi-replica mode (e.g., from config)
  std::vector<uint32_t> follower_sites_;

public:
  // Set follower sites for multi-replica mode
  void SetFollowerSites(const std::vector<uint32_t> &sites) {
    follower_sites_ = sites;
    num_replicas_ = 1 + sites.size(); // leader + followers
  }

  //==========================================================================
  // PENDING COMMITS (waiting for watermarks to advance)
  // Transactions that have finished execution but are waiting for
  // watermarks to advance before being considered durable/committed.
  //==========================================================================
  struct PendingCommit {
    uint64_t txn_id;
    uint64_t timestamp; // Transaction's agreed timestamp
    uint32_t worker_id;
    std::vector<uint32_t> involved_shards;
    std::function<void()> reply_callback; // Called when watermarks advance
  };
  std::mutex pending_commits_mutex_;
  std::map<uint64_t, PendingCommit> pending_commits_;

  // Pending transactions tracking (for async status check)
  mutable std::mutex pending_txns_mutex_;
  std::unordered_set<uint64_t> pending_txns_;

  // (SetPartitionId moved to public section above)

  // (SetWorkerCount moved to public section above)

  // (GetPartitionId, partition_id, HasPendingTxn moved to public section above)

  //==========================================================================
  // WATERMARK METHODS
  //==========================================================================

  /**
   * Update local watermark for a specific worker stream.
   * Called after successful Paxos replication.
   */
  void UpdateLocalWatermark(uint32_t worker_id, uint64_t ts);

  /**
   * Get global watermark for a specific shard/worker.
   * Used by Client/Coordinator to determine commit safety.
   */
  uint64_t GetGlobalWatermark(uint32_t shard_id, uint32_t worker_id);

  /**
   * Handle incoming WatermarkExchange RPC.
   * (Implementation moved to .cc file)
   */

  /**
   * Periodic task to send local watermarks to coordinator for commit decisions.
   */
  void SendWatermarksToCoordinator();

  /**
   * Get current local watermarks (for RPC response).
   */
  std::vector<int64_t> GetLocalWatermarks();

public:
  //==========================================================================
  // WATERMARK-BASED COMMIT DECISION METHODS
  //==========================================================================

  /**
   * Check if transaction can commit based on watermarks.
   * Returns true if timestamp <= watermark[shard][worker] for all involved
   * shards.
   */
  bool CanCommit(uint64_t timestamp, uint32_t worker_id,
                 const std::vector<uint32_t> &involved_shards);

  /**
   * Add a pending commit that will be completed when watermarks advance.
   */
  void AddPendingCommit(uint64_t txn_id, uint64_t timestamp, uint32_t worker_id,
                        const std::vector<uint32_t> &involved_shards,
                        std::function<void()> reply_callback);

  /**
   * Check all pending commits and complete any that can now commit.
   * Called when watermarks are updated.
   */
  void CheckPendingCommits();

  //==========================================================================
  // PHASE 2: RPC BATCHING INFRASTRUCTURE
  // Batch DeadlinePropose and DeadlineConfirm to reduce RPC overhead
  //==========================================================================

  /**
   * Batch buffer for deadline proposals.
   * Accumulates proposals for flush interval before broadcasting.
   */
  struct ProposalBatch {
    std::vector<uint64_t> tids;
    std::vector<uint64_t> proposed_ts;
    std::vector<uint32_t> remote_shards; // Union of all remote shards
  };

  /**
   * Batch buffer for deadline confirmations.
   */
  struct ConfirmBatch {
    std::vector<uint64_t> tids;
    std::vector<uint64_t> agreed_ts;
    std::vector<uint32_t> remote_shards;
  };

  ProposalBatch pending_proposals_;
  ConfirmBatch pending_confirms_;
  std::mutex batch_mutex_;

  // Flush timer
  std::chrono::steady_clock::time_point last_flush_time_;
  static constexpr uint64_t BATCH_FLUSH_INTERVAL_US =
      5000; // 5ms (increased for better batching)
  static constexpr size_t BATCH_SIZE_THRESHOLD =
      50; // Flush early if batch gets large

  /**
   * Queue a deadline proposal for batching.
   * Flushes if batch interval exceeded.
   */
  void QueueDeadlineProposal(uint64_t tid, uint64_t proposed_ts,
                             const std::vector<uint32_t> &remote_shards);

  /**
   * Queue a deadline confirmation for batching.
   */
  void QueueDeadlineConfirmation(uint64_t tid, uint64_t agreed_ts,
                                 const std::vector<uint32_t> &remote_shards);

  /**
   * Flush pending proposal batch to network.
   */
  void FlushProposalBatch();

  /**
   * Flush pending confirmation batch to network.
   */
  void FlushConfirmBatch();

  /**
   * Periodic flush thread (runs every BATCH_FLUSH_INTERVAL_US).
   */
  void BatchFlushLoop();

  std::thread *batch_flush_thread_ = nullptr;

  //==========================================================================
  // CALLBACKS FOR DB OPERATIONS (set by Mako's ShardReceiver)
  //==========================================================================

  // (SetReplicationCallback moved to public section)

  //==========================================================================
  // STATE MACHINE MODE (Tiga-style stored procedure execution)
  // When enabled, bypasses callbacks and uses direct storage access.
  //==========================================================================

  // (SetStateMachine and EnableStateMachineMode moved to public section)

  bool IsStateMachineMode() const { return executor_.IsStateMachineMode(); }

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
  void UpdateDeadlineRecord(uint64_t tid, uint32_t src_shard,
                            uint64_t proposed_ts, uint32_t phase,
                            std::shared_ptr<LuigiLogEntry> entry = nullptr);

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
  // (Moved to public section above)

  /**
   * Send Phase 2 confirmations after repositioning.
   * Called when we were in Case 3 and have repositioned.
   *
   * @param entry The transaction entry (with updated proposed_ts)
   */
  // (Moved to public section above)
};

} // namespace janus
