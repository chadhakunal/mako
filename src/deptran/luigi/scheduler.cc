#include "scheduler.h"
#include "commo.h" // For LuigiCommo
#include "deptran/__dep__.h"
#include "luigi_common.h"
#include "luigi_entry.h"

#include <chrono>
#include <functional>
#include <iostream>

// External Paxos replication function from paxos_main_helper.cc
// This routes through pxs_workers_g if available
extern void add_log_to_nc(const char *log, int len, uint32_t par_id,
                          int batch_size);

namespace janus {

//=============================================================================
// Construction / Destruction
//=============================================================================

SchedulerLuigi::SchedulerLuigi() : SchedulerClassic() {
  // Set executor's scheduler reference so it can call back for RPC coordination
  executor_.SetScheduler(this);
}

SchedulerLuigi::~SchedulerLuigi() { Stop(); }

//=============================================================================
// Thread Management
//=============================================================================

void SchedulerLuigi::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true))
    return;

  // Initialize flush timer
  last_flush_time_ = std::chrono::steady_clock::now();

  // Initialize per-worker replication queues and threads
  replication_queues_.resize(worker_count_);
  replicate_threads_.resize(worker_count_);
  for (uint32_t i = 0; i < worker_count_; i++) {
    replication_queues_[i] = std::make_unique<
        moodycamel::ConcurrentQueue<std::shared_ptr<LuigiLogEntry>>>();
    replicate_threads_[i] =
        new std::thread(&SchedulerLuigi::ReplicateTd, this, i);
  }

  hold_thread_ = new std::thread(&SchedulerLuigi::HoldReleaseTd, this);
  exec_thread_ = new std::thread(&SchedulerLuigi::ExecTd, this);
  watermark_thread_ = new std::thread(&SchedulerLuigi::WatermarkTd, this);
  batch_flush_thread_ = new std::thread(&SchedulerLuigi::BatchFlushLoop, this);
}

void SchedulerLuigi::Stop() {
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false))
    return;

  // Stop replication threads
  for (auto *t : replicate_threads_) {
    if (t) {
      t->join();
      delete t;
    }
  }
  replicate_threads_.clear();
  replication_queues_.clear();

  if (hold_thread_) {
    hold_thread_->join();
    delete hold_thread_;
    hold_thread_ = nullptr;
  }
  if (exec_thread_) {
    exec_thread_->join();
    delete exec_thread_;
    exec_thread_ = nullptr;
  }
  if (watermark_thread_) {
    watermark_thread_->join();
    delete watermark_thread_;
    watermark_thread_ = nullptr;
  }
  if (batch_flush_thread_) {
    batch_flush_thread_->join();
    delete batch_flush_thread_;
    batch_flush_thread_ = nullptr;
  }
}

//=============================================================================
// Utility
//=============================================================================

uint64_t SchedulerLuigi::GetMicrosecondTimestamp() {
  auto tse = std::chrono::system_clock::now().time_since_epoch();
  return (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(tse)
      .count();
}

bool SchedulerLuigi::HasPendingTxn(uint64_t txn_id) const {
  std::lock_guard<std::mutex> lock(pending_txns_mutex_);
  return pending_txns_.find(txn_id) != pending_txns_.end();
}

//=============================================================================
// LuigiDispatchFromRequest: Entry Point from server.cc
//
// Creates a LuigiLogEntry from parsed request data and enqueues it.
// involved_shards contains all shard IDs involved in this transaction,
// which is critical for multi-shard leader agreement.
//=============================================================================

void SchedulerLuigi::LuigiDispatchFromRequest(
    uint64_t txn_id, uint64_t expected_time, const std::vector<LuigiOp> &ops,
    const std::vector<uint32_t> &involved_shards, uint32_t worker_id,
    std::function<void(int status, uint64_t commit_ts,
                       const std::vector<std::string> &read_results)>
        reply_cb) {

  // Track this txn as pending (for async status check)
  {
    std::lock_guard<std::mutex> lock(pending_txns_mutex_);
    pending_txns_.insert(txn_id);
  }

  auto entry = std::make_shared<LuigiLogEntry>(txn_id);
  entry->proposed_ts_ =
      expected_time; // Use expected_time directly as proposed timestamp
  entry->agreed_ts_ =
      expected_time; // Initialize to proposed - will be updated by agreement
  entry->worker_id_ = worker_id; // Store worker ID for per-worker replication
  entry->ops_ = ops;

  // Wrap callback to remove from pending when complete
  entry->reply_cb_ = [this, txn_id,
                      reply_cb](int status, uint64_t commit_ts,
                                const std::vector<std::string> &read_results) {
    // Remove from pending set
    {
      std::lock_guard<std::mutex> lock(pending_txns_mutex_);
      pending_txns_.erase(txn_id);
    }
    // Call original callback
    if (reply_cb) {
      reply_cb(status, commit_ts, read_results);
    }
  };

  // Populate remote_shards_ with OTHER shards (not ourselves)
  // This is used by IsMultiShard() to detect multi-shard transactions
  for (uint32_t shard_id : involved_shards) {
    if (shard_id != shard_id_) {
      entry->remote_shards_.push_back(shard_id);
    }
  }

  // Extract keys for conflict detection (Tiga-style)
  // Convert (table_id, key) pairs to integer key IDs using hash
  for (const auto &op : ops) {
    // Create unique key ID from table_id and key
    // Use std::hash to combine them properly
    std::string combined_key = std::to_string(op.table_id) + ":" + op.key;
    int32_t key_id =
        static_cast<int32_t>(std::hash<std::string>{}(combined_key));
    entry->local_keys_.push_back(key_id);
  }

  // Enqueue to incoming queue (lock-free, thread-safe)
  // InitiateAgreement is called from HoldReleaseTd AFTER conflict detection,
  // so the proposal uses the correct (possibly bumped) timestamp.
  incoming_txn_queue_.enqueue(entry);
}

//=============================================================================
// RequeueForReposition: Called when agreement determines need for Case 3
//
// After a multi-shard agreement, if this leader used a smaller timestamp than
// the agreed one, the txn needs to:
// 1. Have its speculative execution rolled back (done by executor)
// 2. Have its proposed_ts_ updated to agreed_ts_ (done by executor)
// 3. Be repositioned in the priority queue (done here)
//
// We simply enqueue it back to incoming_txn_queue_ with AGREE_FLUSHING status.
// HoldReleaseTd() will see this status and skip conflict detection, going
// directly into priority_queue_ at the new timestamp.
//=============================================================================

void SchedulerLuigi::RequeueForReposition(
    std::shared_ptr<LuigiLogEntry> entry) {
  // Re-insert into priority queue with the new agreed timestamp
  // The agreed_ts has been updated by the agreement protocol
  Log_debug("Luigi RequeueForReposition: tid=%lu, agreed_ts=%lu", entry->tid_,
            entry->agreed_ts_);

  // Put back in incoming queue - HoldReleaseTd will handle the repositioning
  incoming_txn_queue_.enqueue(entry);
}

//=============================================================================
// HoldReleaseTd: The Core of Luigi
//
// This thread runs in a loop and does two things:
// 1. Pulls txns from incoming_txn_queue_, checks conflicts, adds to
// priority_queue_
// 2. Releases txns from priority_queue_ when their deadline passes, sends to
// ready_txn_queue_
//
// Additional responsibility for agreement Case 3 (AGREE_FLUSHING):
// - Txns that need repositioning come back with AGREE_FLUSHING status
// - They go directly into priority_queue_ at their new (agreed) timestamp
// - No conflict check needed - the agreed timestamp is final
//=============================================================================

void SchedulerLuigi::HoldReleaseTd() {
  std::shared_ptr<LuigiLogEntry> entries[256]; // bulk dequeue buffer

  while (running_) {
    uint64_t now = GetMicrosecondTimestamp();

    //-------------------------------------------------------------------------
    // Phase 1: Pull from incoming_txn_queue_, do conflict check, add to
    // priority_queue_
    //-------------------------------------------------------------------------
    size_t cnt = incoming_txn_queue_.try_dequeue_bulk(entries, 256);
    for (size_t i = 0; i < cnt; i++) {
      auto entry = entries[i];
      uint64_t txn_key = entry->tid_;

      //-----------------------------------------------------------------------
      // Check if this is a repositioning after agreement (Case 3)
      // In Tiga, this is the AGREE_FLUSHING state
      //-----------------------------------------------------------------------
      if (entry->agree_status_.load() == LUIGI_AGREE_FLUSHING) {
        // This txn is being repositioned after agreement told us we need
        // a larger timestamp. The agreed_ts_ is already set.
        // Go directly into priority_queue_ without conflict check.

        // The txn already has the updated proposed_ts_ = agreed_ts_
        // (set by the executor before returning to us)

        Log_debug("Luigi HoldReleaseTd: Repositioning txn %lu at new timestamp "
                  "%lu (requeue #%u)",
                  entry->tid_, entry->agreed_ts_, entry->requeue_count_);

        // Insert at new position using agreed_ts (like Tiga's localDdlRank_)
        priority_queue_[{entry->agreed_ts_, entry->worker_id_, entry->tid_}] =
            entry;
        continue; // Skip normal conflict detection
      }

      //-----------------------------------------------------------------------
      // Normal path: NEW txn entering for the first time
      //-----------------------------------------------------------------------

      // CONFLICT DETECTION (from Tiga Algorithm 1, line 1-4):
      // Find the maximum lastReleasedDeadline among all keys this txn touches
      uint64_t max_last_released = 0;
      for (auto &k : entry->local_keys_) {
        auto it = last_released_deadlines_.find(k);
        if (it != last_released_deadlines_.end() &&
            it->second > max_last_released) {
          max_last_released = it->second;
        }
      }

      // If txn's timestamp is too small (conflict), update it
      // This is the LEADER PRIVILEGE: we can bump the timestamp (like Tiga)
      if (entry->agreed_ts_ <= max_last_released) {
        entry->agreed_ts_ = max_last_released + 1;
        entry->proposed_ts_ = entry->agreed_ts_; // Keep in sync
      }

      // CRITICAL: Initiate agreement AFTER conflict detection
      // This ensures we broadcast the correct (possibly bumped) timestamp
      InitiateAgreement(entry);

      // For single-shard txns, InitiateAgreement -> UpdateDeadlineRecord
      // immediately completes and enqueues to ready_txn_queue_.
      // Skip priority_queue insertion to avoid double-enqueue.
      // Multi-shard txns: Insert into priority queue while waiting for
      // agreement. Single-shard txns also go here for ordering.
      std::lock_guard<std::mutex> pq_lock(priority_queue_mutex_);
      priority_queue_[{entry->agreed_ts_, entry->worker_id_, entry->tid_}] =
          entry;
    }

    //-------------------------------------------------------------------------
    // Phase 2: Release txns whose deadline has passed -> ready_txn_queue_
    //-------------------------------------------------------------------------
    {
      std::lock_guard<std::mutex> pq_lock(priority_queue_mutex_);

      while (!priority_queue_.empty()) {
        auto it = priority_queue_.begin();
        uint64_t deadline = std::get<0>(it->first); // Get timestamp from tuple
        auto entry = it->second;

        // 1. Check timestamp ordering
        if (deadline > now) {
          break; // Not ready yet
        }

        // 2. Head-of-Line Blocking for multi-shard agreement
        // We cannot release a multi-shard txn until its timestamp is finalized
        if (!entry->ts_agreed_.load()) {
          // Check for Case 3 Repositioning (FLUSHING)
          if (entry->agree_status_.load() == LUIGI_AGREE_FLUSHING) {
            Log_debug(
                "Luigi HoldReleaseTd: Repositioning tid=%lu from %lu to %lu",
                entry->tid_, deadline, entry->agreed_ts_);
            // Remove and re-insert with new timestamp
            priority_queue_.erase(it);
            priority_queue_[{entry->agreed_ts_, entry->worker_id_,
                             entry->tid_}] = entry;
            continue; // Re-evaluate top
          }

          // Otherwise, just block and wait for agreement to complete.
          // We'll check again in the next iteration.
          break;
        }

        // 3. Ready to release!
        priority_queue_.erase(it);

        // Update lastReleasedDeadlines for all keys this txn touches (like
        // Tiga)
        for (auto &k : entry->local_keys_) {
          if (last_released_deadlines_[k] < entry->agreed_ts_) {
            last_released_deadlines_[k] = entry->agreed_ts_;
          }
        }

        // Hand off to execution thread
        ready_txn_queue_.enqueue(entry);
      }
    }

    //-------------------------------------------------------------------------
    // Small sleep to avoid busy-waiting when queues are empty
    //-------------------------------------------------------------------------
    if (cnt == 0 && priority_queue_.empty()) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
}

//=============================================================================
// ExecTd: Execution Thread
//
// This thread:
// 1. Pulls txns from ready_txn_queue_ (deadline has passed, ready to execute)
// 2. Delegates execution to LuigiExecutor (handles DB ops, multi-shard,
// replication)
// 3. Handles post-execution state:
//    - AGREE_COMPLETE: Normal completion, no further action
//    - AGREE_FLUSHING: Requeue for reposition (Case 3)
//    - AGREE_CONFIRMING: Wait for round 2 confirmations (Case 2)
//=============================================================================

void SchedulerLuigi::ExecTd() {
  std::shared_ptr<LuigiLogEntry> entries[64];

  while (running_) {
    size_t cnt = ready_txn_queue_.try_dequeue_bulk(entries, 64);

    for (size_t i = 0; i < cnt; i++) {
      auto &entry = entries[i];

      // For multi-shard txns in AGREE_INIT state, we must call Execute() to
      // trigger InitiateAgreement(). Skip entries that are:
      // - PENDING (agreement initiated, waiting for proposals)
      // - CONFIRMING (waiting for phase-2 confirmations)
      LuigiAgreeStatus agree_status =
          static_cast<LuigiAgreeStatus>(entry->agree_status_.load());

      if (agree_status == LUIGI_AGREE_PENDING ||
          agree_status == LUIGI_AGREE_CONFIRMING) {
        // Agreement in progress - don't process, wait for async completion
        // UpdateDeadlineRecord() will re-enqueue when ready
        continue;
      }

      // Delegate to executor for clean separation of concerns
      Log_debug("Luigi ExecTd: calling Execute() for tid=%lu agree_status=%d "
                "exec_status=%d",
                entry->tid_, entry->agree_status_.load(),
                entry->exec_status_.load());
      executor_.Execute(entry);

      // Handle post-execution state based on agreement outcome
      LuigiAgreeStatus status =
          static_cast<LuigiAgreeStatus>(entry->agree_status_.load());

      switch (status) {
      case LUIGI_AGREE_COMPLETE:
        // Normal completion - nothing more to do
        // Execute() already called the callback
        break;

      case LUIGI_AGREE_INIT:
        // Should not happen - INIT should transition to PENDING after Execute()
        Log_warn("Luigi ExecTd: txn %lu unexpected INIT after Execute()",
                 entry->tid_);
        break;

      case LUIGI_AGREE_PENDING:
        // Multi-shard txn: InitiateAgreement() was called, RPCs sent.
        // The txn is now waiting for async agreement completion.
        // When all proposals arrive, UpdateDeadlineRecord() will:
        //   - Determine Case 1/2/3
        //   - Set agree_status_ appropriately
        //   - Re-enqueue to ready_txn_queue_ if ready to execute
        // Nothing to do here - just let it wait.
        Log_debug("Luigi ExecTd: txn %lu waiting for agreement (PENDING)",
                  entry->tid_);
        break;

      case LUIGI_AGREE_FLUSHING:
        // Case 3: Need to reposition in priority queue
        // Execute() updated proposed_ts_ to agreed_ts_
        // Requeue for re-processing at new timestamp
        Log_debug("Luigi ExecTd: txn %lu needs reposition, requeuing to "
                  "incoming queue",
                  entry->tid_);
        RequeueForReposition(entry);
        break;

      case LUIGI_AGREE_CONFIRMING:
        // Case 2: Waiting for round 2 confirmations
        // The txn is waiting for phase-2 confirmations from smaller-ts shards.
        // When all confirmations arrive, UpdateDeadlineRecord() will:
        //   - Set agree_status_ to AGREE_COMPLETE
        //   - Re-enqueue to ready_txn_queue_
        // Nothing to do here - just let it wait.
        Log_debug(
            "Luigi ExecTd: txn %lu waiting for phase-2 confirmations (Case 2)",
            entry->tid_);
        break;

      default:
        // Unexpected state - log warning
        Log_warn(
            "Luigi ExecTd: txn %lu has unexpected status %d after Execute()",
            entry->tid_, static_cast<int>(status));
        break;
      }

      if (cnt == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }
  }
}

//=============================================================================
// Agreement Handlers - Tiga-style bidirectional broadcast
//
// In Tiga's model:
// 1. Each leader involved in a multi-shard txn broadcasts its proposal to all
//    other involved shards (fire-and-forget, no waiting for response)
// 2. Each leader collects proposals as they arrive (via RPC handlers)
// 3. When all proposals received (itemCnt_ == expectedCnt_), compute:
//    - agreed_ts = max(all proposals)
//    - Case 1: all match -> AGREE_COMPLETE
//    - Case 2: my_ts == agreed_ts but others differ -> AGREE_CONFIRMING
//    - Case 3: my_ts < agreed_ts -> AGREE_FLUSHING (reposition)
//=============================================================================

void SchedulerLuigi::UpdateDeadlineRecord(
    uint64_t tid, uint32_t src_shard, uint64_t proposed_ts, uint32_t phase,
    std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // Core agreement logic - called for both local and remote proposals.
  //
  // When all proposals are received, this determines the outcome.
  //===========================================================================

  std::lock_guard<std::mutex> lock(deadline_queue_mutex_);

  // For remote proposals (entry == nullptr), we may receive them before
  // our local entry is created. That's fine - we'll store the proposal
  // and it will be counted when the local entry initializes.
  // We only reject if the record exists AND is already complete (erased entry).
  if (entry == nullptr) {
    auto it = deadline_queue_.find(tid);
    if (it != deadline_queue_.end() && it->second.expected_count_ > 0 &&
        it->second.item_count_ >= it->second.expected_count_) {
      // Record exists and is already complete - this is a late/stale proposal
      Log_debug(
          "Luigi UpdateDeadlineRecord: ignoring stale proposal for tid=%lu",
          tid);
      return;
    }
    // Otherwise, store the proposal - it will be processed when local entry
    // arrives
  }

  DeadlineQItem &dqi = deadline_queue_[tid];

  // If entry provided (our local txn), initialize expected count
  if (entry != nullptr && dqi.entry_ == nullptr) {
    dqi.entry_ = entry;
    dqi.expected_count_ =
        entry->remote_shards_.size() + 1; // remotes + ourselves
    Log_debug("Luigi UpdateDeadlineRecord: tid=%lu initialized, expecting %u "
              "proposals",
              tid, dqi.expected_count_);
  }

  // Record this proposal (if not already received from this shard)
  if (src_shard < DeadlineQItem::MAX_SHARDS && !dqi.received_[src_shard]) {
    dqi.deadlines_[src_shard] = proposed_ts;
    dqi.phases_[src_shard] = phase;
    dqi.received_[src_shard] = true;
    dqi.item_count_++;

    Log_debug("[shard %u] Luigi UpdateDeadlineRecord: tid=%lu from shard %u "
              "ts=%lu phase=%u, "
              "now have %u/%u proposals",
              shard_id_, tid, src_shard, proposed_ts, phase, dqi.item_count_,
              dqi.expected_count_);
  } else if (src_shard >= DeadlineQItem::MAX_SHARDS) {
    Log_warn("Luigi UpdateDeadlineRecord: shard_id %u exceeds MAX_SHARDS",
             src_shard);
    return;
  }

  // Check if we have all proposals AND we have the entry
  if (dqi.entry_ != nullptr && dqi.expected_count_ > 0 &&
      dqi.item_count_ == dqi.expected_count_) {

    // Compute agreed timestamp as max of all proposals
    uint64_t agreed_ts = 0;
    bool all_match = true;
    uint64_t first_ts = 0;

    for (uint32_t i = 0; i < DeadlineQItem::MAX_SHARDS; i++) {
      if (dqi.received_[i]) {
        if (dqi.deadlines_[i] > agreed_ts) {
          agreed_ts = dqi.deadlines_[i];
        }
        if (first_ts == 0) {
          first_ts = dqi.deadlines_[i];
        } else if (dqi.deadlines_[i] != first_ts) {
          all_match = false;
        }
      }
    }

    dqi.agreed_deadline_ = agreed_ts;
    uint64_t my_ts = dqi.entry_->proposed_ts_;

    Log_debug("[shard %u] Luigi UpdateDeadlineRecord: tid=%lu COMPLETE - "
              "agreed_ts=%lu, "
              "my_ts=%lu, all_match=%d",
              shard_id_, tid, agreed_ts, my_ts, all_match);

    // Determine which case we're in
    if (all_match) {
      // Case 1: All proposals match - we're done!
      dqi.entry_->agreed_ts_ = agreed_ts;
      dqi.entry_->ts_agreed_.store(true); // Agreement complete!
      dqi.entry_->agree_status_.store(LUIGI_AGREE_COMPLETE);

      Log_debug("Luigi: tid=%lu Case 1 - all match at ts=%lu", tid, agreed_ts);

      // DO NOT enqueue to ready_txn_queue_ here.
      // HoldReleaseTd will pick up the completion and release it in order.

    } else if (my_ts == agreed_ts) {
      // Case 2: I proposed the max, but others differ
      // Wait for others to reposition and confirm (phase 2)
      dqi.entry_->agreed_ts_ = agreed_ts;
      dqi.entry_->agree_status_.store(LUIGI_AGREE_CONFIRMING);

      // Count how many phase-2 confirmations we need
      uint32_t pending = 0;
      for (uint32_t i = 0; i < DeadlineQItem::MAX_SHARDS; i++) {
        if (dqi.received_[i] && dqi.deadlines_[i] < agreed_ts) {
          pending++;
        }
      }

      Log_info("Luigi: tid=%lu Case 2 - I'm max, waiting for %u confirmations",
               tid, pending);

      // Reset to wait for phase 2 confirmations
      dqi.item_count_ = 1; // Only our own (we don't need to re-receive)
      dqi.expected_count_ =
          pending + 1; // Need confirmations from smaller ts shards
      for (uint32_t i = 0; i < DeadlineQItem::MAX_SHARDS; i++) {
        if (i == shard_id_)
          continue;
        if (dqi.received_[i] && dqi.deadlines_[i] < agreed_ts) {
          // Need phase 2 from this shard
          dqi.received_[i] = false;
          dqi.phases_[i] = 0;
        } else if (dqi.received_[i]) {
          // This shard also has max, count as already confirmed
          dqi.item_count_++;
        }
      }

    } else {
      // Case 3: My timestamp is smaller - need to reposition
      dqi.entry_->agreed_ts_ = agreed_ts;
      dqi.entry_->agree_status_.store(LUIGI_AGREE_FLUSHING);

      Log_info(
          "Luigi: tid=%lu Case 3 - my_ts=%lu < agreed=%lu, need reposition",
          tid, my_ts, agreed_ts);

      // HoldReleaseTd will handle repositioning when it sees AGREE_FLUSHING
    }

    // Clean up if complete
    if (dqi.entry_->agree_status_.load() == LUIGI_AGREE_COMPLETE) {
      deadline_queue_.erase(tid);
    }
  }
}

uint64_t SchedulerLuigi::HandleRemoteDeadlineProposal(uint64_t tid,
                                                      uint32_t src_shard,
                                                      uint64_t remote_ts,
                                                      uint32_t phase) {
  //===========================================================================
  // RPC handler for incoming deadline proposals.
  //
  // In Tiga style, we just record the proposal and check for completion.
  // We return our proposal if we have one (for informational purposes).
  //===========================================================================

  Log_info("Luigi HandleRemoteDeadlineProposal: tid=%lu from shard %u ts=%lu "
           "phase=%u",
           tid, src_shard, remote_ts, phase);

  // Get our proposal to return (if we have one)
  uint64_t my_ts = 0;
  {
    std::lock_guard<std::mutex> lock(deadline_queue_mutex_);
    auto it = deadline_queue_.find(tid);
    if (it != deadline_queue_.end() && it->second.entry_ != nullptr) {
      my_ts = it->second.entry_->proposed_ts_;
    }
  }

  // Record the remote proposal (may trigger completion check)
  UpdateDeadlineRecord(tid, src_shard, remote_ts, phase, nullptr);

  return my_ts;
}

bool SchedulerLuigi::HandleRemoteDeadlineConfirm(uint64_t tid,
                                                 uint32_t src_shard,
                                                 uint64_t new_ts) {
  //===========================================================================
  // Phase 2 confirmation - remote shard has repositioned.
  // This is essentially a phase-2 proposal.
  //===========================================================================

  Log_info("Luigi HandleRemoteDeadlineConfirm: tid=%lu from shard %u ts=%lu",
           tid, src_shard, new_ts);

  // Handle as phase 2 proposal
  HandleRemoteDeadlineProposal(tid, src_shard, new_ts, 2);

  return true;
}

void SchedulerLuigi::InitiateAgreement(std::shared_ptr<LuigiLogEntry> entry) {
  uint64_t tid = entry->tid_;
  uint64_t my_ts = entry->proposed_ts_;

  Log_info("Luigi InitiateAgreement: tid=%lu, my_ts=%lu, remote_shards=%zu",
           tid, my_ts, entry->remote_shards_.size());

  UpdateDeadlineRecord(tid, shard_id_, my_ts, 1, entry);

  if (!commo_) {
    Log_warn("Luigi InitiateAgreement: No commo available");
    for (uint32_t remote_shard : entry->remote_shards_) {
      UpdateDeadlineRecord(tid, remote_shard, my_ts, 1, nullptr);
    }
    return;
  }

  // Broadcast to all involved shard leaders
  auto luigi_commo = dynamic_cast<LuigiCommo *>(commo_);
  if (luigi_commo) {
    // Phase 2: Queue for batching instead of immediate broadcast
    QueueDeadlineProposal(tid, my_ts, entry->remote_shards_);
    Log_info("Luigi InitiateAgreement: queued proposal for batching (tid=%lu, "
             "ts=%lu, shards=%zu)",
             tid, my_ts, entry->remote_shards_.size());
  } else {
    Log_warn("Luigi InitiateAgreement: commo_ is not LuigiCommo");
  }
}

void SchedulerLuigi::SendRepositionConfirmations(
    std::shared_ptr<LuigiLogEntry> entry) {
  uint64_t tid = entry->tid_;
  uint64_t new_ts = entry->proposed_ts_;

  Log_info("Luigi SendRepositionConfirmations: tid=%lu, new_ts=%lu", tid,
           new_ts);

  if (!commo_) {
    Log_warn("Luigi SendRepositionConfirmations: No commo available");
    return;
  }

  // Use commo_ broadcast helper for DeadlineConfirm
  auto luigi_commo = dynamic_cast<LuigiCommo *>(commo_);
  if (luigi_commo) {
    // Phase 2: Queue for batching instead of immediate broadcast
    QueueDeadlineConfirmation(entry->tid_, entry->agreed_ts_,
                              entry->remote_shards_);
    Log_info("Luigi SendRepositionConfirmations: queued confirmation for "
             "batching (tid=%lu, ts=%lu, shards=%zu)",
             entry->tid_, entry->agreed_ts_, entry->remote_shards_.size());
  } else {
    Log_warn("Luigi SendRepositionConfirmations: commo_ is not LuigiCommo");
  }

  {
    std::lock_guard<std::mutex> lock(deadline_queue_mutex_);
    deadline_queue_.erase(tid);
  }
}

//=============================================================================
// WATERMARK MANAGEMENT
//=============================================================================

void SchedulerLuigi::UpdateLocalWatermark(uint32_t worker_id, uint64_t ts) {
  std::lock_guard<std::mutex> lock(watermark_mutex_);

  if (worker_id >= watermarks_.size()) {
    // Auto-resize if needed (though should be set by SetWorkerCount)
    watermarks_.resize(worker_id + 1, 0);
  }

  // Watermarks must be monotonic
  if (ts > watermarks_[worker_id]) {
    watermarks_[worker_id] = ts;
    // Log_debug("Luigi Watermark: shard %d worker %d updated to %ld",
    // partition_id_, worker_id, ts);
  }
}

uint64_t SchedulerLuigi::GetGlobalWatermark(uint32_t shard_id,
                                            uint32_t worker_id) {
  std::lock_guard<std::mutex> lock(watermark_mutex_);

  if (shard_id == shard_id_) {
    if (worker_id < watermarks_.size()) {
      return watermarks_[worker_id];
    }
    return 0;
  }

  auto it = global_watermarks_.find(shard_id);
  if (it != global_watermarks_.end() && worker_id < it->second.size()) {
    return it->second[worker_id];
  }
  return 0; // Unknown
}

void SchedulerLuigi::HandleWatermarkExchange(
    uint32_t src_shard, const std::vector<int64_t> &remote_watermarks) {
  // Scope the lock to release before CheckPendingCommits to avoid deadlock
  // (CheckPendingCommits also acquires watermark_mutex_)
  {
    std::lock_guard<std::mutex> lock(watermark_mutex_);

    // Ensure we have storage for this shard's watermarks
    if (global_watermarks_.find(src_shard) == global_watermarks_.end()) {
      global_watermarks_[src_shard] = std::vector<uint64_t>(worker_count_, 0);
    }

    auto &target = global_watermarks_[src_shard];

    for (size_t i = 0; i < remote_watermarks.size(); i++) {
      uint64_t ts = (uint64_t)remote_watermarks[i];
      if (i < target.size() && ts > target[i]) {
        target[i] = ts;
      }
    }

    Log_info("HandleWatermarkExchange: shard %d received from shard %d, "
             "watermark[0]=%lu",
             shard_id_, src_shard, target.size() > 0 ? target[0] : 0);
  }

  // Check if any pending commits can now proceed (lock is released now)
  CheckPendingCommits();
}

std::vector<int64_t> SchedulerLuigi::GetLocalWatermarks() {
  std::lock_guard<std::mutex> lock(watermark_mutex_);
  std::vector<int64_t> result;
  result.reserve(watermarks_.size());
  for (uint64_t wm : watermarks_) {
    result.push_back((int64_t)wm);
  }
  return result;
}

bool SchedulerLuigi::CanCommit(uint64_t timestamp, uint32_t worker_id,
                               const std::vector<uint32_t> &involved_shards) {
  // NOTE: Caller must hold watermark_mutex_!

  // Log what we're checking
  std::string shards_str;
  for (auto s : involved_shards) {
    shards_str += std::to_string(s) + ",";
  }
  Log_debug("CanCommit: shard=%d checking txn ts=%lu for shards=[%s]",
            shard_id_, timestamp, shards_str.c_str());

  // Check: timestamp <= watermark[shard][worker] for ALL involved shards
  for (uint32_t shard : involved_shards) {
    auto it = global_watermarks_.find(shard);
    if (it == global_watermarks_.end()) {
      // No watermarks received from this shard yet
      // For local shard, check local watermarks
      if (shard == shard_id_) {
        if (worker_id >= watermarks_.size() ||
            timestamp > watermarks_[worker_id]) {
          Log_debug("CanCommit: FAIL - local watermark not ready");
          return false;
        }
      } else {
        // Remote shard hasn't sent watermarks yet
        Log_debug("CanCommit: FAIL - no watermarks from shard %d", shard);
        return false;
      }
    } else {
      if (worker_id >= it->second.size()) {
        Log_debug("CanCommit: FAIL - invalid worker_id");
        return false;
      }
      if (timestamp > it->second[worker_id]) {
        Log_debug("CanCommit: FAIL - shard %d watermark=%lu < ts=%lu", shard,
                  it->second[worker_id], timestamp);
        return false;
      }
    }
  }

  Log_debug("CanCommit: SUCCESS - all watermarks ready");
  return true; // All shards have advanced past this timestamp
}

void SchedulerLuigi::AddPendingCommit(
    uint64_t txn_id, uint64_t timestamp, uint32_t worker_id,
    const std::vector<uint32_t> &involved_shards,
    std::function<void()> reply_callback) {
  std::lock_guard<std::mutex> lock(pending_commits_mutex_);

  PendingCommit pending;
  pending.txn_id = txn_id;
  pending.timestamp = timestamp;
  pending.worker_id = worker_id;
  pending.involved_shards = involved_shards;
  pending.reply_callback = reply_callback;

  pending_commits_[txn_id] = std::move(pending);
  Log_debug("AddPendingCommit: txn=%lu ts=%lu waiting for watermarks", txn_id,
            timestamp);
}

void SchedulerLuigi::CheckPendingCommits() {
  std::lock_guard<std::mutex> pending_lock(pending_commits_mutex_);
  std::lock_guard<std::mutex> watermark_lock(watermark_mutex_);

  auto it = pending_commits_.begin();
  while (it != pending_commits_.end()) {
    auto &[txn_id, info] = *it;

    if (CanCommit(info.timestamp, info.worker_id, info.involved_shards)) {
      Log_info(
          "CheckPendingCommits: txn %lu can now commit (watermarks advanced)",
          txn_id);

      // Call the reply callback
      if (info.reply_callback) {
        info.reply_callback();
      }

      it = pending_commits_.erase(it);
    } else {
      ++it;
    }
  }
}

void SchedulerLuigi::SendWatermarksToCoordinator() {
  if (!commo_) {
    return;
  }

  // Get current watermarks
  std::vector<int64_t> current_wms;
  {
    std::lock_guard<std::mutex> lock(watermark_mutex_);
    current_wms.assign(watermarks_.begin(), watermarks_.end());

    // Update our own global watermarks so we can check them in CanCommit
    // This enables each shard to verify all involved shards (including itself)
    global_watermarks_[shard_id_] = watermarks_;
  }

  // Send to all shards (SendWatermarkToCoordinator already broadcasts to all)
  auto luigi_commo = dynamic_cast<LuigiCommo *>(commo_);
  if (luigi_commo) {
    luigi_commo->BroadcastWatermarks(shard_id_, current_wms);
    Log_debug("BroadcastWatermarks: shard=%d sent watermarks to all shards",
              shard_id_);
  }

  // Check if any pending commits can now proceed
  CheckPendingCommits();
}

// Helper to get all shard IDs except self for broadcasting
std::vector<uint32_t> SchedulerLuigi::GetAllShardIdsExceptSelf() const {
  std::vector<uint32_t> all_shards;
  auto config = Config::GetConfig();
  uint32_t num_shards = config->GetNumPartition();
  for (uint32_t i = 0; i < num_shards; ++i) {
    if (i != shard_id_) {
      all_shards.push_back(i);
    }
  }
  return all_shards;
}

//=============================================================================
// Replication Layer
//=============================================================================

// Enqueue entry for async replication by per-worker ReplicateTd thread
void SchedulerLuigi::Replicate(uint32_t worker_id,
                               std::shared_ptr<LuigiLogEntry> entry) {
  // Enqueue to per-worker replication queue (non-blocking)
  if (worker_id < replication_queues_.size() &&
      replication_queues_[worker_id]) {
    replication_queues_[worker_id]->enqueue(entry);
  } else {
    // Fallback: direct replication if queues not initialized
    DoReplicate(worker_id, entry);
  }
}

// Per-worker replication thread - batches entries and replicates with async
// quorum
void SchedulerLuigi::ReplicateTd(uint32_t worker_id) {
  Log_info("ReplicateTd started for worker %u (batched mode)", worker_id);

  constexpr size_t BATCH_SIZE = 100;
  constexpr auto BATCH_TIMEOUT = std::chrono::microseconds(500);

  std::vector<std::shared_ptr<LuigiLogEntry>> batch;
  batch.reserve(BATCH_SIZE);

  auto last_flush = std::chrono::steady_clock::now();

  while (running_.load()) {
    std::shared_ptr<LuigiLogEntry> entry;

    // Collect entries into batch
    while (batch.size() < BATCH_SIZE &&
           replication_queues_[worker_id]->try_dequeue(entry)) {
      batch.push_back(entry);
    }

    // Flush batch if size threshold or timeout
    auto now = std::chrono::steady_clock::now();
    bool should_flush = !batch.empty() && (batch.size() >= BATCH_SIZE ||
                                           (now - last_flush) > BATCH_TIMEOUT);

    if (should_flush) {
      DoBatchReplicate(worker_id, batch);
      batch.clear();
      last_flush = now;
    } else if (batch.empty()) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  }

  // Drain remaining entries on shutdown
  std::shared_ptr<LuigiLogEntry> entry;
  while (replication_queues_[worker_id]->try_dequeue(entry)) {
    batch.push_back(entry);
  }
  if (!batch.empty()) {
    DoBatchReplicate(worker_id, batch);
  }

  Log_info("ReplicateTd stopped for worker %u", worker_id);
}

// Batch replication with async quorum handling
void SchedulerLuigi::DoBatchReplicate(
    uint32_t worker_id,
    const std::vector<std::shared_ptr<LuigiLogEntry>> &batch) {
  if (batch.empty())
    return;

  // Prepare batch data
  std::vector<int64_t> slot_ids;
  std::vector<int64_t> txn_ids;
  std::vector<int64_t> timestamps;
  std::vector<std::string> log_entries;
  uint64_t max_ts = 0;

  slot_ids.reserve(batch.size());
  txn_ids.reserve(batch.size());
  timestamps.reserve(batch.size());
  log_entries.reserve(batch.size());

  {
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    if (worker_id >= paxos_streams_.size()) {
      paxos_streams_.resize(worker_id + 1);
    }

    for (const auto &entry : batch) {
      uint64_t commit_ts = entry->agreed_ts_;
      max_ts = std::max(max_ts, commit_ts);

      // Serialize entry
      std::string log_data = "LUIGI_TXN:" + std::to_string(entry->tid_) + ":" +
                             std::to_string(commit_ts) + ":" +
                             std::to_string(entry->txn_type_) + ":" +
                             std::to_string(entry->ops_.size());
      for (const auto &op : entry->ops_) {
        log_data += ":" + std::to_string(op.op_type) + ":" + op.key;
        if (op.op_type == LUIGI_OP_WRITE) {
          log_data += ":" + op.value;
        }
      }

      // Assign slot and append to local log
      uint64_t slot_id = paxos_streams_[worker_id].next_slot_++;
      LogEntry le{slot_id, entry->tid_, commit_ts, log_data};
      paxos_streams_[worker_id].Append(le);

      slot_ids.push_back(slot_id);
      txn_ids.push_back(entry->tid_);
      timestamps.push_back(commit_ts);
      log_entries.push_back(log_data);
    }
  }

  // For multi-replica: send batch to followers with async quorum
  if (num_replicas_ > 1 && !follower_sites_.empty() && commo_ != nullptr) {
    auto luigi_commo = dynamic_cast<LuigiCommo *>(commo_);
    if (luigi_commo != nullptr) {
      uint32_t n_followers = follower_sites_.size();
      uint32_t quorum_needed = (num_replicas_ / 2);
      if (quorum_needed == 0)
        quorum_needed = 1;

      // Atomic counter for quorum tracking
      auto ack_count = std::make_shared<std::atomic<uint32_t>>(0);
      auto quorum_reached = std::make_shared<std::atomic<bool>>(false);
      auto batch_max_ts = max_ts;
      auto captured_worker_id = worker_id;
      auto self = this;

      // Get prev_committed_slot
      int64_t prev_committed = 0;
      {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        if (worker_id < paxos_streams_.size() && !slot_ids.empty()) {
          prev_committed = slot_ids.front() - 1;
        }
      }

      // Send to all followers
      for (uint32_t follower_site : follower_sites_) {
        luigi_commo->BatchReplicateAsync(
            follower_site, worker_id, prev_committed, slot_ids, txn_ids,
            timestamps, log_entries,
            [ack_count, quorum_needed, quorum_reached, batch_max_ts,
             captured_worker_id,
             self](bool ok, rrr::i32 status, rrr::i64 last_slot) {
              if (ok && status == 0) {
                uint32_t count = ack_count->fetch_add(1) + 1;
                // Check if quorum just reached (only update watermark once)
                if (count >= quorum_needed && !quorum_reached->exchange(true)) {
                  // Quorum reached! Update watermark async
                  std::lock_guard<std::mutex> lock(self->watermark_mutex_);
                  if (captured_worker_id >= self->watermarks_.size()) {
                    self->watermarks_.resize(captured_worker_id + 1, 0);
                  }
                  self->watermarks_[captured_worker_id] = std::max(
                      self->watermarks_[captured_worker_id], batch_max_ts);
                  Log_debug("DoBatchReplicate: quorum reached, watermark=%lu",
                            batch_max_ts);
                }
              }
            });
      }

      // For single-replica or if quorum callback handles it, we're done
      // The watermark update happens async in the callback
      Log_debug("DoBatchReplicate: sent batch of %zu entries to %zu followers",
                batch.size(), follower_sites_.size());
    }
  } else {
    // Single-replica mode: update watermark immediately
    std::lock_guard<std::mutex> lock(watermark_mutex_);
    if (worker_id >= watermarks_.size()) {
      watermarks_.resize(worker_id + 1, 0);
    }
    watermarks_[worker_id] = std::max(watermarks_[worker_id], max_ts);
    Log_debug("DoBatchReplicate: single-replica, watermark=%lu", max_ts);
  }
}

// Legacy single-entry replication (kept for fallback)
void SchedulerLuigi::DoReplicate(uint32_t worker_id,
                                 std::shared_ptr<LuigiLogEntry> entry) {
  std::vector<std::shared_ptr<LuigiLogEntry>> batch{entry};
  DoBatchReplicate(worker_id, batch);
}

// Append a log entry to follower's stream (called by Replicate RPC handler)
void SchedulerLuigi::AppendToLog(uint32_t worker_id, uint64_t slot_id,
                                 uint64_t txn_id, uint64_t timestamp,
                                 const std::string &log_data) {
  std::lock_guard<std::mutex> lock(paxos_mutex_);

  // Ensure streams are initialized
  if (worker_id >= paxos_streams_.size()) {
    paxos_streams_.resize(worker_id + 1);
  }

  LogEntry le{slot_id, txn_id, timestamp, log_data};
  paxos_streams_[worker_id].Append(le);

  Log_info("AppendToLog (follower): shard=%d worker=%d slot=%lu txn=%lu",
           shard_id_, worker_id, slot_id, txn_id);
}

// Batch append log entries to follower's stream (BatchReplicate RPC handler)
uint64_t
SchedulerLuigi::BatchAppendToLog(uint32_t worker_id,
                                 const std::vector<uint64_t> &slot_ids,
                                 const std::vector<uint64_t> &txn_ids,
                                 const std::vector<uint64_t> &timestamps,
                                 const std::vector<std::string> &log_entries) {

  std::lock_guard<std::mutex> lock(paxos_mutex_);

  // Ensure streams are initialized
  if (worker_id >= paxos_streams_.size()) {
    paxos_streams_.resize(worker_id + 1);
  }

  uint64_t last_slot = 0;
  for (size_t i = 0; i < slot_ids.size(); i++) {
    LogEntry le{slot_ids[i], txn_ids[i], timestamps[i], log_entries[i]};
    paxos_streams_[worker_id].Append(le);
    last_slot = slot_ids[i];
  }

  Log_info("BatchAppendToLog (follower): shard=%d worker=%d entries=%zu "
           "last_slot=%lu",
           shard_id_, worker_id, slot_ids.size(), last_slot);

  return last_slot;
}

//=============================================================================
// Watermark Management
//=============================================================================

void SchedulerLuigi::WatermarkTd() {
  while (running_) {
    SendWatermarksToCoordinator();
    // Exchange every 100ms (increased from 50ms for reduced RPC overhead)
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

//=============================================================================
// Server Initialization Methods (moved from inline in header)
//=============================================================================

void SchedulerLuigi::SetWorkerCount(uint32_t count) {
  worker_count_ = count;
  std::lock_guard<std::mutex> lock(watermark_mutex_);
  watermarks_.assign(count, 0);

  // PHASE 4: Initialize Paxos streams after worker count is set
  InitializePaxosStreams();
}

void SchedulerLuigi::SetReplicationCallback(
    LuigiExecutor::ReplicationCallback cb) {
  executor_.SetReplicationCallback(std::move(cb));
}

void SchedulerLuigi::SetStateMachine(std::shared_ptr<LuigiStateMachine> sm) {
  executor_.SetStateMachine(std::move(sm));
}

void SchedulerLuigi::EnableStateMachineMode(bool enable) {
  executor_.EnableStateMachineMode(enable);
}
void SchedulerLuigi::SetPartitionId(uint32_t shard_id) {
  shard_id_ = shard_id;
  executor_.SetPartitionId(shard_id);
}

//=============================================================================
// PHASE 2: RPC BATCHING IMPLEMENTATION
//=============================================================================

void SchedulerLuigi::QueueDeadlineProposal(
    uint64_t tid, uint64_t proposed_ts,
    const std::vector<uint32_t> &remote_shards) {
  std::lock_guard<std::mutex> lock(batch_mutex_);

  pending_proposals_.tids.push_back(tid);
  pending_proposals_.proposed_ts.push_back(proposed_ts);

  // Union remote shards (avoid duplicates)
  for (uint32_t shard : remote_shards) {
    if (std::find(pending_proposals_.remote_shards.begin(),
                  pending_proposals_.remote_shards.end(),
                  shard) == pending_proposals_.remote_shards.end()) {
      pending_proposals_.remote_shards.push_back(shard);
    }
  }

  // Check if immediate flush needed (batch interval exceeded OR batch size
  // exceeded)
  auto now = std::chrono::steady_clock::now();
  auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        now - last_flush_time_)
                        .count();

  bool should_flush = (elapsed_us >= BATCH_FLUSH_INTERVAL_US) ||
                      (pending_proposals_.tids.size() >= BATCH_SIZE_THRESHOLD);

  if (should_flush) {
    FlushProposalBatch();
    FlushConfirmBatch();
    last_flush_time_ = now;
  }
}

void SchedulerLuigi::QueueDeadlineConfirmation(
    uint64_t tid, uint64_t agreed_ts,
    const std::vector<uint32_t> &remote_shards) {
  std::lock_guard<std::mutex> lock(batch_mutex_);

  pending_confirms_.tids.push_back(tid);
  pending_confirms_.agreed_ts.push_back(agreed_ts);

  // Union remote shards
  for (uint32_t shard : remote_shards) {
    if (std::find(pending_confirms_.remote_shards.begin(),
                  pending_confirms_.remote_shards.end(),
                  shard) == pending_confirms_.remote_shards.end()) {
      pending_confirms_.remote_shards.push_back(shard);
    }
  }

  // Check if immediate flush needed (batch interval exceeded OR batch size
  // exceeded)
  auto now = std::chrono::steady_clock::now();
  auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        now - last_flush_time_)
                        .count();

  bool should_flush = (elapsed_us >= BATCH_FLUSH_INTERVAL_US) ||
                      (pending_confirms_.tids.size() >= BATCH_SIZE_THRESHOLD);

  if (should_flush) {
    FlushProposalBatch();
    FlushConfirmBatch();
    last_flush_time_ = now;
  }
}

void SchedulerLuigi::FlushProposalBatch() {
  // Must be called with batch_mutex_ held
  if (pending_proposals_.tids.empty())
    return;

  auto luigi_commo = dynamic_cast<LuigiCommo *>(commo_);
  if (luigi_commo) {
    // Convert to vectors of i64 for RPC
    std::vector<rrr::i64> tids(pending_proposals_.tids.begin(),
                               pending_proposals_.tids.end());
    std::vector<rrr::i64> proposed_ts(pending_proposals_.proposed_ts.begin(),
                                      pending_proposals_.proposed_ts.end());

    // Get current watermarks to piggyback (replaces separate WatermarkExchange)
    std::vector<rrr::i64> watermarks = GetLocalWatermarks();

    luigi_commo->BroadcastDeadlineBatchPropose(
        tids, shard_id_, proposed_ts, watermarks,
        pending_proposals_.remote_shards);

    Log_debug("Flushed %zu proposals + watermarks to %zu shards",
              pending_proposals_.tids.size(),
              pending_proposals_.remote_shards.size());
  }

  // Clear batch
  pending_proposals_.tids.clear();
  pending_proposals_.proposed_ts.clear();
  pending_proposals_.remote_shards.clear();
}

void SchedulerLuigi::FlushConfirmBatch() {
  // Must be called with batch_mutex_ held
  if (pending_confirms_.tids.empty())
    return;

  auto luigi_commo = dynamic_cast<LuigiCommo *>(commo_);
  if (luigi_commo) {
    // Convert to vectors of i64 for RPC
    std::vector<rrr::i64> tids(pending_confirms_.tids.begin(),
                               pending_confirms_.tids.end());
    std::vector<rrr::i64> agreed_ts(pending_confirms_.agreed_ts.begin(),
                                    pending_confirms_.agreed_ts.end());

    luigi_commo->BroadcastDeadlineBatchConfirm(tids, shard_id_, agreed_ts,
                                               pending_confirms_.remote_shards);

    Log_info("Flushed %zu confirmations to %zu shards",
             pending_confirms_.tids.size(),
             pending_confirms_.remote_shards.size());
  }

  // Clear batch
  pending_confirms_.tids.clear();
  pending_confirms_.agreed_ts.clear();
  pending_confirms_.remote_shards.clear();
}

void SchedulerLuigi::BatchFlushLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(
        std::chrono::microseconds(BATCH_FLUSH_INTERVAL_US));

    {
      std::lock_guard<std::mutex> lock(batch_mutex_);
      FlushProposalBatch();
      FlushConfirmBatch();
      last_flush_time_ = std::chrono::steady_clock::now();
    }
  }
}

//=============================================================================
// PHASE 4: PAXOS REPLICATION IMPLEMENTATION
//=============================================================================

void SchedulerLuigi::InitializePaxosStreams() {
  //===========================================================================
  // PHASE 4: Paxos initialization now handled by add_log_to_nc
  //
  // When running with Paxos workers (via setup() in paxos_main_helper.cc),
  // add_log_to_nc will automatically route to the correct Paxos partition.
  //
  // In standalone Luigi mode (without Paxos workers), add_log_to_nc will
  // silently return and we rely on in-memory watermarks only.
  //===========================================================================
  Log_info("Luigi: Phase 4 using add_log_to_nc for Paxos replication (shard=%u "
           "workers=%u)",
           shard_id_, worker_count_);
}

void SchedulerLuigi::ReplayPaxosLog(uint32_t worker_id) {
  //===========================================================================
  // PHASE 4: Log replay is handled by the global Paxos infrastructure
  //
  // When Paxos workers are initialized (via setup() in paxos_main_helper.cc),
  // they automatically replay their logs on startup. Luigi receives replayed
  // entries through registered apply callbacks.
  //
  // In standalone mode, there's no durable log to replay.
  //===========================================================================
  Log_info(
      "Luigi: ReplayPaxosLog called for worker=%u (handled by pxs_workers_g)",
      worker_id);
}

void SchedulerLuigi::CheckpointWatermarks() {
  //===========================================================================
  // PHASE 4: Checkpointing is handled by the global Paxos infrastructure
  //
  // The Paxos workers (pxs_workers_g) manage their own log truncation
  // via FreeSlots(). Luigi just tracks watermarks locally.
  //===========================================================================
  Log_info("Luigi: CheckpointWatermarks for shard=%u (watermarks are local "
           "tracking only)",
           shard_id_);
}

} // namespace janus
