#include "luigi_scheduler.h"

#include "luigi_client.h" // For LuigiClient eRPC coordination
#include "deptran/__dep__.h" // For logging macros

#include <chrono>
#include <functional>
#include <iostream>

// Fix macro conflict between deptran/constants.h and mako/lib/common.h
#undef SUCCESS

namespace janus {

//=============================================================================
// Construction / Destruction
//=============================================================================

SchedulerLuigi::SchedulerLuigi() {
  Log_info("[SCHEDULER-CTOR] SchedulerLuigi constructor called");
  
  // Luigi has its own execution model - no TxLogServer needed
  
  // Set executor's scheduler reference so it can call back for RPC coordination
  executor_.SetScheduler(this);
  
  Log_info("[SCHEDULER-CTOR] SchedulerLuigi constructor completed");
}

SchedulerLuigi::~SchedulerLuigi() { Stop(); }

//=============================================================================
// Thread Management
//=============================================================================

void SchedulerLuigi::Start() {
  Log_info("[SCHEDULER-START] Starting scheduler threads...");
  
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    Log_warn("[SCHEDULER-START] Scheduler already running");
    return;
  }

  Log_info("[SCHEDULER-START] Creating hold/release thread...");
  hold_thread_ = new std::thread(&SchedulerLuigi::HoldReleaseTd, this);
  
  Log_info("[SCHEDULER-START] Creating execution thread...");
  exec_thread_ = new std::thread(&SchedulerLuigi::ExecTd, this);
  
  Log_info("[SCHEDULER-START] Creating watermark thread...");
  watermark_thread_ = new std::thread(&SchedulerLuigi::WatermarkTd, this);
  
  Log_info("[SCHEDULER-START] All threads started successfully");
}

void SchedulerLuigi::Stop() {
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false))
    return;

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
    uint64_t txn_id, uint64_t expected_time, uint32_t txn_type,
    const std::vector<LuigiOp> &ops,
    const std::map<int32_t, std::string> &working_set,
    const std::vector<uint32_t> &involved_shards,
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
  entry->txn_type_ = txn_type; // Set transaction type for state machine execution
  entry->ops_ = ops;
  entry->working_set_ = working_set; // TPC-C parameters

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
    if (shard_id != partition_id_) {
      entry->remote_shards_.push_back(shard_id);
    }
  }

  // Extract keys for conflict detection
  // We use a simple hash of (table_id, key) as the conflict key
  for (const auto &op : ops) {
    // Simple key hash: combine table_id and first few bytes of key
    int32_t conflict_key = op.table_id;
    if (op.key.size() >= 4) {
      conflict_key ^= *reinterpret_cast<const uint32_t *>(op.key.data());
    }
    entry->local_keys_.push_back(conflict_key);
  }

  // Enqueue to incoming queue (lock-free, thread-safe)
  incoming_txn_queue_.enqueue(entry);
  
  Log_info("[SCHEDULER-QUEUE] TXN-%lu: Enqueued (expected_time=%lu, ops=%zu, remote_shards=%zu)", 
           txn_id, expected_time, ops.size(), entry->remote_shards_.size());
}

//=============================================================================
// LuigiDispatch: Original Entry Point (for deptran-style compatibility)
//=============================================================================

void SchedulerLuigi::LuigiDispatch(
    txnid_t tx_id, std::shared_ptr<Marshallable> cmd, uint64_t send_time,
    uint32_t bound, const std::vector<int32_t> &local_keys,
    std::function<void(const TxnOutput &)> reply_cb) {
  auto entry = std::make_shared<LuigiLogEntry>(tx_id);
  entry->cmd_ = cmd;
  entry->send_time_ = send_time;
  entry->bound_ = bound;
  entry->proposed_ts_ = send_time + bound;
  entry->local_keys_ = local_keys;

  // Wrap the old-style callback
  entry->reply_cb_ = [reply_cb](int status, uint64_t commit_ts,
                                const std::vector<std::string> &read_results) {
    TxnOutput output;
    // Convert read_results to TxnOutput if needed
    if (reply_cb) {
      reply_cb(output);
    }
  };

  uint64_t now = GetMicrosecondTimestamp();
  entry->owd_ = (now > send_time) ? (uint32_t)(now - send_time) : 1000;

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
  // Validate state
  if (entry->agree_status_.load() != LUIGI_AGREE_FLUSHING) {
    Log_error("Luigi RequeueForReposition: txn %lu has wrong status %u "
              "(expected AGREE_FLUSHING)",
              entry->tid_, entry->agree_status_.load());
    return;
  }

  Log_info("Luigi RequeueForReposition: txn %lu going back to queue with "
           "timestamp %lu",
           entry->tid_, entry->proposed_ts_);

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
  Log_info("[HOLD-RELEASE-THREAD] ===== THREAD STARTED =====");
  
  std::shared_ptr<LuigiLogEntry> entries[256]; // bulk dequeue buffer

  try {
    Log_info("[HOLD-RELEASE-THREAD] Entering try block, starting main loop");
    int iterations = 0;
    while (running_) {
    iterations++;
    if (iterations <= 5 || iterations % 1000 == 0) {
      Log_info("[HOLD-RELEASE-THREAD] Loop iteration %d", iterations);
    }
    
    uint64_t now = GetMicrosecondTimestamp();

    //-------------------------------------------------------------------------
    // Phase 1: Pull from incoming_txn_queue_, do conflict check, add to
    // priority_queue_
    //-------------------------------------------------------------------------
    size_t queue_size_approx = incoming_txn_queue_.size_approx();
    if (iterations <= 10) {
      Log_info("[HOLD-RELEASE-THREAD] Iteration %d: queue size_approx=%zu", iterations, queue_size_approx);
    }
    
    // FIX: try_dequeue_bulk doesn't work with shared_ptr array - use loop instead
    size_t cnt = 0;
    try {
      while (cnt < 256 && incoming_txn_queue_.try_dequeue(entries[cnt])) {
        cnt++;
      }
    } catch (...) {
      Log_error("[HOLD-RELEASE-THREAD] Exception during dequeue at iteration %d", iterations);
      throw;
    }
    
    if (iterations <= 10) {
      Log_info("[HOLD-RELEASE-THREAD] Iteration %d: dequeued cnt=%zu items (queue had ~%zu)", 
               iterations, cnt, queue_size_approx);
    }
    if (cnt > 0) {
      Log_info("[HOLD-RELEASE-THREAD] *** Dequeued %zu txns from incoming_txn_queue ***", cnt);
    }
    for (size_t i = 0; i < cnt; i++) {
      try {
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

        Log_info("Luigi HoldReleaseTd: Repositioning txn %lu at new timestamp "
                 "%lu (requeue #%u)",
                 entry->tid_, entry->proposed_ts_, entry->requeue_count_);

        // Insert at new position
        priority_queue_[{entry->proposed_ts_, txn_key}] = entry;
        continue; // Skip normal conflict detection
      }

      //-----------------------------------------------------------------------
      // Normal path: NEW txn entering for the first time
      //-----------------------------------------------------------------------

      // CONFLICT DETECTION (from Algorithm 1, line 1-4 in paper):
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
      // This is the LEADER PRIVILEGE: we can bump the timestamp
      if (entry->proposed_ts_ <= max_last_released) {
        entry->proposed_ts_ = max_last_released + 1;
      }

      // Insert into priority_queue_ (sorted by timestamp, then txn_id)
      priority_queue_[{entry->proposed_ts_, txn_key}] = entry;
      } catch (...) {
        Log_error("[HOLD-RELEASE-THREAD] Exception processing entry in Phase 1");
        throw;
      }
    }

    //-------------------------------------------------------------------------
    // Phase 2: Release txns whose deadline has passed -> ready_txn_queue_
    //-------------------------------------------------------------------------
    try {
      while (!priority_queue_.empty()) {
        auto it = priority_queue_.begin();
        uint64_t deadline = it->first.first;

        if (now < deadline) {
          // Earliest deadline not yet reached, stop releasing
          break;
        }

        // Deadline reached! Release this entry
        auto entry = it->second;
        priority_queue_.erase(it);

        // Update lastReleasedDeadlines for all keys this txn touches
        for (auto &k : entry->local_keys_) {
          if (last_released_deadlines_[k] < entry->proposed_ts_) {
            last_released_deadlines_[k] = entry->proposed_ts_;
          }
        }

        // Hand off to execution thread
        ready_txn_queue_.enqueue(entry);
        
        if (entry->tid_ % 50 == 0) {
          Log_info("[HOLD-RELEASE-THREAD] TXN-%lu: Released to ready_txn_queue_ (ts=%lu)",
                   entry->tid_, entry->proposed_ts_);
        }
      }
    } catch (...) {
      Log_error("[HOLD-RELEASE-THREAD] Exception in Phase 2 (priority queue processing) at iteration %d", iterations);
      throw;
    }

      //-------------------------------------------------------------------------
      // Small sleep to avoid busy-waiting when queues are empty
      //-------------------------------------------------------------------------
      if (cnt == 0 && priority_queue_.empty()) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }
  } catch (int e) {
    Log_info("[HOLD-RELEASE-THREAD] Coroutine unwinding (int=%d)", e);
  } catch (const std::exception& e) {
    Log_error("[HOLD-RELEASE-THREAD] Exception: %s", e.what());
  } catch (...) {
    Log_error("[HOLD-RELEASE-THREAD] Unknown exception");
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
  Log_info("[EXEC-THREAD] Started");
  std::shared_ptr<LuigiLogEntry> entries[64];
  int txns_executed = 0;
  int last_log = 0;

  try {
    while (running_) {
    size_t cnt = ready_txn_queue_.try_dequeue_bulk(entries, 64);
    
    if (cnt > 0) {
      txns_executed += cnt;
      if (txns_executed - last_log >= 50) {
        Log_info("[EXEC-THREAD] Dequeued %zu txns from ready queue (total executed: %d)",
                 cnt, txns_executed);
        last_log = txns_executed;
      }
    }

    for (size_t i = 0; i < cnt; i++) {
      auto &entry = entries[i];

      if (entry->tid_ % 50 == 0) {
        Log_info("[EXEC-THREAD] TXN-%lu: Executing (ts=%lu)", entry->tid_, entry->proposed_ts_);
      }

      // Delegate to executor for clean separation of concerns
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
        // Multi-shard txn: InitiateAgreement() was called, RPCs sent.
        // The txn is now waiting for async agreement completion.
        // When all proposals arrive, UpdateDeadlineRecord() will:
        //   - Determine Case 1/2/3
        //   - Set agree_status_ appropriately
        //   - Re-enqueue to ready_txn_queue_ if ready to execute
        // Nothing to do here - just let it wait.
        Log_debug("Luigi ExecTd: txn %lu waiting for agreement (INIT)",
                  entry->tid_);
        break;

      case LUIGI_AGREE_FLUSHING:
        // Case 3: Need to reposition in priority queue
        // Execute() updated proposed_ts_ to agreed_ts_
        // Requeue for re-processing at new timestamp
        Log_info("Luigi ExecTd: txn %lu needs reposition, requeuing to "
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
        Log_info(
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
    }

    if (cnt == 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
  } catch (int e) {
    Log_info("[EXEC-THREAD] Coroutine unwinding (int=%d)", e);
  } catch (const std::exception& e) {
    Log_error("[EXEC-THREAD] Exception: %s", e.what());
  } catch (...) {
    Log_error("[EXEC-THREAD] Unknown exception");
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

  DeadlineQItem &dqi = deadline_queue_[tid];

  // If entry provided (our local txn), initialize expected count
  if (entry != nullptr && dqi.entry_ == nullptr) {
    dqi.entry_ = entry;
    dqi.expected_count_ =
        entry->remote_shards_.size() + 1; // remotes + ourselves
    Log_info("Luigi UpdateDeadlineRecord: tid=%lu initialized, expecting %u "
             "proposals",
             tid, dqi.expected_count_);
  }

  // Record this proposal (if not already received from this shard)
  if (src_shard < DeadlineQItem::MAX_SHARDS && !dqi.received_[src_shard]) {
    dqi.deadlines_[src_shard] = proposed_ts;
    dqi.phases_[src_shard] = phase;
    dqi.received_[src_shard] = true;
    dqi.item_count_++;

    Log_info(
        "Luigi UpdateDeadlineRecord: tid=%lu from shard %u ts=%lu phase=%u, "
        "now have %u/%u proposals",
        tid, src_shard, proposed_ts, phase, dqi.item_count_,
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

    Log_info("Luigi UpdateDeadlineRecord: tid=%lu COMPLETE - agreed_ts=%lu, "
             "my_ts=%lu, all_match=%d",
             tid, agreed_ts, my_ts, all_match);

    // Determine which case we're in
    if (all_match) {
      // Case 1: All proposals match - we're done!
      dqi.entry_->agreed_ts_ = agreed_ts;
      dqi.entry_->agree_status_.store(LUIGI_AGREE_COMPLETE);

      Log_info("Luigi: tid=%lu Case 1 - all match at ts=%lu", tid, agreed_ts);

      // Enqueue for execution completion
      ready_txn_queue_.enqueue(dqi.entry_);

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
        if (i == partition_id_)
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

      // Entry will be requeued by ExecTd after it notices AGREE_FLUSHING
      ready_txn_queue_.enqueue(dqi.entry_);
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

  UpdateDeadlineRecord(tid, partition_id_, my_ts, 1, entry);

  if (!luigi_client_) {
    Log_warn("Luigi InitiateAgreement: No LuigiClient available");
    for (uint32_t remote_shard : entry->remote_shards_) {
      UpdateDeadlineRecord(tid, remote_shard, my_ts, 1, nullptr);
    }
    return;
  }

  for (uint32_t remote_shard : entry->remote_shards_) {
    luigi_client_->InvokeDeadlinePropose(
        remote_shard, tid, my_ts, 1, [](char *) {},
        []() { Log_warn("InitiateAgreement: RPC error"); });
    Log_info("Luigi InitiateAgreement: sent proposal to shard %u",
             remote_shard);
  }
}

void SchedulerLuigi::SendRepositionConfirmations(
    std::shared_ptr<LuigiLogEntry> entry) {
  uint64_t tid = entry->tid_;
  uint64_t new_ts = entry->proposed_ts_;

  Log_info("Luigi SendRepositionConfirmations: tid=%lu, new_ts=%lu", tid,
           new_ts);

  if (!luigi_client_) {
    Log_warn("Luigi SendRepositionConfirmations: No LuigiClient available");
    return;
  }

  for (uint32_t remote_shard : entry->remote_shards_) {
    luigi_client_->InvokeDeadlineConfirm(
        remote_shard, tid, new_ts, [](char *) {},
        []() { Log_warn("SendRepositionConfirmations: RPC error"); });
    Log_info("Luigi SendRepositionConfirmations: sent phase-2 to shard %u",
             remote_shard);
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

  if (shard_id == partition_id_) {
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
  std::lock_guard<std::mutex> lock(watermark_mutex_);

  // Convert int64_t to uint64_t
  std::vector<uint64_t> &target = global_watermarks_[src_shard];
  if (target.size() < remote_watermarks.size()) {
    target.resize(remote_watermarks.size());
  }

  for (size_t i = 0; i < remote_watermarks.size(); i++) {
    uint64_t ts = (uint64_t)remote_watermarks[i];
    if (ts > target[i]) {
      target[i] = ts;
    }
  }

  // Log_debug("Luigi Watermark: received update from shard %d", src_shard);
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

void SchedulerLuigi::BroadcastWatermarks() {
  std::vector<int64_t> current_wms = GetLocalWatermarks();

  if (!luigi_client_) {
    return;
  }

  std::vector<uint64_t> watermarks_u64;
  for (int64_t wm : current_wms) {
    watermarks_u64.push_back(static_cast<uint64_t>(wm));
  }

  // TODO: Broadcast to all known shards
  // Need shard list from configuration
  Log_debug("BroadcastWatermarks: num_watermarks=%zu", watermarks_u64.size());
}

//=============================================================================
// REPLICATION
//=============================================================================

void SchedulerLuigi::Replicate(uint32_t worker_id,
                               const std::shared_ptr<LuigiLogEntry> &entry) {
  // In a real Tiga implementation, this would submit the entry to the
  // Paxos stream corresponding to worker_id.
  // For now, we simulate success and update the watermark immediately.

  if (entry->tid_ % 50 == 0) {
    Log_info("[SCHEDULER-REPLICATE] TXN-%lu: Replicating on worker %d at ts=%lu",
             entry->tid_, worker_id, entry->agreed_ts_);
  }

  // TODO: Integrate real Paxos here if available

  // Update watermark immediately (simulating instant replication for now)
  UpdateLocalWatermark(worker_id, entry->agreed_ts_);
  
  if (entry->tid_ % 50 == 0) {
    Log_info("[SCHEDULER-REPLICATE] TXN-%lu: Watermark updated", entry->tid_);
  }
}

void SchedulerLuigi::WatermarkTd() {
  while (running_) {
    BroadcastWatermarks();
    // Exchange every 50ms
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

} // namespace janus
