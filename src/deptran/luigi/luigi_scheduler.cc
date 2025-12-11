#include "luigi_scheduler.h"

#include <chrono>
#include <iostream>
#include <functional>

namespace janus {

//=============================================================================
// Construction / Destruction
//=============================================================================

SchedulerLuigi::SchedulerLuigi() : SchedulerClassic() {
  // Nothing special needed here; vectors/maps init lazily
}

SchedulerLuigi::~SchedulerLuigi() { Stop(); }

//=============================================================================
// Thread Management
//=============================================================================

void SchedulerLuigi::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) return;

  hold_thread_ = new std::thread(&SchedulerLuigi::HoldReleaseTd, this);
  exec_thread_ = new std::thread(&SchedulerLuigi::ExecTd, this);
}

void SchedulerLuigi::Stop() {
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) return;

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
}

//=============================================================================
// Utility
//=============================================================================

uint64_t SchedulerLuigi::GetMicrosecondTimestamp() {
  auto tse = std::chrono::system_clock::now().time_since_epoch();
  return (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(tse).count();
}

//=============================================================================
// LuigiDispatchFromRequest: Entry Point from server.cc
//
// Creates a LuigiLogEntry from parsed request data and enqueues it.
//=============================================================================

void SchedulerLuigi::LuigiDispatchFromRequest(
    uint64_t txn_id,
    uint64_t send_time,
    uint32_t bound,
    const std::vector<LuigiOp>& ops,
    std::function<void(int status, uint64_t commit_ts, const std::vector<std::string>& read_results)> reply_cb) {
  
  auto entry = std::make_shared<LuigiLogEntry>(txn_id);
  entry->send_time_ = send_time;
  entry->bound_ = bound;
  entry->proposed_ts_ = send_time + bound;
  entry->ops_ = ops;
  entry->reply_cb_ = reply_cb;

  // Extract keys for conflict detection
  // We use a simple hash of (table_id, key) as the conflict key
  for (const auto& op : ops) {
    // Simple key hash: combine table_id and first few bytes of key
    int32_t conflict_key = op.table_id;
    if (op.key.size() >= 4) {
      conflict_key ^= *reinterpret_cast<const uint32_t*>(op.key.data());
    }
    entry->local_keys_.push_back(conflict_key);
  }

  // Calculate one-way delay for diagnostics
  uint64_t now = GetMicrosecondTimestamp();
  entry->owd_ = (now > send_time) ? (uint32_t)(now - send_time) : 1000;

  // Enqueue to incoming queue (lock-free, thread-safe)
  incoming_txn_queue_.enqueue(entry);
}

//=============================================================================
// LuigiDispatch: Original Entry Point (for deptran-style compatibility)
//=============================================================================

void SchedulerLuigi::LuigiDispatch(txnid_t tx_id,
                                   std::shared_ptr<Marshallable> cmd,
                                   uint64_t send_time,
                                   uint32_t bound,
                                   const std::vector<int32_t>& local_keys,
                                   std::function<void(const TxnOutput&)> reply_cb) {
  auto entry = std::make_shared<LuigiLogEntry>(tx_id);
  entry->cmd_ = cmd;
  entry->send_time_ = send_time;
  entry->bound_ = bound;
  entry->proposed_ts_ = send_time + bound;
  entry->local_keys_ = local_keys;
  
  // Wrap the old-style callback
  entry->reply_cb_ = [reply_cb](int status, uint64_t commit_ts, const std::vector<std::string>& read_results) {
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

void SchedulerLuigi::RequeueForReposition(std::shared_ptr<LuigiLogEntry> entry) {
  // Validate state
  if (entry->agree_status_.load() != LUIGI_AGREE_FLUSHING) {
    Log_error("Luigi RequeueForReposition: txn %lu has wrong status %u (expected AGREE_FLUSHING)",
              entry->tid_, entry->agree_status_.load());
    return;
  }
  
  Log_info("Luigi RequeueForReposition: txn %lu going back to queue with timestamp %lu",
           entry->tid_, entry->proposed_ts_);
  
  // Put back in incoming queue - HoldReleaseTd will handle the repositioning
  incoming_txn_queue_.enqueue(entry);
}

//=============================================================================
// HoldReleaseTd: The Core of Luigi
//
// This thread runs in a loop and does two things:
// 1. Pulls txns from incoming_txn_queue_, checks conflicts, adds to priority_queue_
// 2. Releases txns from priority_queue_ when their deadline passes, sends to ready_txn_queue_
//
// Additional responsibility for agreement Case 3 (AGREE_FLUSHING):
// - Txns that need repositioning come back with AGREE_FLUSHING status
// - They go directly into priority_queue_ at their new (agreed) timestamp
// - No conflict check needed - the agreed timestamp is final
//=============================================================================

void SchedulerLuigi::HoldReleaseTd() {
  std::shared_ptr<LuigiLogEntry> entries[256];  // bulk dequeue buffer

  while (running_) {
    uint64_t now = GetMicrosecondTimestamp();

    //-------------------------------------------------------------------------
    // Phase 1: Pull from incoming_txn_queue_, do conflict check, add to priority_queue_
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
        
        Log_info("Luigi HoldReleaseTd: Repositioning txn %lu at new timestamp %lu (requeue #%u)",
                 entry->tid_, entry->proposed_ts_, entry->requeue_count_);
        
        // Insert at new position
        priority_queue_[{entry->proposed_ts_, txn_key}] = entry;
        continue;  // Skip normal conflict detection
      }

      //-----------------------------------------------------------------------
      // Normal path: NEW txn entering for the first time
      //-----------------------------------------------------------------------
      
      // CONFLICT DETECTION (from Algorithm 1, line 1-4 in paper):
      // Find the maximum lastReleasedDeadline among all keys this txn touches
      uint64_t max_last_released = 0;
      for (auto& k : entry->local_keys_) {
        auto it = last_released_deadlines_.find(k);
        if (it != last_released_deadlines_.end() && it->second > max_last_released) {
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
    }

    //-------------------------------------------------------------------------
    // Phase 2: Release txns whose deadline has passed -> ready_txn_queue_
    //-------------------------------------------------------------------------
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
      for (auto& k : entry->local_keys_) {
        if (last_released_deadlines_[k] < entry->proposed_ts_) {
          last_released_deadlines_[k] = entry->proposed_ts_;
        }
      }

      // Hand off to execution thread
      ready_txn_queue_.enqueue(entry);
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
// 2. Delegates execution to LuigiExecutor (handles DB ops, multi-shard, replication)
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
      auto& entry = entries[i];
      
      // Delegate to executor for clean separation of concerns
      executor_.Execute(entry);
      
      // Handle post-execution state based on agreement outcome
      LuigiAgreeStatus status = static_cast<LuigiAgreeStatus>(entry->agree_status_.load());
      
      switch (status) {
        case LUIGI_AGREE_COMPLETE:
          // Normal completion - nothing more to do
          // Execute() already called the callback
          break;
          
        case LUIGI_AGREE_FLUSHING:
          // Case 3: Need to reposition in priority queue
          // Execute() updated proposed_ts_ to agreed_ts_
          // Requeue for re-processing at new timestamp
          Log_info("Luigi ExecTd: txn %lu needs reposition, requeuing to incoming queue",
                   entry->tid_);
          RequeueForReposition(entry);
          break;
          
        case LUIGI_AGREE_CONFIRMING:
          // Case 2: Waiting for round 2 confirmations
          // TODO: Register this entry to receive confirmation callbacks
          // For now, we just log - actual confirmation handling needs RPC infra
          Log_info("Luigi ExecTd: txn %lu waiting for confirmations (TBD)",
                   entry->tid_);
          // The entry will be re-triggered when confirmations arrive
          // TODO: Add to a "waiting for confirmation" map
          break;
          
        default:
          // Unexpected state - log warning
          Log_warn("Luigi ExecTd: txn %lu has unexpected status %d after Execute()",
                   entry->tid_, static_cast<int>(status));
          break;
      }
    }

    if (cnt == 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
}

} // namespace janus
