#include "luigi_scheduler.h"

#include <chrono>
#include <iostream>
#include <functional>

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
    uint64_t expected_time,
    const std::vector<LuigiOp>& ops,
    std::function<void(int status, uint64_t commit_ts, const std::vector<std::string>& read_results)> reply_cb) {
  
  auto entry = std::make_shared<LuigiLogEntry>(txn_id);
  entry->proposed_ts_ = expected_time;  // Use expected_time directly as proposed timestamp
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
    uint64_t tid, uint32_t src_shard, uint64_t proposed_ts,
    uint32_t phase, std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // Core agreement logic - called for both local and remote proposals.
  //
  // When all proposals are received, this determines the outcome.
  //===========================================================================
  
  std::lock_guard<std::mutex> lock(deadline_queue_mutex_);
  
  DeadlineQItem& dqi = deadline_queue_[tid];
  
  // If entry provided (our local txn), initialize expected count
  if (entry != nullptr && dqi.entry_ == nullptr) {
    dqi.entry_ = entry;
    dqi.expected_count_ = entry->remote_shards_.size() + 1;  // remotes + ourselves
    Log_info("Luigi UpdateDeadlineRecord: tid=%lu initialized, expecting %u proposals",
             tid, dqi.expected_count_);
  }
  
  // Record this proposal (if not already received from this shard)
  if (src_shard < DeadlineQItem::MAX_SHARDS && !dqi.received_[src_shard]) {
    dqi.deadlines_[src_shard] = proposed_ts;
    dqi.phases_[src_shard] = phase;
    dqi.received_[src_shard] = true;
    dqi.item_count_++;
    
    Log_info("Luigi UpdateDeadlineRecord: tid=%lu from shard %u ts=%lu phase=%u, "
             "now have %u/%u proposals",
             tid, src_shard, proposed_ts, phase, dqi.item_count_, dqi.expected_count_);
  } else if (src_shard >= DeadlineQItem::MAX_SHARDS) {
    Log_warn("Luigi UpdateDeadlineRecord: shard_id %u exceeds MAX_SHARDS", src_shard);
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
    
    Log_info("Luigi UpdateDeadlineRecord: tid=%lu COMPLETE - agreed_ts=%lu, my_ts=%lu, all_match=%d",
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
      
      Log_info("Luigi: tid=%lu Case 2 - I'm max, waiting for %u confirmations", tid, pending);
      
      // Reset to wait for phase 2 confirmations
      dqi.item_count_ = 1;  // Only our own (we don't need to re-receive)
      dqi.expected_count_ = pending + 1;  // Need confirmations from smaller ts shards
      for (uint32_t i = 0; i < DeadlineQItem::MAX_SHARDS; i++) {
        if (i == partition_id_) continue;
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
      
      Log_info("Luigi: tid=%lu Case 3 - my_ts=%lu < agreed=%lu, need reposition",
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

uint64_t SchedulerLuigi::HandleRemoteDeadlineProposal(
    uint64_t tid, uint32_t src_shard, uint64_t remote_ts, uint32_t phase) {
  //===========================================================================
  // RPC handler for incoming deadline proposals.
  // 
  // In Tiga style, we just record the proposal and check for completion.
  // We return our proposal if we have one (for informational purposes).
  //===========================================================================
  
  Log_info("Luigi HandleRemoteDeadlineProposal: tid=%lu from shard %u ts=%lu phase=%u",
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

bool SchedulerLuigi::HandleRemoteDeadlineConfirm(
    uint64_t tid, uint32_t src_shard, uint64_t new_ts) {
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
  //===========================================================================
  // Initiate Phase 1 of leader agreement (Tiga-style).
  //
  // 1. Record our own proposal in deadline_queue_
  // 2. Broadcast our proposal to all involved shards (fire-and-forget)
  // 3. Return immediately - completion happens in UpdateDeadlineRecord
  //    when all proposals are received
  //===========================================================================
  
  uint64_t tid = entry->tid_;
  uint64_t my_ts = entry->proposed_ts_;
  
  Log_info("Luigi InitiateAgreement: tid=%lu, my_ts=%lu, remote_shards=%zu",
           tid, my_ts, entry->remote_shards_.size());
  
  // Record our own proposal first (this initializes the DeadlineQItem)
  UpdateDeadlineRecord(tid, partition_id_, my_ts, 1, entry);
  
  // Broadcast to all remote shards (fire-and-forget)
  for (uint32_t remote_shard : entry->remote_shards_) {
    LuigiLeaderProxy* proxy = GetLeaderProxy(remote_shard);
    if (proxy == nullptr) {
      Log_warn("Luigi InitiateAgreement: No proxy for shard %u", remote_shard);
      // Without proxy, we can't reach this shard - treat as if they agree with us
      // (In production, this would be a configuration error)
      UpdateDeadlineRecord(tid, remote_shard, my_ts, 1, nullptr);
      continue;
    }
    
    // Build request
    LuigiDeadlineRequest req;
    req.tid = tid;
    req.proposed_ts = my_ts;
    req.src_shard = partition_id_;
    req.phase = 1;
    
    // Fire-and-forget async RPC (like Tiga's Future::safe_release)
    auto fut = proxy->async_DeadlinePropose(req);
    // Don't wait for result - completion happens when we receive RPCs back
    // The rrr framework will clean up the future
    
    Log_info("Luigi InitiateAgreement: sent proposal to shard %u", remote_shard);
  }
}

void SchedulerLuigi::SendRepositionConfirmations(std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // Phase 2: We were in Case 3 (had smaller timestamp), have repositioned,
  // and now notify others that we've updated.
  //
  // This is sent to all involved shards so they can complete their Case 2 wait.
  //===========================================================================
  
  uint64_t tid = entry->tid_;
  uint64_t new_ts = entry->proposed_ts_;  // Should now be agreed_ts_
  
  Log_info("Luigi SendRepositionConfirmations: tid=%lu, new_ts=%lu", tid, new_ts);
  
  for (uint32_t remote_shard : entry->remote_shards_) {
    LuigiLeaderProxy* proxy = GetLeaderProxy(remote_shard);
    if (proxy == nullptr) {
      Log_warn("Luigi SendRepositionConfirmations: No proxy for shard %u", remote_shard);
      continue;
    }
    
    LuigiDeadlineRequest req;
    req.tid = tid;
    req.proposed_ts = new_ts;
    req.src_shard = partition_id_;
    req.phase = 2;  // Phase 2 = confirmation
    
    // Fire-and-forget async RPC
    auto fut = proxy->async_DeadlineConfirm(req);
    
    Log_info("Luigi SendRepositionConfirmations: sent phase-2 to shard %u", remote_shard);
  }
  
  // Clean up our deadline queue entry
  {
    std::lock_guard<std::mutex> lock(deadline_queue_mutex_);
    deadline_queue_.erase(tid);
  }
}

} // namespace janus
