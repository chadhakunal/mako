#include "luigi_executor.h"
#include "luigi_scheduler.h"  // For RPC coordination

#include <chrono>
#include <algorithm>
#include <set>

#include "deptran/__dep__.h"  // For logging macros

namespace janus {

//=============================================================================
// Construction / Destruction
//=============================================================================

LuigiExecutor::LuigiExecutor() {}

LuigiExecutor::~LuigiExecutor() {}

//=============================================================================
// Main Execution Entry Point
//=============================================================================

void LuigiExecutor::Execute(std::shared_ptr<LuigiLogEntry> entry) {
  int status = 0;  // SUCCESS
  uint64_t commit_ts = entry->proposed_ts_;
  
  //-------------------------------------------------------------------------
  // Step 1: Multi-shard detection
  //-------------------------------------------------------------------------
  bool is_multi_shard = IsMultiShard(entry);
  
  //-------------------------------------------------------------------------
  // Step 2: For multi-shard txns, handle agreement state machine
  //
  // Tiga-style async agreement flow:
  // - INIT: Broadcast proposal, return (wait for async completion)
  // - FLUSHING: Reposition and retry (Case 3)
  // - CONFIRMING: Wait for phase-2 confirmations (Case 2)
  // - COMPLETE: Execute!
  //-------------------------------------------------------------------------
  if (is_multi_shard) {
    LuigiAgreeStatus agree_status = 
        static_cast<LuigiAgreeStatus>(entry->agree_status_.load());
    
    switch (agree_status) {
      case LUIGI_AGREE_INIT:
        //-------------------------------------------------------------------
        // First time seeing this txn - initiate agreement
        // This broadcasts our proposal and returns immediately.
        // When all proposals are received, UpdateDeadlineRecord() will
        // determine the case and re-enqueue the txn to ready_queue_.
        //-------------------------------------------------------------------
        Log_info("Luigi Execute: txn %lu initiating agreement (async)", entry->tid_);
        if (scheduler_ != nullptr) {
          scheduler_->InitiateAgreement(entry);
        }
        // Don't execute yet - wait for agreement to complete asynchronously
        entry->exec_status_.store(LUIGI_EXEC_INIT);
        return;
        
      case LUIGI_AGREE_FLUSHING:
        //-------------------------------------------------------------------
        // Case 3: We had smaller timestamp, need to reposition
        // Update our proposed_ts and send confirmations
        //-------------------------------------------------------------------
        Log_info("Luigi Execute: txn %lu repositioning (proposed=%lu -> agreed=%lu)",
                 entry->tid_, entry->proposed_ts_, entry->agreed_ts_);
        
        // Send Phase 2 confirmations to other leaders
        if (scheduler_ != nullptr) {
          entry->proposed_ts_ = entry->agreed_ts_;  // Update our timestamp
          scheduler_->SendRepositionConfirmations(entry);
        }
        
        // Requeue to priority queue at new timestamp
        entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
        entry->requeue_count_++;
        // Caller (ExecTd in scheduler) will handle requeuing
        return;
        
      case LUIGI_AGREE_CONFIRMING:
        //-------------------------------------------------------------------
        // Case 2: We're max, waiting for others to confirm repositioning
        // If we get here, confirmations haven't arrived yet - return and wait
        //-------------------------------------------------------------------
        Log_info("Luigi Execute: txn %lu still waiting for confirmations", entry->tid_);
        // The scheduler's HandleRemoteDeadlineConfirm will re-enqueue us
        // when all confirmations arrive
        return;
        
      case LUIGI_AGREE_COMPLETE:
        //-------------------------------------------------------------------
        // Agreement complete! Proceed to execution
        //-------------------------------------------------------------------
        Log_info("Luigi Execute: txn %lu agreement complete at ts=%lu",
                 entry->tid_, entry->agreed_ts_);
        commit_ts = entry->agreed_ts_;
        entry->ts_agreed_.store(true);
        entry->exec_status_.store(LUIGI_EXEC_DIRECT);
        break;
        
      default:
        Log_error("Luigi Execute: txn %lu unexpected agree_status %d",
                  entry->tid_, static_cast<int>(agree_status));
        status = -1;
        goto done;
    }
  } else {
    // Single-shard: no agreement needed, execute directly
    entry->agreed_ts_ = entry->proposed_ts_;
    entry->ts_agreed_.store(true);
    entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
    entry->exec_status_.store(LUIGI_EXEC_DIRECT);
    commit_ts = entry->proposed_ts_;
  }
  
  //-------------------------------------------------------------------------
  // Step 3: Execute all operations
  //-------------------------------------------------------------------------
  status = ExecuteAllOps(entry);
  if (status != 0) {
    Log_error("Luigi Execute: Operation execution failed for txn %lu", entry->tid_);
    goto done;
  }
  
  //-------------------------------------------------------------------------
  // Step 4: Trigger replication (background Paxos)
  //-------------------------------------------------------------------------
  // Note: We trigger replication even if not all reads succeeded
  // This matches Mako's behavior - replication happens for committed txns
  status = TriggerReplication(entry);
  if (status != 0) {
    Log_error("Luigi Execute: Replication trigger failed for txn %lu", entry->tid_);
    // Don't abort - replication failure is handled by Paxos recovery
    status = 0;  // Reset status, txn still committed locally
  }

done:
  entry->exec_status_.store(LUIGI_EXEC_COMPLETE);
  
  // Call reply callback
  if (entry->reply_cb_) {
    entry->reply_cb_(status, commit_ts, entry->read_results_);
  }
}

//=============================================================================
// Multi-shard Detection & Agreement
//=============================================================================

bool LuigiExecutor::IsMultiShard(const std::shared_ptr<LuigiLogEntry>& entry) {
  // Check if the transaction has remote_shards set
  // This would be populated by the coordinator when it knows the txn
  // touches multiple shards
  
  // For now, we rely on the entry having this information from the coordinator
  // The coordinator knows which partitions a txn touches based on the keys
  
  // Simple check: if remote_shards_ is non-empty, it's multi-shard
  return !entry->remote_shards_.empty();
}

//=============================================================================
// NOTE: Agreement is now handled asynchronously (Tiga-style)
//
// The old PerformLeaderAgreement() and PerformAgreementPhase2() methods
// have been replaced by the scheduler's async agreement mechanism:
//
// 1. InitiateAgreement() broadcasts our proposal (fire-and-forget)
// 2. UpdateDeadlineRecord() collects proposals and determines case
// 3. When complete, txn is re-enqueued to ready_queue_ with appropriate status
// 4. Execute() handles each status accordingly
//
// This avoids blocking the exec thread waiting for RPC responses.
//=============================================================================

//=============================================================================
// Read Operations (delegate to callback)
//=============================================================================

int LuigiExecutor::ExecuteRead(const LuigiOp& op, std::string& value_out) {
  value_out.clear();
  
  // Validate callback is set
  if (!read_cb_) {
    Log_error("Luigi ExecuteRead: read callback not set!");
    return -1;
  }
  
  // Execute via callback (provided by Mako)
  try {
    bool found = read_cb_(op.table_id, op.key, value_out);
    if (!found) {
      // Key not found - this might be expected (e.g., first insert)
      Log_debug("Luigi ExecuteRead: key not found in table %d", op.table_id);
      value_out.clear();
      // Return success - not finding a key is not an error
    }
    return 0;  // SUCCESS
  } catch (const std::exception& ex) {
    Log_error("Luigi ExecuteRead: exception in table %d: %s", 
              op.table_id, ex.what());
    return -1;
  } catch (...) {
    Log_error("Luigi ExecuteRead: unknown exception in table %d", 
              op.table_id);
    return -1;
  }
}

//=============================================================================
// Write Operations (delegate to callback)
//=============================================================================

int LuigiExecutor::ExecuteWrite(const LuigiOp& op) {
  // Validate callback is set
  if (!write_cb_) {
    Log_error("Luigi ExecuteWrite: write callback not set!");
    return -1;
  }
  
  // Execute via callback (provided by Mako)
  try {
    bool success = write_cb_(op.table_id, op.key, op.value);
    if (!success) {
      Log_error("Luigi ExecuteWrite: write failed for table %d", op.table_id);
      return -1;
    }
    return 0;  // SUCCESS
  } catch (const std::exception& ex) {
    Log_error("Luigi ExecuteWrite: exception in table %d: %s", 
              op.table_id, ex.what());
    return -1;
  } catch (...) {
    Log_error("Luigi ExecuteWrite: unknown exception in table %d", 
              op.table_id);
    return -1;
  }
}

//=============================================================================
// Execute All Operations (LOCAL KEYS ONLY)
//
// IMPORTANT: In a multi-shard transaction, each shard only executes
// operations for keys that belong to it. The coordinator sends the full
// transaction to all involved shards, but each shard filters by local_keys_.
//
// Example: Transaction touches keys A (shard 1) and B (shard 2)
//   - Shard 1: local_keys_ = {A}, only executes ops where key == A
//   - Shard 2: local_keys_ = {B}, only executes ops where key == B
//=============================================================================

int LuigiExecutor::ExecuteAllOps(std::shared_ptr<LuigiLogEntry> entry) {
  entry->read_results_.clear();
  
  // Build a set of local keys for O(1) lookup
  // local_keys_ contains the keys that THIS shard owns
  std::set<std::string> local_key_set;
  for (int32_t k : entry->local_keys_) {
    // Convert int32_t key to string for comparison with op.key
    local_key_set.insert(std::to_string(k));
  }
  
  // If local_keys_ is empty but we have shard_to_keys_, use that
  if (local_key_set.empty() && !entry->shard_to_keys_.empty()) {
    // Find our shard's keys from shard_to_keys_
    auto it = entry->shard_to_keys_.find(partition_id_);
    if (it != entry->shard_to_keys_.end()) {
      for (int32_t k : it->second) {
        local_key_set.insert(std::to_string(k));
      }
    }
  }
  
  // Determine if we should filter by local keys
  // If local_key_set is empty, assume single-shard txn and execute all ops
  bool should_filter = !local_key_set.empty();
  
  Log_debug("Luigi ExecuteAllOps: txn %lu has %zu ops, %zu local keys, filter=%d",
            entry->tid_, entry->ops_.size(), local_key_set.size(), should_filter);
  
  //-------------------------------------------------------------------------
  // Phase 1: Execute READ operations (only for local keys)
  //-------------------------------------------------------------------------
  for (auto& op : entry->ops_) {
    if (op.op_type == LUIGI_OP_READ) {
      // Check if this key belongs to us
      bool is_local = !should_filter || (local_key_set.count(op.key) > 0);
      
      if (is_local) {
        std::string value;
        int ret = ExecuteRead(op, value);
        if (ret != 0) {
          Log_error("Luigi ExecuteAllOps: Read failed for txn %lu, key=%s", 
                    entry->tid_, op.key.c_str());
          return -1;
        }
        entry->read_results_.push_back(value);
        op.executed = true;
      } else {
        Log_debug("Luigi ExecuteAllOps: Skipping remote read key=%s for txn %lu",
                  op.key.c_str(), entry->tid_);
      }
    }
  }
  
  //-------------------------------------------------------------------------
  // Phase 2: Execute WRITE operations (only for local keys)
  //-------------------------------------------------------------------------
  for (auto& op : entry->ops_) {
    if (op.op_type == LUIGI_OP_WRITE) {
      // Check if this key belongs to us
      bool is_local = !should_filter || (local_key_set.count(op.key) > 0);
      
      if (is_local) {
        int ret = ExecuteWrite(op);
        if (ret != 0) {
          Log_error("Luigi ExecuteAllOps: Write failed for txn %lu, key=%s",
                    entry->tid_, op.key.c_str());
          return -1;
        }
        op.executed = true;
      } else {
        Log_debug("Luigi ExecuteAllOps: Skipping remote write key=%s for txn %lu",
                  op.key.c_str(), entry->tid_);
      }
    }
  }
  
  return 0;  // SUCCESS
}

//=============================================================================
// Replication (delegate to callback)
//=============================================================================

int LuigiExecutor::TriggerReplication(std::shared_ptr<LuigiLogEntry> entry) {
  // Count writes for logging
  size_t num_writes = 0;
  for (const auto& op : entry->ops_) {
    if (op.op_type == LUIGI_OP_WRITE) {
      num_writes++;
    }
  }
  
  if (num_writes == 0) {
    // Read-only txn, no replication needed
    return 0;
  }
  
  // Use callback if available
  if (replication_cb_) {
    try {
      bool success = replication_cb_(entry);
      if (!success) {
        Log_warn("Luigi TriggerReplication: replication callback returned false for txn %lu",
                 entry->tid_);
        // Don't fail - replication errors are handled by Paxos recovery
      }
    } catch (const std::exception& ex) {
      Log_warn("Luigi TriggerReplication: exception for txn %lu: %s",
               entry->tid_, ex.what());
    }
  } else {
    Log_debug("Luigi TriggerReplication: txn %lu has %zu writes (callback not set)",
              entry->tid_, num_writes);
  }
  
  return 0;
}

//=============================================================================
// Note on Rollback (Option D - Agreement Before Execution)
//
// With Option D, we do NOT execute speculatively for multi-shard txns.
// Instead, we:
//   1. Wait for leader agreement to complete
//   2. Only then execute the transaction
//
// This means we never need to rollback executed operations.
// The "reposition" case (Case 3) simply moves the txn in the priority queue
// without having executed anything yet.
//
// Trade-off: We sacrifice some potential latency hiding (speculative exec)
// in exchange for:
//   - No rollback complexity
//   - Simpler correctness reasoning
//   - No wasted work on failed speculation
//
// If we later want speculative execution (Option A/B/C from the design),
// we would add a RollbackSpeculativeExecution() function here.
//=============================================================================

} // namespace janus
