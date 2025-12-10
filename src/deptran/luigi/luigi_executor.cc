#include "luigi_executor.h"

#include <chrono>

// Mako includes
#include "benchmarks/abstract_ordered_index.h"  // For shard_get/shard_put
#include "deptran/s_main.h"  // For add_log_to_nc (replication)
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
  entry->exec_status_.store(LUIGI_EXEC_DIRECT);
  
  int status = 0;  // SUCCESS
  uint64_t commit_ts = entry->proposed_ts_;
  
  //-------------------------------------------------------------------------
  // Step 1: Multi-shard detection
  //-------------------------------------------------------------------------
  bool is_multi_shard = IsMultiShard(entry);
  
  //-------------------------------------------------------------------------
  // Step 2: Leader agreement (if multi-shard)
  //
  // For multi-shard txns, we need the 3-case agreement protocol:
  // Case 1: All timestamps matched -> release immediately
  // Case 2: This leader used agreed_ts, others didn't -> WAIT
  // Case 3: This leader used smaller ts -> ROLLBACK + reposition
  //-------------------------------------------------------------------------
  if (is_multi_shard) {
    AgreementResult result = PerformLeaderAgreement(entry);
    
    switch (result) {
      case AgreementResult::SUCCESS:
        // Case 1: All matched, or Case 2 confirmed - proceed with execution
        commit_ts = entry->agreed_ts_;
        break;
        
      case AgreementResult::WAIT_ROUND2:
        // Case 2: We used agreed_ts but others haven't - don't release yet
        // The scheduler will handle re-queuing and waiting
        Log_info("Luigi Execute: txn %lu waiting for round 2 confirmation", entry->tid_);
        entry->agree_status_.store(LUIGI_AGREE_CONFIRMING);
        entry->exec_status_.store(LUIGI_EXEC_INIT);  // Reset for re-processing
        // Don't execute yet - return without callback
        return;
        
      case AgreementResult::NEEDS_ROLLBACK:
        // Case 3: We used smaller ts - need to rollback and reposition
        Log_info("Luigi Execute: txn %lu needs rollback (agreed_ts=%lu > proposed_ts=%lu)",
                 entry->tid_, entry->agreed_ts_, entry->proposed_ts_);
        entry->agree_status_.store(LUIGI_AGREE_FLUSHING);
        entry->exec_status_.store(LUIGI_EXEC_ROLLBACK);
        
        // If we already executed speculatively, rollback
        if (entry->exec_status_.load() == LUIGI_EXEC_SPEC) {
          status = RollbackSpeculativeExecution(entry);
          if (status != 0) {
            Log_error("Luigi Execute: Rollback failed for txn %lu", entry->tid_);
            goto done;
          }
        }
        
        // Update proposed timestamp to agreed timestamp and requeue
        entry->proposed_ts_ = entry->agreed_ts_;
        entry->exec_status_.store(LUIGI_EXEC_REPOSITIONED);  // Mark as repositioned
        entry->requeue_count_++;
        // Scheduler will reposition in priority queue based on new timestamp
        // Don't execute yet - return without callback
        return;
        
      case AgreementResult::FAILED:
        Log_error("Luigi Execute: Leader agreement failed for txn %lu", entry->tid_);
        status = -1;
        goto done;
    }
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

LuigiExecutor::AgreementResult LuigiExecutor::PerformLeaderAgreement(
    std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // PLACEHOLDER: Leader Agreement State Machine
  //
  // This implements the 3-case agreement protocol from the Tiga paper:
  //
  // ROUND 1: Each leader proposes proposed_ts_ (based on hold-and-release)
  // 
  // After receiving all proposals, compute agreed_ts_ = max(all proposals)
  //
  // ROUND 1 OUTCOMES (per-leader):
  //   Case 1: ALL timestamps matched (my_proposed == agreed)
  //           -> Release immediately, 0.5 WRTT latency
  //   
  //   Case 2: This leader already used agreed_ts (my_proposed == agreed)
  //           but OTHER leaders proposed smaller timestamps
  //           -> WAIT for round 2 confirmation from those leaders
  //           -> Cannot release until they confirm they've repositioned
  //           -> Prevents timestamp inversion!
  //   
  //   Case 3: This leader used SMALLER ts (my_proposed < agreed)
  //           -> ROLLBACK speculative execution
  //           -> Update proposed_ts_ = agreed_ts_
  //           -> Reposition in holdBuffer at new timestamp
  //           -> Re-execute when reaching head again
  //
  // ROUND 2: Leaders who did Case 3 send confirmation
  //          Leaders in Case 2 can then release
  //
  //===========================================================================
  
  entry->prev_agree_status_.store(entry->agree_status_.load());
  
  if (entry->remote_shards_.empty()) {
    // Single-shard: no agreement needed, use proposed timestamp
    entry->agreed_ts_ = entry->proposed_ts_;
    entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
    entry->ts_agreed_.store(true);
    return AgreementResult::SUCCESS;
  }
  
  // Multi-shard case
  Log_info("Luigi Agreement: Multi-shard txn %lu, proposed_ts=%lu, %zu remote shards",
           entry->tid_, entry->proposed_ts_, entry->remote_shards_.size());
  
  //===========================================================================
  // TODO: Implement actual leader agreement RPC
  //
  // ROUND 1 Implementation:
  //   for each remote_shard in remote_shards_:
  //       response = RPC_ProposeTimestamp(remote_shard, tid_, proposed_ts_)
  //       remote_proposals[remote_shard] = response.proposed_ts
  //   
  //   agreed_ts_ = max(proposed_ts_, max(remote_proposals))
  //   
  //   // Determine which case we're in
  //   if (all proposals == agreed_ts_):
  //       return AgreementResult::SUCCESS  // Case 1
  //   elif (proposed_ts_ == agreed_ts_):
  //       return AgreementResult::WAIT_ROUND2  // Case 2
  //   else:
  //       return AgreementResult::NEEDS_ROLLBACK  // Case 3
  //
  // ROUND 2 Implementation (for Case 3 leaders):
  //   RPC_ConfirmReposition(remote_shards, tid_, agreed_ts_)
  //
  // For leaders in Case 2 waiting:
  //   Wait until ReceiveRepositionConfirm(tid_) from all Case 3 leaders
  //   Then return AgreementResult::SUCCESS
  //
  //===========================================================================
  
  // PLACEHOLDER: For now, simulate Case 1 (all matched) for simplicity
  // In real implementation, this would involve RPC exchanges
  
  entry->agreed_ts_ = entry->proposed_ts_;
  entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
  entry->ts_agreed_.store(true);
  
  Log_info("Luigi Agreement: txn %lu agreed_ts=%lu (PLACEHOLDER - actual agreement TBD)",
           entry->tid_, entry->agreed_ts_);
  
  return AgreementResult::SUCCESS;
}

//=============================================================================
// Read Operations
//=============================================================================

int LuigiExecutor::ExecuteRead(const LuigiOp& op, std::string& value_out) {
  value_out.clear();
  
  // Validate DB tables are set
  if (db_tables_ == nullptr) {
    Log_error("Luigi ExecuteRead: db_tables_ not set!");
    return -1;
  }
  
  // Find the table
  auto it = db_tables_->find(op.table_id);
  if (it == db_tables_->end() || it->second == nullptr) {
    Log_error("Luigi ExecuteRead: table_id %d not found", op.table_id);
    return -1;
  }
  
  abstract_ordered_index* table = it->second;
  
  // Execute the read
  try {
    bool found = table->shard_get(lcdf::Str(op.key), value_out);
    if (!found) {
      // Key not found - this might be expected (e.g., first insert)
      Log_debug("Luigi ExecuteRead: key not found in table %d", op.table_id);
      value_out.clear();
      // Return success - not finding a key is not an error
    }
    return 0;  // SUCCESS
  } catch (const std::exception& ex) {
    Log_error("Luigi ExecuteRead: shard_get exception in table %d: %s", 
              op.table_id, ex.what());
    return -1;
  } catch (...) {
    Log_error("Luigi ExecuteRead: shard_get unknown exception in table %d", 
              op.table_id);
    return -1;
  }
}

//=============================================================================
// Write Operations
//=============================================================================

int LuigiExecutor::ExecuteWrite(const LuigiOp& op) {
  // Validate DB tables are set
  if (db_tables_ == nullptr) {
    Log_error("Luigi ExecuteWrite: db_tables_ not set!");
    return -1;
  }
  
  // Find the table
  auto it = db_tables_->find(op.table_id);
  if (it == db_tables_->end() || it->second == nullptr) {
    Log_error("Luigi ExecuteWrite: table_id %d not found", op.table_id);
    return -1;
  }
  
  abstract_ordered_index* table = it->second;
  
  // Execute the write
  try {
    table->shard_put(lcdf::Str(op.key), op.value);
    return 0;  // SUCCESS
  } catch (const std::exception& ex) {
    Log_error("Luigi ExecuteWrite: shard_put exception in table %d: %s", 
              op.table_id, ex.what());
    return -1;
  } catch (...) {
    Log_error("Luigi ExecuteWrite: shard_put unknown exception in table %d", 
              op.table_id);
    return -1;
  }
}

//=============================================================================
// Execute All Operations
//=============================================================================

int LuigiExecutor::ExecuteAllOps(std::shared_ptr<LuigiLogEntry> entry) {
  entry->read_results_.clear();
  
  // Separate reads and writes for cleaner execution order
  // (In some isolation levels, reads should happen before writes)
  
  //-------------------------------------------------------------------------
  // Phase 1: Execute all READ operations
  //-------------------------------------------------------------------------
  for (auto& op : entry->ops_) {
    if (op.op_type == LUIGI_OP_READ) {
      std::string value;
      int ret = ExecuteRead(op, value);
      if (ret != 0) {
        Log_error("Luigi ExecuteAllOps: Read failed for txn %lu", entry->tid_);
        return -1;
      }
      entry->read_results_.push_back(value);
      op.executed = true;
    }
  }
  
  //-------------------------------------------------------------------------
  // Phase 2: Execute all WRITE operations
  //-------------------------------------------------------------------------
  for (auto& op : entry->ops_) {
    if (op.op_type == LUIGI_OP_WRITE) {
      int ret = ExecuteWrite(op);
      if (ret != 0) {
        Log_error("Luigi ExecuteAllOps: Write failed for txn %lu", entry->tid_);
        return -1;
      }
      op.executed = true;
    }
  }
  
  return 0;  // SUCCESS
}

//=============================================================================
// Replication
//=============================================================================

int LuigiExecutor::TriggerReplication(std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // TODO: Implement log serialization and replication
  //
  // The implementation should:
  // 1. Serialize entry->ops_ into a binary log buffer
  // 2. Include: txn_id, commit_timestamp, list of (table_id, key, value) for writes
  // 3. Call add_log_to_nc(log_buffer, log_len, partition_id_)
  //
  // Log format (suggested):
  //   [8 bytes: txn_id]
  //   [8 bytes: commit_ts]
  //   [4 bytes: num_writes]
  //   For each write:
  //     [4 bytes: table_id]
  //     [4 bytes: key_len]
  //     [key_len bytes: key]
  //     [4 bytes: value_len]
  //     [value_len bytes: value]
  //
  // For now, we skip replication (placeholder)
  //===========================================================================
  
  // Count writes for logging
  size_t num_writes = 0;
  for (const auto& op : entry->ops_) {
    if (op.op_type == LUIGI_OP_WRITE) {
      num_writes++;
    }
  }
  
  if (num_writes > 0) {
    Log_debug("Luigi TriggerReplication: txn %lu has %zu writes (replication not yet implemented)",
              entry->tid_, num_writes);
  }
  
  // Placeholder: return success
  // When implemented, failures here would be logged but not abort the txn
  // (Paxos handles durability guarantees)
  return 0;
}

//=============================================================================
// Rollback Support for Case 3 Agreement
//=============================================================================

int LuigiExecutor::RollbackSpeculativeExecution(std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // Rollback Speculative Writes
  //
  // When a multi-shard txn goes through agreement and we're in Case 3
  // (our proposed_ts_ was smaller than agreed_ts_), we need to:
  //
  // 1. Undo any writes we did speculatively
  // 2. Clear the executed state
  // 3. The txn will be repositioned in the queue and re-executed later
  //
  // NOTE: In Tiga, speculative execution writes to speculativeVersion_[key].
  //       Rollback sets speculativeVersion_[key] = {UINT64_MAX, UINT32_MAX}
  //       to invalidate the speculative write.
  //
  // In Luigi/Mako with direct shard_put:
  // - We can either restore old values (if tracked) OR
  // - Use Mako's STO abort mechanism OR  
  // - NOT do speculative execution until agreement is done (simplest)
  //
  // IMPORTANT: Reads don't need rollback - they're idempotent.
  //            Only writes (state changes) need to be undone.
  //===========================================================================
  
  Log_info("Luigi Rollback: txn %lu, requeue_count=%u",
           entry->tid_, entry->requeue_count_);
  
  // If we haven't actually executed writes yet (waiting for agreement),
  // there's nothing to rollback
  if (entry->exec_status_.load() != LUIGI_EXEC_SPEC) {
    Log_debug("Luigi Rollback: txn %lu not in EXEC_SPEC state, nothing to rollback",
              entry->tid_);
    return 0;
  }
  
  //===========================================================================
  // TODO: Implement actual rollback
  //
  // Option A: Track old values during speculative write, restore them here
  //   for each (table_id, key, old_value) in speculative_writes:
  //       if old_value.empty():
  //           table->shard_delete(key)
  //       else:
  //           table->shard_put(key, old_value)
  //
  // Option B: Use Tiga-style speculative versions
  //   speculativeVersion_[key] = {UINT64_MAX, UINT32_MAX}  // Invalidate
  //
  // Option C: Use Mako's abort mechanism (if integrated with STO)
  //   Sto::silent_abort()
  //
  // Option D: Don't execute speculatively for multi-shard txns
  //   Wait for agreement first, then execute (simplest, no rollback needed)
  //
  // For now: placeholder, clear execution state
  //===========================================================================
  
  // Clear execution state
  entry->read_results_.clear();
  
  // Reset op execution flags
  for (auto& op : entry->ops_) {
    op.executed = false;
  }
  
  // Update execution status
  entry->exec_status_.store(LUIGI_EXEC_ROLLBACK);
  
  Log_info("Luigi Rollback: txn %lu rollback complete (placeholder - actual undo TBD)",
           entry->tid_);
  
  return 0;
}

} // namespace janus
