#include "luigi_executor.h"

#include <chrono>
#include <algorithm>

// Mako includes
#include "benchmarks/abstract_ordered_index.h"  // For shard_get/shard_put
#include "deptran/__dep__.h"  // For logging macros
// #include "deptran/s_main.h"  // For add_log_to_nc (replication) - TODO

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
  // Step 2: For single-shard txns, execute directly
  //         For multi-shard txns, do agreement FIRST (Option D - no speculative exec)
  //
  // Agreement Protocol (3-case):
  // - Round 1: All leaders propose their hold-release timestamps
  // - agreed_ts = max(all proposals)
  // - Case 1: All timestamps matched -> release immediately
  // - Case 2: This leader used agreed_ts, others proposed smaller -> WAIT for them
  // - Case 3: This leader used smaller ts -> reposition to agreed_ts and retry
  //
  // Key: We do NOT execute until agreement is complete. This avoids rollbacks.
  //-------------------------------------------------------------------------
  if (is_multi_shard) {
    AgreementResult result = PerformLeaderAgreement(entry);
    
    switch (result) {
      case AgreementResult::AGREEMENT_SUCCESS:
        // Case 1: All timestamps matched, or Case 2 received all confirmations
        // Safe to proceed with execution
        commit_ts = entry->agreed_ts_;
        entry->exec_status_.store(LUIGI_EXEC_DIRECT);
        break;
        
      case AgreementResult::WAIT_ROUND2:
        // Case 2: We used agreed_ts but others proposed smaller timestamps
        // Must wait for them to confirm they've repositioned before we can execute
        // This prevents timestamp inversion (we can't execute before they reposition)
        Log_info("Luigi Execute: txn %lu in CONFIRMING state - waiting for round 2", entry->tid_);
        entry->agree_status_.store(LUIGI_AGREE_CONFIRMING);
        entry->exec_status_.store(LUIGI_EXEC_INIT);  // Not started yet
        // Return to scheduler - will be re-triggered when confirmations arrive
        return;
        
      case AgreementResult::NEEDS_REPOSITION:
        // Case 3: We proposed smaller ts than agreed_ts
        // Must reposition in the priority queue and retry when we reach head again
        // No rollback needed since we haven't executed yet (Option D)
        Log_info("Luigi Execute: txn %lu needs reposition (proposed=%lu < agreed=%lu)",
                 entry->tid_, entry->proposed_ts_, entry->agreed_ts_);
        entry->agree_status_.store(LUIGI_AGREE_FLUSHING);
        entry->exec_status_.store(LUIGI_EXEC_INIT);  // Not started
        
        // Update proposed timestamp to agreed timestamp
        entry->proposed_ts_ = entry->agreed_ts_;
        entry->requeue_count_++;
        // Scheduler will reposition in priority queue based on new timestamp
        // No callback - scheduler will retry when this txn reaches head again
        return;
        
      case AgreementResult::AGREEMENT_FAILED:
        Log_error("Luigi Execute: Leader agreement failed for txn %lu", entry->tid_);
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

LuigiExecutor::AgreementResult LuigiExecutor::PerformLeaderAgreement(
    std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // Leader Agreement State Machine (Tiga-style, Agreement-Before-Execution)
  //
  // Phase 1: Exchange timestamp proposals
  // Phase 2: Confirm repositions (if needed)
  //
  // 3-Case outcomes after Phase 1:
  //   Case 1: All proposals == agreed_ts -> SUCCESS (0.5 WRTT)
  //   Case 2: My proposal == agreed_ts, others smaller -> WAIT_ROUND2
  //   Case 3: My proposal < agreed_ts -> NEEDS_REPOSITION
  //===========================================================================
  
  entry->prev_agree_status_.store(entry->agree_status_.load());
  
  //-------------------------------------------------------------------------
  // Single-shard fast path
  //-------------------------------------------------------------------------
  if (entry->remote_shards_.empty()) {
    entry->agreed_ts_ = entry->proposed_ts_;
    entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
    entry->ts_agreed_.store(true);
    return AgreementResult::AGREEMENT_SUCCESS;
  }
  
  //-------------------------------------------------------------------------
  // Check if we're in a re-entry (coming back from FLUSHING)
  //-------------------------------------------------------------------------
  if (entry->agree_status_.load() == LUIGI_AGREE_FLUSHING) {
    // We were repositioned and are now re-entering with updated proposed_ts_
    // We need to do Phase 2: send confirmation to other leaders
    Log_info("Luigi Agreement: txn %lu re-entering after reposition (Phase 2)",
             entry->tid_);
    return PerformAgreementPhase2(entry);
  }
  
  //-------------------------------------------------------------------------
  // Phase 1: Exchange timestamp proposals (AGREE_INIT -> determine case)
  //-------------------------------------------------------------------------
  Log_info("Luigi Agreement: txn %lu Phase 1, proposed_ts=%lu, %zu remote shards",
           entry->tid_, entry->proposed_ts_, entry->remote_shards_.size());
  
  // Collect proposals from all involved shards (including ourselves)
  std::map<uint32_t, uint64_t> shard_proposals;
  shard_proposals[partition_id_] = entry->proposed_ts_;  // Our proposal
  
  //===========================================================================
  // TODO: RPC to collect proposals from remote shards
  //
  // for (uint32_t remote_shard : entry->remote_shards_) {
  //     // Build request: tid_, my proposed_ts, keys involved
  //     LuigiDeadlineRequest req;
  //     req.tid = entry->tid_;
  //     req.proposed_ts = entry->proposed_ts_;
  //     req.my_shard_id = partition_id_;
  //     req.keys = entry->shard_to_keys_[remote_shard];
  //
  //     // Send RPC and get response
  //     LuigiDeadlineResponse resp = RPC_ProposeDeadline(remote_shard, req);
  //     shard_proposals[remote_shard] = resp.proposed_ts;
  // }
  //
  // PLACEHOLDER: For now, simulate that remote shards propose the same timestamp
  //===========================================================================
  for (uint32_t remote_shard : entry->remote_shards_) {
    // PLACEHOLDER: Assume all remote shards agree with our timestamp
    // In real implementation, this comes from RPC response
    shard_proposals[remote_shard] = entry->proposed_ts_;
  }
  
  //-------------------------------------------------------------------------
  // Compute agreed timestamp = max(all proposals)
  //-------------------------------------------------------------------------
  uint64_t agreed_ts = entry->proposed_ts_;
  for (const auto& [shard_id, ts] : shard_proposals) {
    if (ts > agreed_ts) {
      agreed_ts = ts;
    }
  }
  entry->agreed_ts_ = agreed_ts;
  
  Log_debug("Luigi Agreement: txn %lu agreed_ts=%lu (from %zu shards)",
            entry->tid_, agreed_ts, shard_proposals.size());
  
  //-------------------------------------------------------------------------
  // Determine which case we're in
  //-------------------------------------------------------------------------
  bool all_matched = true;
  for (const auto& [shard_id, ts] : shard_proposals) {
    if (ts != agreed_ts) {
      all_matched = false;
      break;
    }
  }
  
  if (all_matched) {
    //-----------------------------------------------------------------------
    // Case 1: All proposals matched -> release immediately
    //-----------------------------------------------------------------------
    Log_info("Luigi Agreement: txn %lu CASE 1 - all matched at ts=%lu",
             entry->tid_, agreed_ts);
    entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
    entry->ts_agreed_.store(true);
    return AgreementResult::AGREEMENT_SUCCESS;
  }
  
  if (entry->proposed_ts_ == agreed_ts) {
    //-----------------------------------------------------------------------
    // Case 2: We have the max timestamp, but others proposed smaller
    // Must wait for them to confirm they've repositioned (Phase 2)
    //
    // CRITICAL: We cannot execute yet! If we execute before they reposition,
    // they might execute at their smaller timestamp, violating ordering.
    //-----------------------------------------------------------------------
    Log_info("Luigi Agreement: txn %lu CASE 2 - waiting for round 2 (our ts=%lu, agreed=%lu)",
             entry->tid_, entry->proposed_ts_, agreed_ts);
    entry->agree_status_.store(LUIGI_AGREE_CONFIRMING);
    
    // Track which shards we're waiting for
    // TODO: Store pending confirmations
    // entry->pending_confirmations_.clear();
    // for (const auto& [shard_id, ts] : shard_proposals) {
    //     if (ts < agreed_ts) {
    //         entry->pending_confirmations_.insert(shard_id);
    //     }
    // }
    
    return AgreementResult::WAIT_ROUND2;
  }
  
  //-------------------------------------------------------------------------
  // Case 3: We proposed smaller timestamp -> need to reposition
  //-------------------------------------------------------------------------
  Log_info("Luigi Agreement: txn %lu CASE 3 - reposition (proposed=%lu < agreed=%lu)",
           entry->tid_, entry->proposed_ts_, agreed_ts);
  entry->agree_status_.store(LUIGI_AGREE_FLUSHING);
  entry->ts_agreed_.store(true);  // We know the agreed value
  
  return AgreementResult::NEEDS_REPOSITION;
}

LuigiExecutor::AgreementResult LuigiExecutor::PerformAgreementPhase2(
    std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // Phase 2: Confirmation after reposition
  //
  // We were in Case 3 (our proposed_ts < agreed_ts), have repositioned,
  // and now re-entering. We need to:
  // 1. Send confirmation to leaders in Case 2 (who are waiting for us)
  // 2. If all confirmations received, we can proceed to execute
  //===========================================================================
  
  Log_info("Luigi Agreement Phase 2: txn %lu sending confirmations, new_ts=%lu",
           entry->tid_, entry->proposed_ts_);
  
  //===========================================================================
  // TODO: RPC to confirm reposition to other shards
  //
  // for (uint32_t remote_shard : entry->remote_shards_) {
  //     LuigiConfirmRequest req;
  //     req.tid = entry->tid_;
  //     req.new_ts = entry->proposed_ts_;  // Should equal agreed_ts_
  //     req.my_shard_id = partition_id_;
  //     
  //     RPC_ConfirmReposition(remote_shard, req);
  // }
  //
  // PLACEHOLDER: Assume confirmations sent and received successfully
  //===========================================================================
  
  entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
  entry->ts_agreed_.store(true);
  
  Log_info("Luigi Agreement Phase 2: txn %lu complete, ready to execute at ts=%lu",
           entry->tid_, entry->proposed_ts_);
  
  return AgreementResult::AGREEMENT_SUCCESS;
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
