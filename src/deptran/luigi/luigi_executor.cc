#include "luigi_executor.h"
#include "luigi_scheduler.h" // For RPC coordination
#include "luigi_state_machine.h"

#include <algorithm>
#include <chrono>
#include <set>

#include "deptran/__dep__.h" // For logging macros

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
  try {
  // ALWAYS log for first 10 transactions or every 50th
  if (entry->tid_ <= 10 || entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR] TXN-%lu: Execute called", entry->tid_);
  }

  int status = 0; // SUCCESS
  uint64_t commit_ts = entry->proposed_ts_;

  //-------------------------------------------------------------------------
  // Step 1: Multi-shard detection
  //-------------------------------------------------------------------------
  bool is_multi_shard = IsMultiShard(entry);
  
  if (entry->tid_ <= 10 || entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR] TXN-%lu: Multi-shard=%d", entry->tid_, is_multi_shard);
  }

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
      // Multi-shard: Initiate Tiga-style leader agreement
      // This broadcasts our proposal to other shards asynchronously.
      // Agreement completes when UpdateDeadlineRecord() receives all proposals.
      //-------------------------------------------------------------------
      if (scheduler_ != nullptr) {
        scheduler_->InitiateAgreement(entry);
        Log_info("Luigi Execute: txn %lu initiated agreement", entry->tid_);
      } else {
        // Fallback if no scheduler (shouldn't happen in normal flow)
        Log_warn("Luigi Execute: txn %lu no scheduler, fallback to direct",
                 entry->tid_);
        entry->agree_status_.store(LUIGI_AGREE_COMPLETE);
        entry->agreed_ts_ = entry->proposed_ts_;
        entry->ts_agreed_.store(true);
      }
      // Return - agreement will complete asynchronously
      // UpdateDeadlineRecord() will re-enqueue when all proposals arrive
      return;

    case LUIGI_AGREE_FLUSHING:
      //-------------------------------------------------------------------
      // Case 3: We had smaller timestamp, need to reposition
      // Update our proposed_ts and send confirmations
      //-------------------------------------------------------------------
      Log_info(
          "Luigi Execute: txn %lu repositioning (proposed=%lu -> agreed=%lu)",
          entry->tid_, entry->proposed_ts_, entry->agreed_ts_);

      // Send Phase 2 confirmations to other leaders
      if (scheduler_ != nullptr) {
        entry->proposed_ts_ = entry->agreed_ts_; // Update our timestamp
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
      Log_info("Luigi Execute: txn %lu still waiting for confirmations",
               entry->tid_);
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
  // Choose between callback mode (Mako) and state machine mode (Tiga-style)
  //-------------------------------------------------------------------------
  if (entry->tid_ <= 10 || entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR] TXN-%lu: Executing operations (mode=%s)",
             entry->tid_, (use_state_machine_ && state_machine_) ? "state_machine" : "callbacks");
  }
  
  if (use_state_machine_ && state_machine_) {
    status = ExecuteViaStateMachine(entry);
  } else {
    status = ExecuteAllOps(entry);
  }
  
  if (status != 0) {
    Log_error("[EXECUTOR] TXN-%lu: Operation execution FAILED (status=%d)", entry->tid_, status);
    goto done;
  }
  
  if (entry->tid_ <= 10 || entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR] TXN-%lu: Operations executed successfully", entry->tid_);
  }

  //-------------------------------------------------------------------------
  // Step 4: Trigger replication (background Paxos)
  // TEMPORARILY DISABLED for debugging
  //-------------------------------------------------------------------------
  // Note: We trigger replication even if not all reads succeeded
  // This matches Mako's behavior - replication happens for committed txns
  if (entry->tid_ <= 10 || entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR] TXN-%lu: SKIPPING replication for debugging", entry->tid_);
  }
  // DISABLED: status = TriggerReplication(entry);

done:
  entry->exec_status_.store(LUIGI_EXEC_COMPLETE);

  if (entry->tid_ <= 10 || entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR] TXN-%lu: About to call reply callback (has_cb=%d)",
             entry->tid_, entry->reply_cb_ != nullptr);
  }
  // Call reply callback
  if (entry->reply_cb_) {
    entry->reply_cb_(status, commit_ts, entry->read_results_);
  }
  if (entry->tid_ <= 10 || entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR] TXN-%lu: Reply callback completed", entry->tid_);
  }
  } catch (int e) {
    Log_error("[EXECUTOR] TXN-%lu: Caught int exception %d", entry->tid_, e);
    throw; // Re-throw to be caught by ExecTd
  } catch (const std::exception& e) {
    Log_error("[EXECUTOR] TXN-%lu: Caught std::exception: %s", entry->tid_, e.what());
    throw;
  } catch (...) {
    Log_error("[EXECUTOR] TXN-%lu: Caught unknown exception", entry->tid_);
    throw;
  }
}

//=============================================================================
// Multi-shard Detection & Agreement
//=============================================================================

bool LuigiExecutor::IsMultiShard(const std::shared_ptr<LuigiLogEntry> &entry) {
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

int LuigiExecutor::ExecuteRead(const LuigiOp &op, std::string &value_out) {
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
    return 0; // SUCCESS
  } catch (const std::exception &ex) {
    Log_error("Luigi ExecuteRead: exception in table %d: %s", op.table_id,
              ex.what());
    return -1;
  } catch (...) {
    Log_error("Luigi ExecuteRead: unknown exception in table %d", op.table_id);
    return -1;
  }
}

//=============================================================================
// Write Operations (delegate to callback)
//=============================================================================

int LuigiExecutor::ExecuteWrite(const LuigiOp &op) {
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
    return 0; // SUCCESS
  } catch (const std::exception &ex) {
    Log_error("Luigi ExecuteWrite: exception in table %d: %s", op.table_id,
              ex.what());
    return -1;
  } catch (...) {
    Log_error("Luigi ExecuteWrite: unknown exception in table %d", op.table_id);
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

  Log_debug(
      "Luigi ExecuteAllOps: txn %lu has %zu ops, %zu local keys, filter=%d",
      entry->tid_, entry->ops_.size(), local_key_set.size(), should_filter);

  //-------------------------------------------------------------------------
  // Phase 1: Execute READ operations (only for local keys)
  //-------------------------------------------------------------------------
  for (auto &op : entry->ops_) {
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
        Log_debug(
            "Luigi ExecuteAllOps: Skipping remote read key=%s for txn %lu",
            op.key.c_str(), entry->tid_);
      }
    }
  }

  //-------------------------------------------------------------------------
  // Phase 2: Execute WRITE operations (only for local keys)
  //-------------------------------------------------------------------------
  for (auto &op : entry->ops_) {
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
        Log_debug(
            "Luigi ExecuteAllOps: Skipping remote write key=%s for txn %lu",
            op.key.c_str(), entry->tid_);
      }
    }
  }

  return 0; // SUCCESS
}

//=============================================================================
// Replication Trigger (Abstracted)
//=============================================================================

int LuigiExecutor::TriggerReplication(std::shared_ptr<LuigiLogEntry> entry) {
  //-------------------------------------------------------------------------
  // In Tiga, replication happens via per-worker Paxos streams.
  // We determine the worker/stream ID from the transaction metadata.
  //-------------------------------------------------------------------------

  // Extract worker ID (bits 63-48 of txn_id)
  uint32_t worker_id = (uint32_t)((entry->tid_ >> 48) & 0xFFFF);

  if (entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR-REPLICATE] TXN-%lu: Triggering replication (worker_id=%u)",
             entry->tid_, worker_id);
  }

  // Use scheduler's Replication layer
  if (scheduler_) {
    scheduler_->Replicate(worker_id, entry);
  } else {
    Log_error("[EXECUTOR-REPLICATE] TXN-%lu: Scheduler not set!", entry->tid_);
    return -1;
  }
  
  if (entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR-REPLICATE] TXN-%lu: Replication triggered", entry->tid_);
  }

  // NOTE: In the original Mako code, there was a replication_cb_.
  // We've replaced that pattern with the Scheduler's Replicate method
  // which handles the per-stream logic and watermark updates.

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

//=============================================================================
// State Machine Mode Execution (Tiga-style stored procedures)
//
// When using state machine mode:
// - No STO transaction tracking overhead
// - Direct storage access via memdb
// - Execution logic contained in the state machine
//
// This mode is used when Luigi runs independently (not integrated with Mako).
//=============================================================================

int LuigiExecutor::ExecuteViaStateMachine(
    std::shared_ptr<LuigiLogEntry> entry) {
  if (!state_machine_) {
    Log_error("[EXECUTOR-SM] TXN-%lu: State machine not set!", entry->tid_);
    return -1;
  }
  
  if (entry->tid_ % 50 == 0) {
    Log_info("[EXECUTOR-SM] TXN-%lu: Executing via state machine", entry->tid_);
  }

  // Filter operations for local keys (same logic as ExecuteAllOps)
  std::vector<LuigiOp> local_ops;
  std::set<std::string> local_key_set;

  // Build local key set
  for (int32_t k : entry->local_keys_) {
    local_key_set.insert(std::to_string(k));
  }
  if (local_key_set.empty() && !entry->shard_to_keys_.empty()) {
    auto it = entry->shard_to_keys_.find(partition_id_);
    if (it != entry->shard_to_keys_.end()) {
      for (int32_t k : it->second) {
        local_key_set.insert(std::to_string(k));
      }
    }
  }

  bool should_filter = !local_key_set.empty();

  // Collect local operations
  for (const auto &op : entry->ops_) {
    bool is_local = !should_filter || (local_key_set.count(op.key) > 0);
    if (is_local) {
      local_ops.push_back(op);
    }
  }

  Log_debug(
      "Luigi ExecuteViaStateMachine: txn %lu executing %zu local ops via %s",
      entry->tid_, local_ops.size(), state_machine_->RTTI().c_str());

  // Execute via state machine (using working_set for TPC-C)
  std::map<std::string, std::string> output;
  bool success = state_machine_->Execute(
      entry->txn_type_,
      entry->working_set_, // Pass TPC-C parameters instead of ops
      &output, entry->tid_);

  if (!success) {
    Log_error("Luigi ExecuteViaStateMachine: execution failed for txn %lu",
              entry->tid_);
    return -1;
  }

  // Populate read results from output
  entry->read_results_.clear();
  for (const auto &op : local_ops) {
    if (op.op_type == LUIGI_OP_READ) {
      auto it = output.find(op.key);
      if (it != output.end()) {
        entry->read_results_.push_back(it->second);
      } else {
        entry->read_results_.push_back("");
      }
    }
  }

  // Mark operations as executed
  for (auto &op : entry->ops_) {
    bool is_local = !should_filter || (local_key_set.count(op.key) > 0);
    if (is_local) {
      op.executed = true;
    }
  }

  return 0; // SUCCESS
}

} // namespace janus
