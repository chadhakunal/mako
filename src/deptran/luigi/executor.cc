#include "executor.h"
#include "scheduler.h" // For RPC coordination
#include "state_machine.h"

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
  // Guard against double execution - atomically transition from INIT to DIRECT
  uint32_t expected = LUIGI_EXEC_INIT;
  if (!entry->exec_status_.compare_exchange_strong(
          expected, static_cast<uint32_t>(LUIGI_EXEC_DIRECT))) {
    // Already executing or completed - skip
    Log_debug("Luigi Execute: txn %lu already executing/completed (status=%d)",
              entry->tid_, expected);
    return;
  }

  int status = 0; // SUCCESS
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
      Log_debug("Luigi Execute: txn %lu initiating agreement (async)",
                entry->tid_);
      // Mark as pending BEFORE initiating to prevent re-entry
      entry->agree_status_.store(LUIGI_AGREE_PENDING);
      if (scheduler_ != nullptr) {
        scheduler_->InitiateAgreement(entry);
      }
      // Keep exec_status_ as DIRECT (don't reset to INIT) to prevent
      // another Execute() call from passing the CAS guard
      // When UpdateDeadlineRecord sets COMPLETE, it will re-enqueue
      // the entry which will be picked up but rejected by CAS
      // UNLESS we're in COMPLETE state, in which case we want to allow it

      // If agreement completed during InitiateAgreement (synchronous case),
      // we should proceed to execution. Check the status again:
      if (entry->agree_status_.load() == LUIGI_AGREE_COMPLETE) {
        // Agreement already completed! Fall through to execution
        commit_ts = entry->agreed_ts_;
        break;
      }
      // Still waiting for agreement - return without executing
      return;

    case LUIGI_AGREE_FLUSHING:
      //-------------------------------------------------------------------
      // Case 3: We had smaller timestamp, need to reposition
      // Update our proposed_ts and send confirmations
      //-------------------------------------------------------------------
      Log_debug(
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
      Log_debug("Luigi Execute: txn %lu still waiting for confirmations",
                entry->tid_);
      // The scheduler's HandleRemoteDeadlineConfirm will re-enqueue us
      // when all confirmations arrive
      entry->exec_status_.store(LUIGI_EXEC_INIT);
      return;

    case LUIGI_AGREE_PENDING:
      //-------------------------------------------------------------------
      // Agreement initiated, waiting for all proposals to arrive
      // This should not normally happen - ExecTd should filter these out
      //-------------------------------------------------------------------
      Log_debug("Luigi Execute: txn %lu still pending agreement", entry->tid_);
      entry->exec_status_.store(LUIGI_EXEC_INIT);
      return;

    case LUIGI_AGREE_COMPLETE:
      //-------------------------------------------------------------------
      // Agreement complete! Proceed to execution
      // Note: ts_agreed_ is already set by UpdateDeadlineRecord()
      //-------------------------------------------------------------------
      Log_debug("Luigi Execute: txn %lu agreement complete at ts=%lu",
                entry->tid_, entry->agreed_ts_);
      commit_ts = entry->agreed_ts_;
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
  if (use_state_machine_ && state_machine_) {
    status = ExecuteViaStateMachine(entry);
  } else {
    status = ExecuteAllOps(entry);
  }
  if (status != 0) {
    Log_error("Luigi Execute: Operation execution failed for txn %lu",
              entry->tid_);
    goto done;
  }

  //-------------------------------------------------------------------------
  // Step 4: Trigger replication (background Paxos)
  //-------------------------------------------------------------------------
  // Note: We trigger replication even if not all reads succeeded
  // This matches Mako's behavior - replication happens for committed txns
  status = TriggerReplication(entry);
  if (status != 0) {
    Log_error("Luigi Execute: Replication trigger failed for txn %lu",
              entry->tid_);
    // Don't abort - replication failure is handled by Paxos recovery
    status = 0; // Reset status, txn still committed locally
  }

done:
  entry->exec_status_.store(LUIGI_EXEC_COMPLETE);

  // Call reply callback
  if (entry->reply_cb_) {
    Log_debug("Luigi Execute: calling reply_cb for txn %lu status=%d ts=%lu",
              entry->tid_, status, commit_ts);
    entry->reply_cb_(status, commit_ts, entry->read_results_);
  } else {
    Log_warn("Luigi Execute: no reply_cb for txn %lu!", entry->tid_);
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
  // We determine the worker ID from the transaction metadata.
  //-------------------------------------------------------------------------

  // Use worker_id field directly (not extracted from txn_id)
  uint32_t worker_id = entry->worker_id_;

  // Use scheduler's Replication layer
  if (scheduler_) {
    scheduler_->Replicate(worker_id, entry);
    return 0;
  } else {
    Log_error("Luigi TriggerReplication: scheduler not set!");
    return -1;
  }
  // NOTE: In the original Mako code, there was a replication_cb_.
  // We've replaced that pattern with the Scheduler's Replicate method
  // which handles the per-stream logic and watermark updates.
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
    Log_error("Luigi ExecuteViaStateMachine: state machine not set!");
    return -1;
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
