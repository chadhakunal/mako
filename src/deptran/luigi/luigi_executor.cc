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
  uint64_t commit_ts = entry->local_deadline_;
  
  //-------------------------------------------------------------------------
  // Step 1: Multi-shard detection
  //-------------------------------------------------------------------------
  bool is_multi_shard = IsMultiShard(entry);
  
  //-------------------------------------------------------------------------
  // Step 2: Leader agreement (if multi-shard)
  //-------------------------------------------------------------------------
  if (is_multi_shard) {
    if (!PerformLeaderAgreement(entry)) {
      Log_error("Luigi Execute: Leader agreement failed for txn %lu", entry->tid_);
      status = -1;
      goto done;
    }
    // Use agreed timestamp if different from local
    commit_ts = entry->agreed_deadline_;
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
  entry->exec_status_.store(LUIGI_EXEC_DONE);
  
  // Call reply callback
  if (entry->reply_cb_) {
    entry->reply_cb_(status, commit_ts, entry->read_results_);
  }
}

//=============================================================================
// Multi-shard Detection & Agreement
//=============================================================================

bool LuigiExecutor::IsMultiShard(const std::shared_ptr<LuigiLogEntry>& entry) {
  // Check if the transaction has remote_partitions set
  // This would be populated by the coordinator when it knows the txn
  // touches multiple shards
  
  // For now, we rely on the entry having this information from the coordinator
  // The coordinator knows which partitions a txn touches based on the keys
  
  // Simple check: if remote_partitions_ is non-empty, it's multi-shard
  return !entry->remote_partitions_.empty();
}

bool LuigiExecutor::PerformLeaderAgreement(std::shared_ptr<LuigiLogEntry> entry) {
  //===========================================================================
  // PLACEHOLDER: Leader Agreement for Multi-Shard Transactions
  //
  // In the full Tiga protocol:
  // 1. This leader has proposed local_deadline_ based on conflict detection
  // 2. Other leaders involved in this txn have also proposed their deadlines
  // 3. We need to exchange proposals and agree on max(all_proposed_deadlines)
  //
  // Implementation options:
  // a) RPC-based: Send/receive deadline proposals to/from other leaders
  // b) Coordinator-mediated: Coordinator collects all proposals, broadcasts max
  //
  // For now, we implement a simple pass-through for single-shard txns
  // and log a warning for multi-shard txns that would need agreement.
  //===========================================================================
  
  if (entry->remote_partitions_.empty()) {
    // Single-shard: no agreement needed, use local deadline
    entry->agreed_deadline_ = entry->local_deadline_;
    return true;
  }
  
  // Multi-shard case
  Log_info("Luigi: Multi-shard txn %lu detected, partitions involved: %zu + local",
           entry->tid_, entry->remote_partitions_.size());
  
  //===========================================================================
  // TODO: Implement actual leader agreement
  //
  // Pseudocode:
  //   proposed_deadlines = {local_deadline_}
  //   for each remote_partition in remote_partitions_:
  //       // Send our proposal, receive theirs
  //       remote_deadline = RPC_ExchangeDeadline(remote_partition, local_deadline_)
  //       proposed_deadlines.add(remote_deadline)
  //   
  //   agreed_deadline_ = max(proposed_deadlines)
  //
  // For now, just use local deadline (works correctly for single-shard)
  //===========================================================================
  
  entry->agreed_deadline_ = entry->local_deadline_;
  Log_info("Luigi: Using local deadline %lu for multi-shard txn %lu (agreement not yet implemented)",
           entry->agreed_deadline_, entry->tid_);
  
  return true;
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

} // namespace janus
