#pragma once

#include "luigi_entry.h"

#include <map>
#include <string>
#include <vector>
#include <functional>

// Forward declaration
class abstract_ordered_index;

namespace janus {

/**
 * LuigiExecutor: Handles transaction execution for Luigi.
 *
 * Responsibilities:
 * - Execute read operations (shard_get)
 * - Execute write operations (shard_put)
 * - Multi-shard detection and leader agreement coordination
 * - Trigger replication via add_log_to_nc
 *
 * Design principles:
 * - Clean separation between read/write logic
 * - Explicit multi-shard vs single-shard handling
 * - Replication is triggered AFTER execution (like Mako)
 */
class LuigiExecutor {
 public:
  LuigiExecutor();
  ~LuigiExecutor();

  //===========================================================================
  // Configuration
  //===========================================================================
  
  // Set the database tables reference (must be called before Execute)
  void SetDbTables(std::map<int, abstract_ordered_index*>* tables) { 
    db_tables_ = tables; 
  }
  
  // Set the local partition ID (used for multi-shard detection)
  void SetPartitionId(uint32_t par_id) { partition_id_ = par_id; }

  //===========================================================================
  // Main Execution Entry Point
  //===========================================================================
  
  /**
   * Execute a transaction entry.
   * 
   * This is the main entry point called by ExecTd in the scheduler.
   * It handles:
   * 1. Multi-shard detection
   * 2. Leader agreement (if multi-shard) - placeholder
   * 3. Read/Write execution
   * 4. Replication trigger
   * 5. Callback invocation
   */
  void Execute(std::shared_ptr<LuigiLogEntry> entry);

 private:
  //===========================================================================
  // Multi-shard Detection & Agreement
  //===========================================================================
  
  /**
   * Check if a transaction involves multiple shards/partitions.
   * 
   * @param entry The transaction entry
   * @return true if the transaction touches keys on other partitions
   * 
   * NOTE: For now, we assume all ops in entry are for the local partition.
   *       Multi-shard detection requires partition-to-key mapping info
   *       from the coordinator.
   */
  bool IsMultiShard(const std::shared_ptr<LuigiLogEntry>& entry);

  /**
   * Perform leader agreement for multi-shard transactions.
   * 
   * In Tiga, when a txn touches multiple shards:
   * 1. Each leader proposes its local_deadline
   * 2. Leaders exchange proposals
   * 3. All leaders agree on max(proposed_deadlines)
   * 4. Txn executes at the agreed timestamp
   * 
   * @param entry The transaction entry (may update entry->agreed_deadline_)
   * @return true if agreement succeeded, false on timeout/failure
   * 
   * PLACEHOLDER: Currently returns true immediately for single-shard txns.
   *              Multi-shard agreement requires RPC to other leaders.
   */
  bool PerformLeaderAgreement(std::shared_ptr<LuigiLogEntry> entry);

  //===========================================================================
  // Read/Write Operations
  //===========================================================================
  
  /**
   * Execute a single read operation.
   * 
   * @param op The read operation
   * @param value_out Output: the read value (empty if not found)
   * @return 0 on success, -1 on error
   */
  int ExecuteRead(const LuigiOp& op, std::string& value_out);

  /**
   * Execute a single write operation.
   * 
   * @param op The write operation
   * @return 0 on success, -1 on error
   */
  int ExecuteWrite(const LuigiOp& op);

  /**
   * Execute all operations in a transaction.
   * 
   * Processes reads first (for snapshot isolation semantics), then writes.
   * Results are stored in entry->read_results_.
   * 
   * @param entry The transaction entry
   * @return 0 on success, -1 on any operation failure
   */
  int ExecuteAllOps(std::shared_ptr<LuigiLogEntry> entry);

  //===========================================================================
  // Replication
  //===========================================================================
  
  /**
   * Trigger background replication via Mako's Paxos.
   * 
   * Serializes the transaction log and calls add_log_to_nc().
   * This happens AFTER execution (Mako's design).
   * 
   * @param entry The executed transaction entry
   * @return 0 on success, -1 on serialization error
   * 
   * TODO: Implement log serialization format
   */
  int TriggerReplication(std::shared_ptr<LuigiLogEntry> entry);

  //===========================================================================
  // Member Variables
  //===========================================================================
  
  // Database tables reference (from server.cc)
  std::map<int, abstract_ordered_index*>* db_tables_ = nullptr;
  
  // Local partition ID
  uint32_t partition_id_ = 0;
  
  // For multi-shard: set of partition IDs that this server leads
  // (Currently unused - placeholder for future multi-shard support)
  // std::unordered_set<uint32_t> local_partitions_;
};

} // namespace janus
