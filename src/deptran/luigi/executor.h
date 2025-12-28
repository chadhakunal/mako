#pragma once

#include "luigi_entry.h"
#include "state_machine.h"

#include <map>
#include <string>
#include <vector>
#include <functional>
#include <memory>

namespace janus {

// Forward declaration
class SchedulerLuigi;

/**
 * LuigiExecutor: Handles transaction execution for Luigi.
 *
 * Responsibilities:
 * - Coordinate leader agreement for multi-shard transactions
 * - Execute read/write operations via callbacks (provided by Mako)
 * - Trigger replication via callbacks (provided by Mako)
 *
 * Design principles:
 * - Clean separation between agreement logic and DB operations
 * - DB operations are delegated to Mako via callbacks
 * - Replication is triggered AFTER execution (like Mako)
 */
class LuigiExecutor {
 public:
  //===========================================================================
  // Callback Types (provided by Mako's ShardReceiver)
  //===========================================================================
  
  // Read callback: (table_id, key) -> (success, value)
  using ReadCallback = std::function<bool(int table_id, const std::string& key, std::string& value_out)>;
  
  // Write callback: (table_id, key, value) -> success
  using WriteCallback = std::function<bool(int table_id, const std::string& key, const std::string& value)>;
  
  // Replication callback: (entry) -> success
  using ReplicationCallback = std::function<bool(const std::shared_ptr<LuigiLogEntry>& entry)>;

  LuigiExecutor();
  ~LuigiExecutor();

  //===========================================================================
  // Configuration
  //===========================================================================
  
  // Set the local partition ID (used for multi-shard detection)
  void SetPartitionId(uint32_t par_id) { partition_id_ = par_id; }
  
  // Set the scheduler reference (for RPC coordination)
  void SetScheduler(SchedulerLuigi* sched) { scheduler_ = sched; }
  
  // Set callbacks for DB operations (provided by Mako)
  void SetReadCallback(ReadCallback cb) { read_cb_ = std::move(cb); }
  void SetWriteCallback(WriteCallback cb) { write_cb_ = std::move(cb); }
  void SetReplicationCallback(ReplicationCallback cb) { replication_cb_ = std::move(cb); }

  //===========================================================================
  // State Machine Mode (Tiga-style stored procedure execution)
  //===========================================================================
  
  // Set the state machine for direct storage access (no STO overhead)
  void SetStateMachine(std::shared_ptr<LuigiStateMachine> sm) {
    state_machine_ = std::move(sm);
  }
  
  // Enable/disable state machine mode (default: disabled, uses callbacks)
  void EnableStateMachineMode(bool enable) { use_state_machine_ = enable; }
  bool IsStateMachineMode() const { return use_state_machine_ && state_machine_ != nullptr; }

  //===========================================================================
  // Main Execution Entry Point
  //===========================================================================
  
  /**
   * Execute a transaction entry.
   * 
   * This is the main entry point called by ExecTd in the scheduler.
   * It handles:
   * 1. Multi-shard detection
   * 2. Leader agreement (if multi-shard)
   * 3. Read/Write execution via callbacks
   * 4. Replication trigger via callback
   * 5. Reply callback invocation
   */
  void Execute(std::shared_ptr<LuigiLogEntry> entry);

 private:
  //===========================================================================
  // Multi-shard Detection
  //===========================================================================
  
  bool IsMultiShard(const std::shared_ptr<LuigiLogEntry>& entry);

  //===========================================================================
  // NOTE: Agreement is now async (Tiga-style bidirectional broadcast)
  // 
  // The scheduler handles agreement via:
  // - InitiateAgreement(): Broadcasts proposal (fire-and-forget)
  // - UpdateDeadlineRecord(): Collects proposals and determines case
  // - Re-enqueues txn with appropriate agree_status_ when complete
  //
  // Execute() handles each status accordingly (see switch statement).
  //===========================================================================

  //===========================================================================
  // Read/Write Operations (delegate to callbacks)
  //===========================================================================
  
  int ExecuteRead(const LuigiOp& op, std::string& value_out);
  int ExecuteWrite(const LuigiOp& op);
  int ExecuteAllOps(std::shared_ptr<LuigiLogEntry> entry);

  //===========================================================================
  // State Machine Mode Execution
  //===========================================================================
  
  int ExecuteViaStateMachine(std::shared_ptr<LuigiLogEntry> entry);

  //===========================================================================
  // Replication (delegate to callback)
  //===========================================================================
  
  int TriggerReplication(std::shared_ptr<LuigiLogEntry> entry);

  //===========================================================================
  // Member Variables
  //===========================================================================
  
  uint32_t partition_id_ = 0;
  SchedulerLuigi* scheduler_ = nullptr;
  
  // Callbacks for DB operations (set by Mako's ShardReceiver)
  ReadCallback read_cb_;
  WriteCallback write_cb_;
  ReplicationCallback replication_cb_;
  
  // State machine for direct execution (Tiga-style)
  std::shared_ptr<LuigiStateMachine> state_machine_;
  bool use_state_machine_ = false;
};

} // namespace janus
