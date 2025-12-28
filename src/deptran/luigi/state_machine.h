#pragma once

/**
 * LuigiStateMachine - Tiga-style state machine interface for Luigi
 *
 * This provides stored-procedure style execution where:
 * - Transactions are received as complete operation specs
 * - Execution happens locally on the leader
 * - No STO transaction tracking overhead
 *
 * Uses Mako's existing memdb storage layer (identical to Tiga's).
 */

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "memdb/row.h"
#include "memdb/schema.h"
#include "memdb/table.h"
#include "memdb/txn_unsafe.h"
#include "memdb/value.h"

#include "luigi_entry.h"

namespace janus {

/**
 * LuigiStateMachine - Base class for stored-procedure execution
 *
 * Follows Tiga's StateMachine interface but adapted for Luigi's
 * LuigiOp-based operation format.
 */
class LuigiStateMachine {
protected:
  uint32_t shard_id_;
  uint32_t replica_id_;
  uint32_t shard_num_;
  uint32_t replica_num_;

public:
  LuigiStateMachine(uint32_t shard_id, uint32_t replica_id, uint32_t shard_num,
                    uint32_t replica_num)
      : shard_id_(shard_id), replica_id_(replica_id), shard_num_(shard_num),
        replica_num_(replica_num) {}

  virtual ~LuigiStateMachine() = default;

  // Identification
  uint32_t ShardId() const { return shard_id_; }
  uint32_t ReplicaId() const { return replica_id_; }
  uint32_t ShardNum() const { return shard_num_; }
  uint32_t ReplicaNum() const { return replica_num_; }

  virtual std::string RTTI() = 0;

  //=========================================================================
  // Core Execution Interface
  //=========================================================================

  /**
   * Execute a transaction's operations.
   *
   * @param txn_type Transaction type (e.g., NEW_ORDER, PAYMENT)
   * @param working_set Transaction parameters (like Tiga's ws_)
   * @param output Map to store read results (key -> value)
   * @param txn_id Transaction ID for tracking
   * @return true if execution succeeded
   */
  virtual bool Execute(uint32_t txn_type,
                       const std::map<int32_t, std::string> &working_set,
                       std::map<std::string, std::string> *output,
                       uint64_t txn_id = 0) = 0;

  /**
   * Initialize tables for a benchmark.
   * Called once during setup.
   */
  virtual void InitializeTables() = 0;

  /**
   * Populate tables with initial data.
   * Called during data loading phase.
   */
  virtual void PopulateData() {}

  //=========================================================================
  // Shard Mapping (for multi-shard transactions)
  //=========================================================================

  /**
   * Determine which shards are involved in a transaction.
   *
   * @param txn_type Transaction type
   * @param ops Operations in the transaction
   * @param shard_ops Output: map from shard_id to ops for that shard
   */
  virtual void
  MapOpsToShards(uint32_t txn_type, const std::vector<LuigiOp> &ops,
                 std::map<uint32_t, std::vector<LuigiOp>> *shard_ops) {
    // Default: all ops go to current shard
    (*shard_ops)[shard_id_] = ops;
  }

  /**
   * Check if an operation is local to this shard.
   */
  virtual bool IsLocalOp(const LuigiOp &op) const {
    // Default: hash key to shard
    // Subclasses override for benchmark-specific sharding
    return KeyToShard(op.key) == shard_id_;
  }

protected:
  /**
   * Hash a key to a shard ID.
   * Default implementation: simple modulo hash.
   */
  virtual uint32_t KeyToShard(const std::string &key) const {
    if (shard_num_ == 0)
      return 0;

    // FNV-1a hash (matches Mako's mbta_sharded_ordered_index)
    constexpr uint64_t fnv_offset = 14695981039346656037ULL;
    constexpr uint64_t fnv_prime = 1099511628211ULL;
    uint64_t hash = fnv_offset;
    for (char c : key) {
      hash ^= static_cast<uint8_t>(c);
      hash *= fnv_prime;
    }
    return static_cast<uint32_t>(hash) % shard_num_;
  }
};

/**
 * LuigiMicroStateMachine - Simple key-value store for micro benchmarks
 *
 * Uses a simple in-memory map (like Tiga's MicroStateMachine).
 * No schema, no tables - just direct key-value access.
 */
class LuigiMicroStateMachine : public LuigiStateMachine {
private:
  // Simple key-value store (key -> value)
  std::unordered_map<std::string, std::string> kv_store_;

  // Optional: track versions for debugging
  std::unordered_map<std::string, uint64_t> versions_;

public:
  LuigiMicroStateMachine(uint32_t shard_id, uint32_t replica_id,
                         uint32_t shard_num, uint32_t replica_num)
      : LuigiStateMachine(shard_id, replica_id, shard_num, replica_num) {}

  std::string RTTI() override { return "LuigiMicroStateMachine"; }

  bool Execute(uint32_t txn_type,
               const std::map<int32_t, std::string> &working_set,
               std::map<std::string, std::string> *output,
               uint64_t txn_id = 0) override;

  void InitializeTables() override {
    // No tables needed for micro benchmark
    kv_store_.clear();
    versions_.clear();
  }

  void PopulateData() override;

  //=========================================================================
  // Direct Storage Access (for simple benchmarks)
  //=========================================================================

  bool Get(const std::string &key, std::string &value) {
    auto it = kv_store_.find(key);
    if (it != kv_store_.end()) {
      value = it->second;
      return true;
    }
    return false;
  }

  void Put(const std::string &key, const std::string &value) {
    kv_store_[key] = value;
    versions_[key]++;
  }

  bool Exists(const std::string &key) const {
    return kv_store_.find(key) != kv_store_.end();
  }

  size_t Size() const { return kv_store_.size(); }
};

/**
 * LuigiTPCCStateMachine - TPC-C benchmark state machine
 *
 * Uses memdb tables for structured TPC-C data.
 * Follows Tiga's TPCCStateMachine design.
 */
class LuigiTPCCStateMachine : public LuigiStateMachine {
private:
  // Database handler for direct access
  mdb::TxnMgrUnsafe txn_mgr_;

  // TPC-C Tables
  mdb::Table *tbl_warehouse_ = nullptr;
  mdb::Table *tbl_district_ = nullptr;
  mdb::Table *tbl_customer_ = nullptr;
  mdb::Table *tbl_history_ = nullptr;
  mdb::Table *tbl_new_order_ = nullptr;
  mdb::Table *tbl_order_ = nullptr;
  mdb::Table *tbl_order_line_ = nullptr;
  mdb::Table *tbl_order_cid_secondary_ =
      nullptr; // Secondary index: (c_id, d_id, w_id) -> o_id
  mdb::Table *tbl_item_ = nullptr;
  mdb::Table *tbl_stock_ = nullptr;

  // Schemas (owned)
  std::vector<mdb::Schema *> schemas_;

  // TPC-C configuration
  uint32_t num_warehouses_ = 1;
  uint32_t num_districts_per_wh_ = 10;
  uint32_t num_customers_per_district_ = 3000;
  uint32_t num_items_ = 100000;

public:
  LuigiTPCCStateMachine(uint32_t shard_id, uint32_t replica_id,
                        uint32_t shard_num, uint32_t replica_num)
      : LuigiStateMachine(shard_id, replica_id, shard_num, replica_num) {}

  ~LuigiTPCCStateMachine() override;

  std::string RTTI() override { return "LuigiTPCCStateMachine"; }

  bool Execute(uint32_t txn_type,
               const std::map<int32_t, std::string> &working_set,
               std::map<std::string, std::string> *output,
               uint64_t txn_id = 0) override;

  void InitializeTables() override;
  void PopulateData() override;

  void
  MapOpsToShards(uint32_t txn_type, const std::vector<LuigiOp> &ops,
                 std::map<uint32_t, std::vector<LuigiOp>> *shard_ops) override;

  //=========================================================================
  // TPC-C Transaction Implementations
  //=========================================================================

  bool ExecuteNewOrder(const std::map<int32_t, std::string> &working_set,
                       std::map<std::string, std::string> *output,
                       uint64_t txn_id);

  bool ExecutePayment(const std::map<int32_t, std::string> &working_set,
                      std::map<std::string, std::string> *output,
                      uint64_t txn_id);

  bool ExecuteOrderStatus(const std::map<int32_t, std::string> &working_set,
                          std::map<std::string, std::string> *output,
                          uint64_t txn_id);

  bool ExecuteDelivery(const std::map<int32_t, std::string> &working_set,
                       std::map<std::string, std::string> *output,
                       uint64_t txn_id);

  bool ExecuteStockLevel(const std::map<int32_t, std::string> &working_set,
                         std::map<std::string, std::string> *output,
                         uint64_t txn_id);

  //=========================================================================
  // Configuration
  //=========================================================================

  void SetConfig(uint32_t warehouses, uint32_t districts, uint32_t customers,
                 uint32_t items) {
    num_warehouses_ = warehouses;
    num_districts_per_wh_ = districts;
    num_customers_per_district_ = customers;
    num_items_ = items;
  }

protected:
  uint32_t KeyToShard(const std::string &key) const override;
};

} // namespace janus
