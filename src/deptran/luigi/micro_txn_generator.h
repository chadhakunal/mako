#pragma once

/**
 * MicroTxnGenerator - Simple micro benchmark transaction generator for Luigi
 *
 * Generates simple read/write transactions matching Mako's simpleTransaction
 * tests. Uses uniform random key distribution.
 */

#include "txn_generator.h"
#include <unordered_set>

namespace janus {

// Table IDs for micro benchmark
constexpr uint16_t MICRO_TABLE_A = 1;
constexpr uint16_t MICRO_TABLE_B = 2;
constexpr uint16_t MICRO_TABLE_C = 3;
constexpr uint16_t MICRO_TABLE_D = 4;

// Operation types
constexpr uint8_t MICRO_OP_READ = 0;
constexpr uint8_t MICRO_OP_WRITE = 1;

/**
 * Multi-shard micro benchmark generator.
 * Generates transactions that touch one key per shard.
 */
class MicroTxnGenerator : public LuigiTxnGenerator {
private:
  double read_ratio_ = 0.5;

public:
  MicroTxnGenerator(const TxnGeneratorConfig &config)
      : LuigiTxnGenerator(config) {}

  std::string RTTI() override { return "MicroTxnGenerator"; }

  void GetTxnReq(LuigiTxnRequest *req, uint32_t req_id,
                 uint32_t client_id) override {
    req->client_id = client_id;
    req->req_id = req_id;
    req->txn_type = LUIGI_TXN_MICRO;

    req->working_set.clear();
    req->target_shards.clear();
    req->ops.clear();

    bool is_read = RandomDouble() < read_ratio_;
    uint8_t op_type = is_read ? MICRO_OP_READ : MICRO_OP_WRITE;

    // Generate one key per shard (uniform random)
    std::unordered_set<int32_t> used_keys;

    for (uint32_t sid = 0; sid < config_.shard_num; sid++) {
      int32_t key;
      do {
        key = RandomInt(0, config_.key_num);
      } while (used_keys.count(key) > 0);

      used_keys.insert(key);
      std::string key_str = KeyToString(key); // "key_5" format

      // Route using string hash (matches state machine)
      req->target_shards.insert(KeyToShard(key_str));
      req->working_set[key] = "0";

      uint16_t table_id = MICRO_TABLE_A + (sid % 4);
      std::string value_str = is_read ? "" : ValueToString(RandomInt(0, 1000));

      req->ops.push_back(MakeOp(table_id, op_type, key_str, value_str));
    }
  }

  void SetReadRatio(double ratio) {
    read_ratio_ = std::max(0.0, std::min(1.0, ratio));
  }
};

/**
 * Single-shard micro benchmark generator.
 * For baseline testing without distributed coordination.
 */
class SingleShardMicroTxnGenerator : public LuigiTxnGenerator {
private:
  uint32_t fixed_shard_ = 0;
  double read_ratio_ = 0.5;

public:
  SingleShardMicroTxnGenerator(const TxnGeneratorConfig &config,
                               uint32_t shard_id = 0)
      : LuigiTxnGenerator(config), fixed_shard_(shard_id) {}

  std::string RTTI() override { return "SingleShardMicroTxnGenerator"; }

  void GetTxnReq(LuigiTxnRequest *req, uint32_t req_id,
                 uint32_t client_id) override {
    req->client_id = client_id;
    req->req_id = req_id;
    req->txn_type = LUIGI_TXN_MICRO;

    req->working_set.clear();
    req->target_shards.clear();

    int32_t key = RandomInt(0, config_.key_num);
    bool is_read = RandomDouble() < read_ratio_;

    // For reads: empty value, for writes: actual value
    std::string value_str = is_read ? "" : std::to_string(RandomInt(0, 1000));
    req->working_set[key] = value_str;
    req->target_shards.insert(fixed_shard_);
  }

  bool NeedDispatch(const LuigiTxnRequest &req) override {
    return false; // Single shard, no coordination needed
  }

  void SetReadRatio(double ratio) {
    read_ratio_ = std::max(0.0, std::min(1.0, ratio));
  }
};

} // namespace janus
