#pragma once

/**
 * Luigi-specific message types and constants.
 *
 * This file defines all the request/response structures for Luigi protocol
 * communication over Mako's eRPC transport.
 */

#include <cstdint>
#include <cstring>
#include <string>

namespace janus {
namespace luigi {

//=============================================================================
// Constants
//=============================================================================

// Maximum number of key-value pairs in a Luigi dispatch
constexpr size_t kMaxOps = 32;
constexpr size_t kMaxShards = 16; // Max shards involved in one txn
constexpr size_t kMaxKeyLength = 128;
constexpr size_t kMaxValueLength = 512;

// Request types (must match mako::common.h for transport compatibility)
constexpr uint8_t kLuigiDispatchReqType = 14;
constexpr uint8_t kOwdPingReqType = 15;
constexpr uint8_t kLuigiStatusReqType = 16;
constexpr uint8_t kDeadlineProposeReqType = 17;
constexpr uint8_t kDeadlineConfirmReqType = 18;
constexpr uint8_t kWatermarkExchangeReqType = 19;

// Operation types
constexpr uint8_t kOpRead = 0;
constexpr uint8_t kOpWrite = 1;

// Status values for async Luigi dispatch
constexpr int kStatusQueued = 100;   // Txn queued, not yet complete
constexpr int kStatusComplete = 101; // Txn completed successfully
constexpr int kStatusAborted = 102;  // Txn aborted
constexpr int kStatusNotFound = 103; // Txn not found (expired or invalid)

// Error codes
constexpr int kOk = 0;
constexpr int kAbort = 1;

//=============================================================================
// Request/Response Structures
//=============================================================================

struct DispatchRequest {
  uint16_t target_server_id; // Target shard
  uint32_t req_nr;           // Request number (for matching response)
  uint64_t txn_id;           // Unique transaction ID
  uint64_t expected_time;    // Timestamp at which transaction should execute
  uint32_t worker_id; // Worker/client thread ID (for per-worker replication)
  uint16_t num_ops;   // Number of operations in this dispatch
  uint16_t num_involved_shards;         // Number of shards involved in this txn
  uint16_t involved_shards[kMaxShards]; // List of all involved shard IDs
  // Each op: [table_id(2) | op_type(1) | klen(2) | vlen(2) | key | value]
  char ops_data[kMaxOps * (kMaxKeyLength + kMaxValueLength + 8)];
};

struct DispatchResponse {
  uint32_t req_nr;
  uint64_t txn_id;
  int status;                // SUCCESS or ABORT
  uint64_t commit_timestamp; // The timestamp at which txn was committed
  uint16_t num_results;      // Number of read results
  char results_data[kMaxOps * kMaxValueLength];
};

struct StatusRequest {
  uint16_t target_server_id; // Target shard
  uint32_t req_nr;           // Request number
  uint64_t txn_id;           // Transaction ID to check
};

struct StatusResponse {
  uint32_t req_nr;
  uint64_t txn_id;
  int status;                // QUEUED, COMPLETE, ABORTED, NOT_FOUND
  uint64_t commit_timestamp; // Valid only if COMPLETE
  uint16_t num_results;      // Number of read results (valid if COMPLETE)
  char results_data[kMaxOps * kMaxValueLength];
};

struct OwdPingRequest {
  uint16_t target_server_id; // Target shard
  uint32_t req_nr;           // Request number
  uint64_t send_time;        // Timestamp when ping was sent
};

struct OwdPingResponse {
  uint32_t req_nr; // Echo back request number
  int status;      // 0 = OK
};

//=============================================================================
// Coordination RPC Structures (Leader-to-Leader)
//=============================================================================

struct DeadlineProposeRequest {
  uint16_t target_server_id; // Target shard
  uint32_t req_nr;           // Request number
  uint64_t tid;              // Transaction ID
  uint64_t proposed_ts;      // Proposing leader's timestamp
  uint32_t src_shard;        // Source shard ID
  uint32_t phase;            // 1 = initial proposal, 2 = confirmation
};

struct DeadlineProposeResponse {
  uint32_t req_nr;      // Echo back request number
  uint64_t tid;         // Transaction ID
  uint64_t proposed_ts; // This shard's proposal (0 if unknown)
  uint32_t shard_id;    // This shard's ID
  int32_t status;       // 0 = OK
};

struct DeadlineConfirmRequest {
  uint16_t target_server_id; // Target shard
  uint32_t req_nr;           // Request number
  uint64_t tid;              // Transaction ID
  uint32_t src_shard;        // Source shard ID
  uint64_t new_ts;           // New timestamp after repositioning
};

struct DeadlineConfirmResponse {
  uint32_t req_nr; // Echo back request number
  int32_t status;  // 0 = OK
};

struct WatermarkExchangeRequest {
  uint16_t target_server_id; // Target shard
  uint32_t req_nr;           // Request number
  uint32_t src_shard;        // Source shard ID
  uint16_t num_watermarks;   // Number of watermarks in array
  uint64_t watermarks[32];   // Watermark values (support up to 32 workers)
};

struct WatermarkExchangeResponse {
  uint32_t req_nr; // Echo back request number
  int32_t status;  // 0 = OK
};

//=============================================================================
// Operation structure (used internally)
//=============================================================================

struct Op {
  uint16_t table_id;
  uint8_t op_type; // kOpRead or kOpWrite
  std::string key;
  std::string value; // For writes only
};

} // namespace luigi
} // namespace janus
