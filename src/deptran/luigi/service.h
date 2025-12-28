#pragma once

/**
 * LuigiService: RPC request handler for Luigi protocol
 *
 * Implements the LuigiService interface generated from luigi.rpc.
 * Handles incoming RPC requests and dispatches to the scheduler.
 */

#include <chrono>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../constants.h"
#include "../procedure.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "luigi.h" // Generated RRR service interface (LuigiService base class)
#include "luigi_common.h"

namespace janus {

class LuigiServer; // Forward declaration

/**
 * LuigiServiceImpl: Handles incoming Luigi protocol RPC requests.
 *
 * Similar to RaftService - implements RPC handlers only.
 */
class LuigiServiceImpl : public LuigiService {
public:
  explicit LuigiServiceImpl(LuigiServer *server);
  ~LuigiServiceImpl() = default;

  //===========================================================================
  // RRR LuigiService Interface Implementation
  //===========================================================================

  void Dispatch(const rrr::i64 &txn_id, const rrr::i64 &expected_time,
                const rrr::i32 &worker_id,
                const std::vector<rrr::i32> &involved_shards,
                const std::string &ops_data, rrr::i32 *status,
                rrr::i64 *commit_timestamp, std::string *results_data,
                rrr::DeferredReply *defer);

  void OwdPing(const rrr::i64 &send_time, rrr::i32 *status,
               rrr::DeferredReply *defer);

  void DeadlinePropose(const rrr::i64 &tid, const rrr::i32 &src_shard,
                       const rrr::i64 &proposed_ts, rrr::i32 *status,
                       rrr::DeferredReply *defer);

  void DeadlineConfirm(const rrr::i64 &tid, const rrr::i32 &src_shard,
                       const rrr::i64 &agreed_ts, rrr::i32 *status,
                       rrr::DeferredReply *defer);

  void WatermarkExchange(const rrr::i32 &src_shard,
                         const std::vector<rrr::i64> &watermarks,
                         rrr::i32 *status, rrr::DeferredReply *defer);

  // Phase 2: Batch RPC handlers (includes piggybacked watermarks)
  void DeadlineBatchPropose(const std::vector<rrr::i64> &tids,
                            const rrr::i32 &src_shard,
                            const std::vector<rrr::i64> &proposed_timestamps,
                            const std::vector<rrr::i64> &watermarks,
                            rrr::i32 *status, rrr::DeferredReply *defer);

  void DeadlineBatchConfirm(const std::vector<rrr::i64> &tids,
                            const rrr::i32 &src_shard,
                            const std::vector<rrr::i64> &agreed_timestamps,
                            rrr::i32 *status, rrr::DeferredReply *defer);

  // Phase 4: Replicate RPC handler (follower-side log append)
  void Replicate(const rrr::i32 &worker_id, const rrr::i64 &slot_id,
                 const rrr::i64 &txn_id, const rrr::i64 &timestamp,
                 const std::string &log_data, rrr::i32 *status,
                 rrr::DeferredReply *defer);

  // Phase 4: BatchReplicate RPC handler (batch log append, Raft-style)
  void BatchReplicate(const rrr::i32 &worker_id,
                      const rrr::i64 &prev_committed_slot,
                      const std::vector<rrr::i64> &slot_ids,
                      const std::vector<rrr::i64> &txn_ids,
                      const std::vector<rrr::i64> &timestamps,
                      const std::vector<std::string> &log_entries,
                      rrr::i32 *status, rrr::i64 *last_appended_slot,
                      rrr::DeferredReply *defer);

protected:
  //===========================================================================
  // Result Storage (for async polling)
  //===========================================================================

  struct TxnResult {
    int status;
    uint64_t commit_timestamp;
    std::vector<std::string> read_results;
    std::chrono::steady_clock::time_point completion_time;
  };

  void StoreResult(uint64_t txn_id, int status, uint64_t commit_ts,
                   const std::vector<std::string> &read_results);

private:
  LuigiServer *server_; // Back-pointer to server

  // Async result storage
  std::unordered_map<uint64_t, TxnResult> completed_txns_;
  mutable std::shared_mutex results_mutex_;
};

} // namespace janus
