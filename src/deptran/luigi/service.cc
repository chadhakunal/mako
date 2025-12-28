#include "service.h"
#include "scheduler.h"
#include "server.h"

#include "deptran/__dep__.h"
#include "luigi_common.h"

#include <algorithm>
#include <chrono>

namespace janus {

LuigiServiceImpl::LuigiServiceImpl(LuigiServer *server) : server_(server) {}

//=============================================================================
// RRR Handler Implementations
//=============================================================================

void LuigiServiceImpl::Dispatch(
    const rrr::i64 &txn_id, const rrr::i64 &expected_time,
    const rrr::i32 &worker_id, const std::vector<rrr::i32> &involved_shards,
    const std::string &ops_data, rrr::i32 *status, rrr::i64 *commit_timestamp,
    std::string *results_data, rrr::DeferredReply *defer) {

  Log_debug("LuigiDispatch: received txn_id=%ld worker_id=%d op_size=%zu",
            txn_id, worker_id, ops_data.size());

  // Parse operations from binary data
  std::vector<LuigiOp> ops;
  const char *data_ptr = ops_data.data();
  const char *data_end = data_ptr + ops_data.size();

  while (data_ptr < data_end) {
    LuigiOp op;

    if (data_ptr + sizeof(uint16_t) > data_end)
      break;
    op.table_id = *reinterpret_cast<const uint16_t *>(data_ptr);
    data_ptr += sizeof(uint16_t);

    if (data_ptr + sizeof(uint8_t) > data_end)
      break;
    op.op_type = *reinterpret_cast<const uint8_t *>(data_ptr);
    data_ptr += sizeof(uint8_t);

    if (data_ptr + sizeof(uint16_t) > data_end)
      break;
    uint16_t klen = *reinterpret_cast<const uint16_t *>(data_ptr);
    data_ptr += sizeof(uint16_t);

    if (data_ptr + sizeof(uint16_t) > data_end)
      break;
    uint16_t vlen = *reinterpret_cast<const uint16_t *>(data_ptr);
    data_ptr += sizeof(uint16_t);

    if (data_ptr + klen > data_end)
      break;
    op.key.assign(data_ptr, klen);
    data_ptr += klen;

    if (vlen > 0) {
      if (data_ptr + vlen > data_end)
        break;
      op.value.assign(data_ptr, vlen);
      data_ptr += vlen;
    }

    ops.push_back(op);
  }

  // Convert involved shards
  std::vector<uint32_t> involved_shards_u32;
  for (auto shard : involved_shards) {
    involved_shards_u32.push_back(static_cast<uint32_t>(shard));
  }

  auto *scheduler = server_->GetScheduler();
  if (scheduler == nullptr) {
    Log_warn("Luigi scheduler not initialized");
    *status = luigi::kAbort;
    *commit_timestamp = 0;
    defer->reply();
    return;
  }

  // Keep pointer to defer for async reply after execution completes
  auto defer_ptr = defer;
  auto status_ptr = status;
  auto commit_timestamp_ptr = commit_timestamp;
  auto results_data_ptr = results_data;
  auto scheduler_ptr = scheduler; // Capture scheduler for watermark check

  scheduler->LuigiDispatchFromRequest(
      txn_id, expected_time, ops, involved_shards_u32, worker_id,
      [defer_ptr, status_ptr, commit_timestamp_ptr, results_data_ptr, txn_id,
       scheduler_ptr, worker_id,
       involved_shards_u32](int result_status, uint64_t commit_ts,
                            const std::vector<std::string> &read_results) {
        // Set output values
        *status_ptr = result_status;
        *commit_timestamp_ptr = commit_ts;
        results_data_ptr->clear();
        for (const auto &r : read_results) {
          results_data_ptr->append(r);
        }

        // If transaction failed, reply immediately
        if (result_status != luigi::kOk) {
          Log_debug("Service callback: txn=%ld ABORTED, replying immediately",
                    txn_id);
          defer_ptr->reply();
          return;
        }

        // Reply immediately after execution - don't wait for watermarks
        // (Watermark infrastructure kept for future use but not blocking
        // commits)
        Log_debug("Service callback: txn=%ld COMMIT immediate", txn_id);
        defer_ptr->reply();
      });
}

void LuigiServiceImpl::OwdPing(const rrr::i64 &send_time, rrr::i32 *status,
                               rrr::DeferredReply *defer) {
  Log_debug("OwdPing: received ping with send_time=%ld", send_time);
  *status = luigi::kOk;
  defer->reply();
}

void LuigiServiceImpl::DeadlinePropose(const rrr::i64 &tid,
                                       const rrr::i32 &src_shard,
                                       const rrr::i64 &proposed_ts,
                                       rrr::i32 *status,
                                       rrr::DeferredReply *defer) {

  Log_debug("DeadlinePropose: tid=%ld from shard %d ts=%ld", tid, src_shard,
            proposed_ts);

  // Record the proposed timestamp from src_shard for this transaction
  auto *scheduler = server_->GetScheduler();
  if (scheduler != nullptr) {
    scheduler->HandleRemoteDeadlineProposal(tid, src_shard, proposed_ts, 1);
  }

  *status = luigi::kOk;
  defer->reply();
}

void LuigiServiceImpl::DeadlineConfirm(const rrr::i64 &tid,
                                       const rrr::i32 &src_shard,
                                       const rrr::i64 &agreed_ts,
                                       rrr::i32 *status,
                                       rrr::DeferredReply *defer) {

  Log_debug("DeadlineConfirm: tid=%ld from shard %d ts=%ld", tid, src_shard,
            agreed_ts);

  // Record the confirmation (phase 2) from src_shard for this transaction
  auto *scheduler = server_->GetScheduler();
  if (scheduler != nullptr) {
    scheduler->HandleRemoteDeadlineConfirm(tid, src_shard, agreed_ts);
  }

  *status = luigi::kOk;
  defer->reply();
}

void LuigiServiceImpl::WatermarkExchange(
    const rrr::i32 &src_shard, const std::vector<rrr::i64> &watermarks,
    rrr::i32 *status, rrr::DeferredReply *defer) {

  // Forward to scheduler to store remote watermarks
  auto *scheduler = server_->GetScheduler();
  if (scheduler != nullptr) {
    scheduler->HandleWatermarkExchange(src_shard, watermarks);
    Log_debug("WatermarkExchange: received %zu watermarks from shard %d",
              watermarks.size(), src_shard);
  }

  *status = luigi::kOk;
  defer->reply();
}

//=============================================================================
// Result Storage
//=============================================================================

void LuigiServiceImpl::StoreResult(
    uint64_t txn_id, int status, uint64_t commit_ts,
    const std::vector<std::string> &read_results) {

  std::unique_lock<std::shared_mutex> lock(results_mutex_);

  TxnResult result;
  result.status =
      (status == luigi::kOk) ? luigi::kStatusComplete : luigi::kStatusAborted;
  result.commit_timestamp = commit_ts;
  result.read_results = read_results;
  result.completion_time = std::chrono::steady_clock::now();

  completed_txns_[txn_id] = std::move(result);

  Log_debug("Luigi result stored for txn %lu: status=%d, commit_ts=%lu", txn_id,
            result.status, commit_ts);
}

//=============================================================================
// PHASE 2: BATCH RPC HANDLERS
//=============================================================================

void LuigiServiceImpl::DeadlineBatchPropose(
    const std::vector<rrr::i64> &tids, const rrr::i32 &src_shard,
    const std::vector<rrr::i64> &proposed_timestamps,
    const std::vector<rrr::i64> &watermarks, rrr::i32 *status,
    rrr::DeferredReply *defer) {

  Log_debug("DeadlineBatchPropose: %zu proposals, %zu watermarks from shard %d",
            tids.size(), watermarks.size(), src_shard);

  auto *scheduler = server_->GetScheduler();
  if (scheduler != nullptr) {
    // Process watermarks (piggybacked, replaces separate WatermarkExchange RPC)
    if (!watermarks.empty()) {
      scheduler->HandleWatermarkExchange(src_shard, watermarks);
    }

    // Process each proposal individually
    for (size_t i = 0; i < tids.size(); i++) {
      scheduler->HandleRemoteDeadlineProposal(tids[i], src_shard,
                                              proposed_timestamps[i], 1);
    }
  }

  *status = luigi::kOk;
  defer->reply();
}

void LuigiServiceImpl::DeadlineBatchConfirm(
    const std::vector<rrr::i64> &tids, const rrr::i32 &src_shard,
    const std::vector<rrr::i64> &agreed_timestamps, rrr::i32 *status,
    rrr::DeferredReply *defer) {

  Log_debug("DeadlineBatchConfirm: %zu confirmations from shard %d",
            tids.size(), src_shard);

  // Process each confirmation individually
  auto *scheduler = server_->GetScheduler();
  if (scheduler != nullptr) {
    for (size_t i = 0; i < tids.size(); i++) {
      scheduler->HandleRemoteDeadlineConfirm(tids[i], src_shard,
                                             agreed_timestamps[i]);
    }
  }

  *status = luigi::kOk;
  defer->reply();
}

//=============================================================================
// PHASE 4: REPLICATE RPC HANDLER (follower-side)
//=============================================================================

void LuigiServiceImpl::Replicate(const rrr::i32 &worker_id,
                                 const rrr::i64 &slot_id,
                                 const rrr::i64 &txn_id,
                                 const rrr::i64 &timestamp,
                                 const std::string &log_data, rrr::i32 *status,
                                 rrr::DeferredReply *defer) {
  Log_debug("Replicate: worker=%d slot=%ld txn=%ld ts=%ld", worker_id, slot_id,
            txn_id, timestamp);

  // Append to follower's per-worker log
  auto *scheduler = server_->GetScheduler();
  if (scheduler != nullptr) {
    scheduler->AppendToLog(worker_id, slot_id, txn_id, timestamp, log_data);
  }

  // Ack immediately (Raft happy path)
  *status = luigi::kOk;
  defer->reply();
}

void LuigiServiceImpl::BatchReplicate(
    const rrr::i32 &worker_id, const rrr::i64 &prev_committed_slot,
    const std::vector<rrr::i64> &slot_ids, const std::vector<rrr::i64> &txn_ids,
    const std::vector<rrr::i64> &timestamps,
    const std::vector<std::string> &log_entries, rrr::i32 *status,
    rrr::i64 *last_appended_slot, rrr::DeferredReply *defer) {

  Log_debug("BatchReplicate: worker=%d entries=%zu prev_committed=%ld",
            worker_id, slot_ids.size(), prev_committed_slot);

  // Convert to uint64_t vectors
  std::vector<uint64_t> u_slot_ids(slot_ids.begin(), slot_ids.end());
  std::vector<uint64_t> u_txn_ids(txn_ids.begin(), txn_ids.end());
  std::vector<uint64_t> u_timestamps(timestamps.begin(), timestamps.end());

  // Batch append to follower's per-worker log
  auto *scheduler = server_->GetScheduler();
  uint64_t last_slot = 0;
  if (scheduler != nullptr && !slot_ids.empty()) {
    last_slot = scheduler->BatchAppendToLog(worker_id, u_slot_ids, u_txn_ids,
                                            u_timestamps, log_entries);
  }

  *status = luigi::kOk;
  *last_appended_slot = static_cast<rrr::i64>(last_slot);
  defer->reply();
}

} // namespace janus
