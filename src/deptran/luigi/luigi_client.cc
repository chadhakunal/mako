/**
 * LuigiClient: Standalone client implementation for Luigi protocol.
 */

#include "luigi_client.h"

#include <chrono>
#include <random>

#include "benchmarks/sto/Interface.hh"
#include "rrr/base/logging.hpp"

namespace janus {

//=============================================================================
// LuigiDispatchBuilder Implementation
//=============================================================================

LuigiDispatchBuilder::LuigiDispatchBuilder() {
  request_ = new luigi::DispatchRequest();
  memset(request_, 0, sizeof(luigi::DispatchRequest));
}

LuigiDispatchBuilder::~LuigiDispatchBuilder() { delete request_; }

LuigiDispatchBuilder::LuigiDispatchBuilder(
    LuigiDispatchBuilder &&other) noexcept
    : request_(other.request_), msg_len_(other.msg_len_) {
  other.request_ = nullptr;
  other.msg_len_ = 0;
}

LuigiDispatchBuilder &
LuigiDispatchBuilder::operator=(LuigiDispatchBuilder &&other) noexcept {
  if (this != &other) {
    delete request_;
    request_ = other.request_;
    msg_len_ = other.msg_len_;
    other.request_ = nullptr;
    other.msg_len_ = 0;
  }
  return *this;
}

LuigiDispatchBuilder &LuigiDispatchBuilder::SetTxnId(uint64_t txn_id) {
  request_->txn_id = txn_id;
  return *this;
}

LuigiDispatchBuilder &
LuigiDispatchBuilder::SetExpectedTime(uint64_t expected_time) {
  request_->expected_time = expected_time;
  return *this;
}

LuigiDispatchBuilder &
LuigiDispatchBuilder::SetTargetServer(uint16_t server_id) {
  request_->target_server_id = server_id;
  return *this;
}

LuigiDispatchBuilder &LuigiDispatchBuilder::SetReqNr(uint32_t req_nr) {
  request_->req_nr = req_nr;
  return *this;
}

LuigiDispatchBuilder &
LuigiDispatchBuilder::SetInvolvedShards(const std::vector<uint32_t> &shards) {
  request_->num_involved_shards = std::min(shards.size(), luigi::kMaxShards);
  for (size_t i = 0; i < request_->num_involved_shards; i++) {
    request_->involved_shards[i] = static_cast<uint16_t>(shards[i]);
  }
  return *this;
}

LuigiDispatchBuilder &LuigiDispatchBuilder::AddRead(uint16_t table_id,
                                                    const std::string &key) {
  if (request_->num_ops >= luigi::kMaxOps) {
    Log_warn("LuigiDispatchBuilder: Max ops reached, ignoring read");
    return *this;
  }

  char *ptr = request_->ops_data + msg_len_;

  // table_id (2 bytes)
  *reinterpret_cast<uint16_t *>(ptr) = table_id;
  ptr += sizeof(uint16_t);

  // op_type (1 byte) - READ
  *ptr = luigi::kOpRead;
  ptr += sizeof(uint8_t);

  // klen (2 bytes)
  uint16_t klen = static_cast<uint16_t>(key.size());
  *reinterpret_cast<uint16_t *>(ptr) = klen;
  ptr += sizeof(uint16_t);

  // vlen (2 bytes) - 0 for reads
  *reinterpret_cast<uint16_t *>(ptr) = 0;
  ptr += sizeof(uint16_t);

  // key
  memcpy(ptr, key.data(), klen);
  ptr += klen;

  msg_len_ = ptr - request_->ops_data;
  request_->num_ops++;

  return *this;
}

LuigiDispatchBuilder &LuigiDispatchBuilder::AddWrite(uint16_t table_id,
                                                     const std::string &key,
                                                     const std::string &value) {
  if (request_->num_ops >= luigi::kMaxOps) {
    Log_warn("LuigiDispatchBuilder: Max ops reached, ignoring write");
    return *this;
  }

  char *ptr = request_->ops_data + msg_len_;

  // table_id (2 bytes)
  *reinterpret_cast<uint16_t *>(ptr) = table_id;
  ptr += sizeof(uint16_t);

  // op_type (1 byte) - WRITE
  *ptr = luigi::kOpWrite;
  ptr += sizeof(uint8_t);

  // klen (2 bytes)
  uint16_t klen = static_cast<uint16_t>(key.size());
  *reinterpret_cast<uint16_t *>(ptr) = klen;
  ptr += sizeof(uint16_t);

  // vlen (2 bytes)
  uint16_t vlen = static_cast<uint16_t>(value.size());
  *reinterpret_cast<uint16_t *>(ptr) = vlen;
  ptr += sizeof(uint16_t);

  // key
  memcpy(ptr, key.data(), klen);
  ptr += klen;

  // value
  memcpy(ptr, value.data(), vlen);
  ptr += vlen;

  msg_len_ = ptr - request_->ops_data;
  request_->num_ops++;

  return *this;
}

size_t LuigiDispatchBuilder::GetTotalSize() const {
  return sizeof(luigi::DispatchRequest) - sizeof(request_->ops_data) + msg_len_;
}

//=============================================================================
// LuigiClient Implementation
//=============================================================================

LuigiClient::LuigiClient(const std::string &config_file, Transport *transport,
                         uint64_t client_id)
    : config_(config_file), rpc_transport_(transport), client_id_(client_id) {
  // Generate random client ID if not provided
  while (client_id_ == 0) {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    client_id_ = dis(gen);
  }
}

void LuigiClient::ReceiveResponse(uint8_t reqType, char *respBuf) {
  switch (reqType) {
  case luigi::kLuigiDispatchReqType:
    HandleDispatchReply(respBuf);
    break;
  case luigi::kLuigiStatusReqType:
    HandleStatusReply(respBuf);
    break;
  case luigi::kOwdPingReqType:
    HandleOwdPingReply(respBuf);
    break;
  case luigi::kDeadlineProposeReqType:
    HandleDeadlineProposeReply(respBuf);
    break;
  case luigi::kDeadlineConfirmReqType:
    HandleDeadlineConfirmReply(respBuf);
    break;
  case luigi::kWatermarkExchangeReqType:
    HandleWatermarkExchangeReply(respBuf);
    break;
  default:
    Log_warn("LuigiClient: Unrecognized response type: %d", reqType);
    break;
  }
}

void LuigiClient::InvokeDispatch(
    uint64_t txn_nr, std::map<int, LuigiDispatchBuilder *> &requests_per_shard,
    ResponseCallback continuation, ErrorCallback error_continuation,
    uint32_t timeout) {

  Log_debug("InvokeDispatch: num_shards=%zu", requests_per_shard.size());

  uint32_t req_id = ++last_req_id_;
  req_id *= 10;

  // Get first shard's server_id for tracking
  uint16_t server_id = 0;
  if (!requests_per_shard.empty()) {
    server_id =
        requests_per_shard.begin()->second->GetRequest()->target_server_id;
  }

  current_request_ = {"luigiDispatch", req_id,       txn_nr,
                      server_id,       continuation, error_continuation};

  // Build data to send per shard
  std::map<int, std::pair<char *, size_t>> data_to_send;

  for (auto &kv : requests_per_shard) {
    int shard_idx = kv.first;
    LuigiDispatchBuilder *builder = kv.second;

    // Set request number
    builder->SetReqNr(req_id);

    data_to_send[shard_idx] = {reinterpret_cast<char *>(builder->GetRequest()),
                               builder->GetTotalSize()};
  }

  blocked_ = true;
  num_response_waiting_ = data_to_send.size();

  // Send to all involved shards using mako's transport
  // Note: Using mako's transport API directly
  rpc_transport_->SendBatchRequestToAll(
      nullptr, // receiver - not used for client sends
      luigi::kLuigiDispatchReqType,
      config_.warehouses + 5 + server_id % TThread::get_num_erpc_server(),
      sizeof(luigi::DispatchResponse), data_to_send);
}

void LuigiClient::HandleDispatchReply(char *respBuf) {
  auto *resp = reinterpret_cast<luigi::DispatchResponse *>(respBuf);

  Log_debug(
      "Luigi dispatch reply: req_nr=%d, txn_id=%lu, status=%d, commit_ts=%lu",
      resp->req_nr, resp->txn_id, resp->status, resp->commit_timestamp);

  if (resp->req_nr != current_request_.req_nr) {
    Log_debug("Received reply for wrong request; req_nr=%u, expected=%u",
              resp->req_nr, current_request_.req_nr);
    return;
  }

  // Invoke callback
  if (current_request_.response_cb) {
    current_request_.response_cb(respBuf);
  }

  // Track pending responses
  if (num_response_waiting_ > 0) {
    num_response_waiting_--;
  }
  if (num_response_waiting_ == 0) {
    blocked_ = false;
    current_request_.req_nr = 0;
  }
}

void LuigiClient::InvokeStatusCheck(uint64_t txn_nr, uint64_t txn_id,
                                    const std::vector<int> &shard_indices,
                                    ResponseCallback continuation,
                                    ErrorCallback error_continuation,
                                    uint32_t timeout) {

  Log_debug("InvokeStatusCheck: txn_id=%lu, num_shards=%zu", txn_id,
            shard_indices.size());

  uint32_t req_id = ++last_req_id_;
  req_id *= 10;

  current_request_ = {"luigiStatusCheck", req_id, txn_nr, 0, continuation,
                      error_continuation};

  // Build data to send per shard
  std::map<int, std::pair<char *, size_t>> data_to_send;
  std::vector<luigi::StatusRequest *> allocated_requests;

  for (int shard_idx : shard_indices) {
    auto *req = new luigi::StatusRequest();
    req->target_server_id = 0;
    req->req_nr = req_id;
    req->txn_id = txn_id;

    data_to_send[shard_idx] = {reinterpret_cast<char *>(req),
                               sizeof(luigi::StatusRequest)};
    allocated_requests.push_back(req);
  }

  blocked_ = true;
  num_response_waiting_ = data_to_send.size();

  this->rpc_transport_->SendBatchRequestToAll(
      this, luigi::kLuigiStatusReqType, config_.warehouses + 5,
      sizeof(luigi::StatusResponse), data_to_send);

  // Clean up allocated requests
  for (auto *req : allocated_requests) {
    delete req;
  }
}

void LuigiClient::HandleStatusReply(char *respBuf) {
  auto *resp = reinterpret_cast<luigi::StatusResponse *>(respBuf);

  Log_debug("Luigi status reply: req_nr=%d, txn_id=%lu, status=%d",
            resp->req_nr, resp->txn_id, resp->status);

  if (resp->req_nr != current_request_.req_nr) {
    Log_debug("Received reply for wrong request; req_nr=%u, expected=%u",
              resp->req_nr, current_request_.req_nr);
    return;
  }

  if (current_request_.response_cb) {
    current_request_.response_cb(respBuf);
  }

  if (num_response_waiting_ > 0) {
    num_response_waiting_--;
  }
  if (num_response_waiting_ == 0) {
    blocked_ = false;
    current_request_.req_nr = 0;
  }
}

void LuigiClient::InvokeOwdPing(uint64_t txn_nr, int shard_idx,
                                ResponseCallback continuation,
                                ErrorCallback error_continuation,
                                uint32_t timeout) {

  Log_debug("InvokeOwdPing: shard=%d", shard_idx);

  uint32_t req_id = ++last_req_id_;
  req_id *= 10;

  current_request_ = {"owdPing", req_id,       txn_nr,
                      0,         continuation, error_continuation};

  auto *req = reinterpret_cast<luigi::OwdPingRequest *>(
      this->rpc_transport_->GetRequestBuf(sizeof(luigi::OwdPingRequest),
                                          sizeof(luigi::OwdPingResponse)));

  req->target_server_id = 0;
  req->req_nr = req_id;
  req->send_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();

  blocked_ = true;
  num_response_waiting_ = 1;

  this->rpc_transport_->SendRequestToShard(this, luigi::kOwdPingReqType,
                                           shard_idx, config_.warehouses + 5,
                                           sizeof(luigi::OwdPingRequest));
}

void LuigiClient::HandleOwdPingReply(char *respBuf) {
  auto *resp = reinterpret_cast<luigi::OwdPingResponse *>(respBuf);

  Log_debug("OWD ping reply: req_nr=%d, status=%d", resp->req_nr, resp->status);

  if (resp->req_nr != current_request_.req_nr) {
    Log_debug("Received reply for wrong request; req_nr=%u, expected=%u",
              resp->req_nr, current_request_.req_nr);
    return;
  }

  if (current_request_.response_cb) {
    current_request_.response_cb(respBuf);
  }

  if (num_response_waiting_ > 0) {
    num_response_waiting_--;
  }
  if (num_response_waiting_ == 0) {
    blocked_ = false;
    current_request_.req_nr = 0;
  }
}

//=============================================================================
// Coordination RPCs (Leader-to-Leader)
//=============================================================================

void LuigiClient::InvokeDeadlinePropose(uint32_t target_shard, uint64_t tid,
                                         uint64_t proposed_ts, uint32_t phase,
                                         ResponseCallback continuation,
                                         ErrorCallback error_continuation) {
  Log_debug("InvokeDeadlinePropose: target=%u, tid=%lu, ts=%lu, phase=%u",
            target_shard, tid, proposed_ts, phase);

  uint32_t req_id = ++last_req_id_;
  current_request_ = {"deadlinePropose", req_id, 0, 0, continuation,
                      error_continuation};

  luigi::DeadlineProposeRequest req;
  req.target_server_id = target_shard;
  req.req_nr = req_id;
  req.tid = tid;
  req.proposed_ts = proposed_ts;
  req.src_shard = 0;
  req.phase = phase;

  std::map<int, std::pair<char *, size_t>> data_to_send;
  data_to_send[target_shard] = {reinterpret_cast<char *>(&req), sizeof(req)};

  blocked_ = true;
  num_response_waiting_ = 1;

  rpc_transport_->SendBatchRequestToAll(
      this, luigi::kDeadlineProposeReqType,
      config_.warehouses + 5,
      sizeof(luigi::DeadlineProposeResponse), data_to_send);
}

void LuigiClient::InvokeDeadlineConfirm(uint32_t target_shard, uint64_t tid,
                                         uint64_t new_ts,
                                         ResponseCallback continuation,
                                         ErrorCallback error_continuation) {
  Log_debug("InvokeDeadlineConfirm: target=%u, tid=%lu, new_ts=%lu",
            target_shard, tid, new_ts);

  uint32_t req_id = ++last_req_id_;
  current_request_ = {"deadlineConfirm", req_id, 0, 0, continuation,
                      error_continuation};

  luigi::DeadlineConfirmRequest req;
  req.target_server_id = target_shard;
  req.req_nr = req_id;
  req.tid = tid;
  req.src_shard = 0;
  req.new_ts = new_ts;

  std::map<int, std::pair<char *, size_t>> data_to_send;
  data_to_send[target_shard] = {reinterpret_cast<char *>(&req), sizeof(req)};

  blocked_ = true;
  num_response_waiting_ = 1;

  rpc_transport_->SendBatchRequestToAll(
      this, luigi::kDeadlineConfirmReqType,
      config_.warehouses + 5,
      sizeof(luigi::DeadlineConfirmResponse), data_to_send);
}

void LuigiClient::InvokeWatermarkExchange(
    uint32_t target_shard, const std::vector<uint64_t> &watermarks,
    ResponseCallback continuation, ErrorCallback error_continuation) {
  Log_debug("InvokeWatermarkExchange: target=%u, num_wm=%zu", target_shard,
            watermarks.size());

  uint32_t req_id = ++last_req_id_;
  current_request_ = {"watermarkExchange", req_id, 0, 0, continuation,
                      error_continuation};

  luigi::WatermarkExchangeRequest req;
  req.target_server_id = target_shard;
  req.req_nr = req_id;
  req.src_shard = 0;
  req.num_watermarks = std::min(watermarks.size(), size_t(32));

  for (size_t i = 0; i < req.num_watermarks; i++) {
    req.watermarks[i] = watermarks[i];
  }

  std::map<int, std::pair<char *, size_t>> data_to_send;
  data_to_send[target_shard] = {reinterpret_cast<char *>(&req), sizeof(req)};

  blocked_ = true;
  num_response_waiting_ = 1;

  rpc_transport_->SendBatchRequestToAll(
      this, luigi::kWatermarkExchangeReqType,
      config_.warehouses + 5,
      sizeof(luigi::WatermarkExchangeResponse), data_to_send);
}

void LuigiClient::HandleDeadlineProposeReply(char *respBuf) {
  auto *resp = reinterpret_cast<luigi::DeadlineProposeResponse *>(respBuf);

  Log_debug("DeadlinePropose reply: req_nr=%u, tid=%lu, ts=%lu, shard=%u",
            resp->req_nr, resp->tid, resp->proposed_ts, resp->shard_id);

  if (resp->req_nr != current_request_.req_nr) {
    return;
  }

  if (current_request_.response_cb) {
    current_request_.response_cb(respBuf);
  }

  if (num_response_waiting_ > 0) {
    num_response_waiting_--;
  }
  if (num_response_waiting_ == 0) {
    blocked_ = false;
    current_request_.req_nr = 0;
  }
}

void LuigiClient::HandleDeadlineConfirmReply(char *respBuf) {
  auto *resp = reinterpret_cast<luigi::DeadlineConfirmResponse *>(respBuf);

  Log_debug("DeadlineConfirm reply: req_nr=%u, status=%d", resp->req_nr,
            resp->status);

  if (resp->req_nr != current_request_.req_nr) {
    return;
  }

  if (current_request_.response_cb) {
    current_request_.response_cb(respBuf);
  }

  if (num_response_waiting_ > 0) {
    num_response_waiting_--;
  }
  if (num_response_waiting_ == 0) {
    blocked_ = false;
    current_request_.req_nr = 0;
  }
}

void LuigiClient::HandleWatermarkExchangeReply(char *respBuf) {
  auto *resp = reinterpret_cast<luigi::WatermarkExchangeResponse *>(respBuf);

  Log_debug("WatermarkExchange reply: req_nr=%u, status=%d", resp->req_nr,
            resp->status);

  if (resp->req_nr != current_request_.req_nr) {
    return;
  }

  if (current_request_.response_cb) {
    current_request_.response_cb(respBuf);
  }

  if (num_response_waiting_ > 0) {
    num_response_waiting_--;
  }
  if (num_response_waiting_ == 0) {
    blocked_ = false;
    current_request_.req_nr = 0;
  }
}

} // namespace janus
