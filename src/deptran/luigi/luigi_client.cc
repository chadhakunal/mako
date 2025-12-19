/**
 * LuigiClient: Standalone client implementation for Luigi protocol.
 */

#include "luigi_client.h"

#include <chrono>
#include <memory>
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
    : request_(other.request_), msg_len_(other.msg_len_), ws_len_(other.ws_len_) {
  other.request_ = nullptr;
  other.msg_len_ = 0;
  other.ws_len_ = 0;
}

LuigiDispatchBuilder &
LuigiDispatchBuilder::operator=(LuigiDispatchBuilder &&other) noexcept {
  if (this != &other) {
    delete request_;
    request_ = other.request_;
    msg_len_ = other.msg_len_;
    ws_len_ = other.ws_len_;
    other.request_ = nullptr;
    other.msg_len_ = 0;
    other.ws_len_ = 0;
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

LuigiDispatchBuilder &LuigiDispatchBuilder::SetTxnType(uint32_t txn_type) {
  request_->txn_type = txn_type;
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

LuigiDispatchBuilder &LuigiDispatchBuilder::AddWorkingSetEntry(int32_t var_id, const std::string &value) {
  if (request_->num_working_set >= luigi::kMaxWorkingSetEntries) {
    Log_warn("LuigiDispatchBuilder: Max working_set entries reached, ignoring");
    return *this;
  }

  // CRITICAL: Write to the actual serialization location, not request_->working_set_data!
  // working_set is serialized immediately after ops_data in the message buffer.
  // Location: request start + ops_data offset + actual ops length + current ws length
  size_t header_size = offsetof(luigi::DispatchRequest, ops_data);
  char *base = reinterpret_cast<char*>(request_);
  char *ptr = base + header_size + msg_len_ + ws_len_;

  // var_id (4 bytes)
  *reinterpret_cast<int32_t *>(ptr) = var_id;
  ptr += sizeof(int32_t);

  // vlen (2 bytes)
  uint16_t vlen = static_cast<uint16_t>(value.size());
  *reinterpret_cast<uint16_t *>(ptr) = vlen;
  ptr += sizeof(uint16_t);

  // value
  memcpy(ptr, value.data(), vlen);
  ptr += vlen;

  ws_len_ = ptr - (base + header_size + msg_len_);
  request_->num_working_set++;

  return *this;
}

LuigiDispatchBuilder &LuigiDispatchBuilder::SetWorkingSet(const std::map<int32_t, std::string> &working_set) {
  Log_info("LuigiDispatchBuilder: SetWorkingSet called with %zu entries", working_set.size());
  int i = 0;
  for (const auto &[var_id, value] : working_set) {
    if (i < 5 || i == (int)working_set.size() - 1) {  // Log first 5 and last
      Log_info("  [CLIENT] entry[%d]: var_id=%d, value='%s' (len=%zu)",
               i, var_id, value.c_str(), value.size());
    }
    AddWorkingSetEntry(var_id, value);
    i++;
  }
  Log_info("LuigiDispatchBuilder: Serialized %u working_set entries, ws_len=%zu (first 16 bytes: %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x)",
           request_->num_working_set, ws_len_,
           (unsigned char)request_->working_set_data[0], (unsigned char)request_->working_set_data[1],
           (unsigned char)request_->working_set_data[2], (unsigned char)request_->working_set_data[3],
           (unsigned char)request_->working_set_data[4], (unsigned char)request_->working_set_data[5],
           (unsigned char)request_->working_set_data[6], (unsigned char)request_->working_set_data[7],
           (unsigned char)request_->working_set_data[8], (unsigned char)request_->working_set_data[9],
           (unsigned char)request_->working_set_data[10], (unsigned char)request_->working_set_data[11],
           (unsigned char)request_->working_set_data[12], (unsigned char)request_->working_set_data[13],
           (unsigned char)request_->working_set_data[14], (unsigned char)request_->working_set_data[15]);
  return *this;
}

size_t LuigiDispatchBuilder::GetTotalSize() const {
  return sizeof(luigi::DispatchRequest) - sizeof(request_->ops_data) - sizeof(request_->working_set_data) + msg_len_ + ws_len_;
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
  Log_info("[LUIGI-CLIENT] ReceiveResponse called: reqType=%d", reqType);
  switch (reqType) {
  case luigi::kLuigiDispatchReqType:
    Log_info("[LUIGI-CLIENT] Routing to HandleDispatchReply");
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

  if (txn_nr <= 10 || txn_nr % 50 == 0) {
    Log_info("[LUIGI-CLIENT] TXN-%lu: InvokeDispatch for %zu shards", 
             txn_nr, requests_per_shard.size());
  }

  uint32_t req_id = ++last_req_id_;
  req_id *= 10;

  // Use sender's partition ID (0 for benchmark client)
  // This is used for RPC client connection tracking, NOT routing
  uint16_t server_id = 0;  // Benchmark client acts as partition 0

  // NOTE: We store pending request AFTER building data_to_send so we know the
  // actual number of remote shards that will send RPC responses.
  // This avoids a bug where num_responses_pending includes local shard but
  // only remote shards send RPCs, causing callbacks to never fire.

  // Build data to send per shard
  // NOTE: Local shard handled directly via local_receiver_, remote via RPC
  std::map<int, std::pair<char *, size_t>> data_to_send;
  int local_shard = BenchmarkConfig::getInstance().getShardIndex();
  LuigiDispatchBuilder* local_builder = nullptr;

  for (auto &kv : requests_per_shard) {
    int shard_idx = kv.first;
    LuigiDispatchBuilder *builder = kv.second;

    // Set request number
    builder->SetReqNr(req_id);

    // Handle local shard directly
    if (shard_idx == local_shard) {
      local_builder = builder;  // Save for direct handling
      if (txn_nr <= 10 || txn_nr % 50 == 0) {
        Log_info("[LUIGI-CLIENT] TXN-%lu: Will handle local shard %d directly",
                 txn_nr, shard_idx);
      }
      continue;
    }

    char* req_buf = reinterpret_cast<char *>(builder->GetRequest());
    size_t req_size = builder->GetTotalSize();
    data_to_send[shard_idx] = {req_buf, req_size};

    if (txn_nr <= 10 || txn_nr % 50 == 0) {
      Log_info("[LUIGI-CLIENT] TXN-%lu: Remote shard %d - req_buf=%p, size=%zu, bytes 66-81: %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x",
               txn_nr, shard_idx, (void*)req_buf, req_size,
               (unsigned char)req_buf[66], (unsigned char)req_buf[67],
               (unsigned char)req_buf[68], (unsigned char)req_buf[69],
               (unsigned char)req_buf[70], (unsigned char)req_buf[71],
               (unsigned char)req_buf[72], (unsigned char)req_buf[73],
               (unsigned char)req_buf[74], (unsigned char)req_buf[75],
               (unsigned char)req_buf[76], (unsigned char)req_buf[77],
               (unsigned char)req_buf[78], (unsigned char)req_buf[79],
               (unsigned char)req_buf[80], (unsigned char)req_buf[81]);
    }
  }

  // Handle local shard request directly (if any)
  if (local_builder && local_receiver_) {
    auto* req = local_builder->GetRequest();
    if (txn_nr <= 10 || txn_nr % 50 == 0) {
      Log_info("[LUIGI-CLIENT] TXN-%lu: Local shard - before call: num_working_set=%u, first 16 bytes: %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x",
               txn_nr, req->num_working_set,
               (unsigned char)req->working_set_data[0], (unsigned char)req->working_set_data[1],
               (unsigned char)req->working_set_data[2], (unsigned char)req->working_set_data[3],
               (unsigned char)req->working_set_data[4], (unsigned char)req->working_set_data[5],
               (unsigned char)req->working_set_data[6], (unsigned char)req->working_set_data[7],
               (unsigned char)req->working_set_data[8], (unsigned char)req->working_set_data[9],
               (unsigned char)req->working_set_data[10], (unsigned char)req->working_set_data[11],
               (unsigned char)req->working_set_data[12], (unsigned char)req->working_set_data[13],
               (unsigned char)req->working_set_data[14], (unsigned char)req->working_set_data[15]);
    }

    // CRITICAL: DispatchResponse is ~16KB due to results_data[16384]
    // Must allocate on heap to avoid stack smashing
    auto resp_buf = std::make_unique<char[]>(sizeof(luigi::DispatchResponse));
    local_receiver_->ReceiveRequest(
        luigi::kLuigiDispatchReqType,
        reinterpret_cast<char*>(req),
        resp_buf.get());
    if (txn_nr <= 10 || txn_nr % 50 == 0) {
      // Log_info("[LUIGI-CLIENT] TXN-%lu: Local shard handled directly", txn_nr);
    }
  }

  blocked_ = true;
  num_response_waiting_ = data_to_send.size();

  // NOW store pending request with correct response count (only remote shards)
  // This must happen AFTER building data_to_send so we know actual RPC count
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    PendingRequest pr;
    pr.name = "luigiDispatch";
    pr.req_nr = req_id;
    pr.txn_nr = txn_nr;
    pr.server_id = server_id;
    pr.response_cb = continuation;
    pr.error_cb = error_continuation;
    pr.num_responses_pending = static_cast<int>(data_to_send.size());  // Only remote shards!
    pending_requests_[req_id] = pr;
  }

  if (data_to_send.empty()) {
    // All local - no RPCs needed, just return success immediately
    if (txn_nr <= 10 || txn_nr % 50 == 0) {
      // Log_info("[LUIGI-CLIENT] TXN-%lu: All local (handled), completing", txn_nr);
    }
    blocked_ = false;
    num_response_waiting_ = 0;
    if (continuation) {
      continuation(nullptr);
    }
    return;
  }

  // IMPORTANT: The 3rd parameter to SendBatchRequestToAll is used for:
  // 1. RPC client connection: Connects to remote_shard_base_port + server_id
  // 2. NOT for routing on the remote side (that uses target_server_id in payload)
  //
  // The server transport is created with ID = warehouses + 5 + alpha (from rpc_setup.cc:91)
  // For alpha=0: ID = 6 + 5 + 0 = 11
  // So we must use the SAME ID to connect to the remote shard's server!
  uint16_t connection_server_id = config_.warehouses + 5;  // Match server transport ID

  if (txn_nr <= 10 || txn_nr % 50 == 0) {
    Log_info("[LUIGI-CLIENT] TXN-%lu: Sending to %zu remote shards (conn_server_id=%u)",
             txn_nr, data_to_send.size(), connection_server_id);
  }

  // Send to all involved REMOTE shards using mako's transport
  // Note: Using mako's transport API directly
  rpc_transport_->SendBatchRequestToAll(
      this, // receiver - responses will be routed to HandleDispatchReply
      luigi::kLuigiDispatchReqType,
      connection_server_id,  // Use remote shard's partition ID for connection
      sizeof(luigi::DispatchResponse), data_to_send);
      
  if (txn_nr <= 10 || txn_nr % 50 == 0) {
    Log_info("[LUIGI-CLIENT] TXN-%lu: RPC sent to %zu remote shards, waiting for responses",
             txn_nr, data_to_send.size());
  }
}

void LuigiClient::HandleDispatchReply(char *respBuf) {
  auto *resp = reinterpret_cast<luigi::DispatchResponse *>(respBuf);

  if (resp->txn_id <= 10 || resp->txn_id % 50 == 0) {
    Log_info("[LUIGI-CLIENT] TXN-%lu: Received dispatch reply (status=%d, commit_ts=%lu, req_nr=%u)",
             resp->txn_id, resp->status, resp->commit_timestamp, resp->req_nr);
  }

  // Look up the pending request by req_nr
  PendingRequest pending;
  bool found = false;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_requests_.find(resp->req_nr);
    if (it != pending_requests_.end()) {
      pending = it->second;
      pending.num_responses_pending--;
      if (pending.num_responses_pending <= 0) {
        // All responses received, remove from map
        pending_requests_.erase(it);
      } else {
        // Update count
        it->second.num_responses_pending = pending.num_responses_pending;
      }
      found = true;
    }
  }

  if (!found) {
    Log_debug("[LUIGI-CLIENT] Received reply for unknown request; req_nr=%u", resp->req_nr);
    return;
  }

  // Invoke callback
  if (pending.response_cb) {
    pending.response_cb(respBuf);
  }

  if (resp->txn_id <= 10 || resp->txn_id % 50 == 0) {
    Log_info("[LUIGI-CLIENT] TXN-%lu: Callback invoked, remaining=%d", 
             resp->txn_id, pending.num_responses_pending);
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

  // target_server_id for OWD ping - use par_id 0 (first partition in any shard)
  req->target_server_id = 0;
  req->req_nr = req_id;
  req->send_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();

  blocked_ = true;
  num_response_waiting_ = 1;

  // Send OWD ping to partition 0 of the target shard
  this->rpc_transport_->SendRequestToShard(this, luigi::kOwdPingReqType,
                                           shard_idx, 0,  // server_id = 0 (first partition)
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
  // target_server_id must be a partition on the destination shard
  // Use first partition of target shard (contiguous partitioning)
  req.target_server_id = target_shard * config_.warehouses;  
  req.req_nr = req_id;
  req.tid = tid;
  req.proposed_ts = proposed_ts;
  req.src_shard = BenchmarkConfig::getInstance().getShardIndex();
  req.phase = phase;

  std::map<int, std::pair<char *, size_t>> data_to_send;
  data_to_send[target_shard] = {reinterpret_cast<char *>(&req), sizeof(req)};

  blocked_ = true;
  num_response_waiting_ = 1;

  uint16_t sender_rpc_id = config_.warehouses + 5;
  Log_info("[LUIGI-CLIENT] Sending DeadlinePropose: target_shard=%u, target_server_id=%u, sender_rpc_id=%u",
           target_shard, req.target_server_id, sender_rpc_id);
  
  rpc_transport_->SendBatchRequestToAll(
      this, luigi::kDeadlineProposeReqType,
      sender_rpc_id,
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
  // target_server_id must be a partition on the destination shard
  req.target_server_id = target_shard * config_.warehouses;
  req.req_nr = req_id;
  req.tid = tid;
  req.src_shard = BenchmarkConfig::getInstance().getShardIndex();
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
  // target_server_id must be a partition on the destination shard
  req.target_server_id = target_shard * config_.warehouses;
  req.req_nr = req_id;
  req.src_shard = BenchmarkConfig::getInstance().getShardIndex();
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
