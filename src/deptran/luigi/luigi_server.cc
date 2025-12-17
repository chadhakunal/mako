/**
 * LuigiServer: Standalone server implementation for Luigi protocol.
 */

#include "luigi_server.h"
#include "deptran/__dep__.h"
#include "deptran/rcc/tx.h"
#include "deptran/s_main.h"
#include "luigi_common.h"
#include "luigi_owd.h"
#include "luigi_scheduler.h"
#include "luigi_state_machine.h"

#include "benchmarks/benchmark_config.h"
#include "benchmarks/common.h"
#include "lib/common.h"
#include "lib/fasttransport.h"
#include "lib/helper_queue.h"
#include "lib/transport_request_handle.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

namespace janus {

//=============================================================================
// LuigiReceiver Implementation
//=============================================================================

LuigiReceiver::LuigiReceiver(const std::string &config_file)
    : config_(config_file) {}

LuigiReceiver::~LuigiReceiver() { StopScheduler(); }

//=============================================================================
// TransportReceiver Interface
//=============================================================================

size_t LuigiReceiver::ReceiveRequest(uint8_t reqType, char *reqBuf,
                                     char *respBuf) {
  size_t respLen = 0;

  switch (reqType) {
  case luigi::kLuigiDispatchReqType:
    HandleDispatch(reqBuf, respBuf, respLen);
    break;
  case luigi::kLuigiStatusReqType:
    HandleStatusCheck(reqBuf, respBuf, respLen);
    break;
  case luigi::kOwdPingReqType:
    HandleOwdPing(reqBuf, respBuf, respLen);
    break;
  case luigi::kDeadlineProposeReqType:
    HandleDeadlinePropose(reqBuf, respBuf, respLen);
    break;
  case luigi::kDeadlineConfirmReqType:
    HandleDeadlineConfirm(reqBuf, respBuf, respLen);
    break;
  case luigi::kWatermarkExchangeReqType:
    HandleWatermarkExchange(reqBuf, respBuf, respLen);
    break;
  default:
    Log_warn("LuigiReceiver: Unrecognized request type: %d", reqType);
    break;
  }

  return respLen;
}

//=============================================================================
// Luigi Scheduler Management
//=============================================================================

void LuigiReceiver::InitScheduler(uint32_t shard_id) {
  if (scheduler_ != nullptr) {
    return; // Already initialized
  }

  scheduler_ = new SchedulerLuigi();
  scheduler_->SetPartitionId(shard_id);

  // Set worker count based on warehouses (default 1)
  uint32_t worker_count = (config_.warehouses > 0) ? config_.warehouses : 1;
  scheduler_->SetWorkerCount(worker_count);

  // Set scheduler reference in executor
  // This is required for Replicate() to work
  // Note: executor_ is a private member, so we need a method in SchedulerLuigi
  // Actually, SchedulerLuigi creates its own executor, so we can assume it sets
  // itself up? Let's check SchedulerLuigi constructor/init.

  // Luigi uses state machine mode - no need for read/write callbacks
  // The state machine will be set via SetStateMachine() in LuigiServer::Run()

  // Set up replication callback
  scheduler_->SetReplicationCallback(
      [this](const std::shared_ptr<LuigiLogEntry> &entry) -> bool {
        return ReplicateEntry(entry);
      });

  // Transport and RPC setup handled externally (like Mako)
  Log_info("Luigi scheduler initialized for shard %d with %d workers", shard_id,
           worker_count);
}

void LuigiReceiver::StopScheduler() {
  // Transport cleanup handled externally

  if (scheduler_ != nullptr) {
    scheduler_->Stop();
    delete scheduler_;
    scheduler_ = nullptr;
    Log_info("Luigi scheduler stopped for shard %d",
             scheduler_->GetPartitionId());
  }
}

//=============================================================================
// Request Handlers
//=============================================================================

void LuigiReceiver::HandleDispatch(char *reqBuf, char *respBuf,
                                   size_t &respLen) {
  auto *req = reinterpret_cast<luigi::DispatchRequest *>(reqBuf);

  // Parse operations from request
  std::vector<LuigiOp> ops;
  char *data_ptr = req->ops_data;

  for (uint16_t i = 0; i < req->num_ops; i++) {
    LuigiOp op;

    // Read table_id (2 bytes)
    op.table_id = *reinterpret_cast<uint16_t *>(data_ptr);
    data_ptr += sizeof(uint16_t);

    // Read op_type (1 byte)
    op.op_type = *reinterpret_cast<uint8_t *>(data_ptr);
    data_ptr += sizeof(uint8_t);

    // Read key length (2 bytes)
    uint16_t klen = *reinterpret_cast<uint16_t *>(data_ptr);
    data_ptr += sizeof(uint16_t);

    // Read value length (2 bytes)
    uint16_t vlen = *reinterpret_cast<uint16_t *>(data_ptr);
    data_ptr += sizeof(uint16_t);

    // Read key
    op.key.assign(data_ptr, klen);
    data_ptr += klen;

    // Read value (for writes)
    if (vlen > 0) {
      op.value.assign(data_ptr, vlen);
      data_ptr += vlen;
    }

    ops.push_back(op);
  }

  // Extract involved shards
  std::vector<uint32_t> involved_shards;
  for (uint16_t i = 0; i < req->num_involved_shards && i < luigi::kMaxShards;
       i++) {
    involved_shards.push_back(req->involved_shards[i]);
  }

  // Prepare response
  auto *resp = reinterpret_cast<luigi::DispatchResponse *>(respBuf);
  resp->req_nr = req->req_nr;
  resp->txn_id = req->txn_id;
  respLen = sizeof(luigi::DispatchResponse);

  if (scheduler_ == nullptr) {
    Log_warn("Luigi scheduler not initialized, rejecting request");
    resp->status = luigi::kAbort;
    resp->commit_timestamp = 0;
    resp->num_results = 0;
    return;
  }

  uint64_t txn_id = req->txn_id;

  // Dispatch to scheduler with async callback
  scheduler_->LuigiDispatchFromRequest(
      txn_id, req->expected_time, ops, involved_shards,
      [this, txn_id](int status, uint64_t commit_ts,
                     const std::vector<std::string> &read_results) {
        StoreResult(txn_id, status, commit_ts, read_results);
      });

  // Return QUEUED immediately
  resp->status = luigi::kStatusQueued;
  resp->commit_timestamp = 0;
  resp->num_results = 0;

  Log_debug("Luigi dispatch queued txn %lu, expected_time %lu", txn_id,
            req->expected_time);
}

void LuigiReceiver::HandleStatusCheck(char *reqBuf, char *respBuf,
                                      size_t &respLen) {
  auto *req = reinterpret_cast<luigi::StatusRequest *>(reqBuf);
  auto *resp = reinterpret_cast<luigi::StatusResponse *>(respBuf);

  resp->req_nr = req->req_nr;
  resp->txn_id = req->txn_id;
  respLen = sizeof(luigi::StatusResponse);

  // Look up result
  {
    std::shared_lock<std::shared_mutex> lock(results_mutex_);
    auto it = completed_txns_.find(req->txn_id);

    if (it == completed_txns_.end()) {
      // Check if still pending
      if (scheduler_ != nullptr && scheduler_->HasPendingTxn(req->txn_id)) {
        resp->status = luigi::kStatusQueued;
      } else {
        resp->status = luigi::kStatusNotFound;
      }
      resp->commit_timestamp = 0;
      resp->num_results = 0;
      return;
    }

    // Found completed result
    const auto &result = it->second;
    resp->status = result.status;
    resp->commit_timestamp = result.commit_timestamp;
    resp->num_results = result.read_results.size();

    // Copy read results
    char *results_ptr = resp->results_data;
    for (const auto &val : result.read_results) {
      uint16_t vlen = val.size();
      memcpy(results_ptr, &vlen, sizeof(uint16_t));
      results_ptr += sizeof(uint16_t);
      memcpy(results_ptr, val.data(), vlen);
      results_ptr += vlen;
    }
  }

  // Remove result after retrieval
  if (resp->status == luigi::kStatusComplete ||
      resp->status == luigi::kStatusAborted) {
    std::unique_lock<std::shared_mutex> lock(results_mutex_);
    completed_txns_.erase(req->txn_id);
  }

  Log_debug("Luigi status check txn %lu: status=%d", req->txn_id, resp->status);
}

void LuigiReceiver::HandleOwdPing(char *reqBuf, char *respBuf,
                                  size_t &respLen) {
  auto *req = reinterpret_cast<luigi::OwdPingRequest *>(reqBuf);
  auto *resp = reinterpret_cast<luigi::OwdPingResponse *>(respBuf);

  resp->req_nr = req->req_nr;
  resp->status = 0; // OK
  respLen = sizeof(luigi::OwdPingResponse);

  Log_debug("OWD ping received, req_nr=%d, send_time=%lu", req->req_nr,
            req->send_time);
}

//=============================================================================
// Result Storage
//=============================================================================

void LuigiReceiver::StoreResult(uint64_t txn_id, int status, uint64_t commit_ts,
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

  // Periodic cleanup
  static int cleanup_counter = 0;
  if (++cleanup_counter >= 100) {
    cleanup_counter = 0;
    lock.unlock();
    CleanupStaleResults();
  }
}

void LuigiReceiver::CleanupStaleResults(int ttl_seconds) {
  std::unique_lock<std::shared_mutex> lock(results_mutex_);

  auto now = std::chrono::steady_clock::now();
  auto ttl = std::chrono::seconds(ttl_seconds);

  for (auto it = completed_txns_.begin(); it != completed_txns_.end();) {
    if (now - it->second.completion_time > ttl) {
      it = completed_txns_.erase(it);
    } else {
      ++it;
    }
  }
}

bool LuigiReceiver::ReplicateEntry(
    const std::shared_ptr<LuigiLogEntry> &entry) {
  // Get Paxos configuration
  auto &benchConfig = BenchmarkConfig::getInstance();

  if (!benchConfig.getIsReplicated()) {
    // No replication needed, just return success
    return true;
  }

  // Serialize Luigi entry to log buffer
  // Format: [tid (8 bytes)][timestamp (8 bytes)][num_ops (4 bytes)][ops
  // data...]
  size_t log_size = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

  // Calculate ops data size
  for (const auto &op : entry->ops_) {
    log_size += sizeof(uint16_t); // table_id
    log_size += sizeof(uint8_t);  // op_type
    log_size += sizeof(uint16_t); // key length
    log_size += op.key.size();
    log_size += sizeof(uint16_t); // value length
    log_size += op.value.size();
  }

  std::vector<unsigned char> log_buffer(log_size);
  unsigned char *ptr = log_buffer.data();

  // Write tid
  *reinterpret_cast<uint64_t *>(ptr) = entry->tid_;
  ptr += sizeof(uint64_t);

  // Write timestamp
  *reinterpret_cast<uint64_t *>(ptr) = entry->agreed_ts_;
  ptr += sizeof(uint64_t);

  // Write num_ops
  *reinterpret_cast<uint32_t *>(ptr) = entry->ops_.size();
  ptr += sizeof(uint32_t);

  // Write ops
  for (const auto &op : entry->ops_) {
    *reinterpret_cast<uint16_t *>(ptr) = op.table_id;
    ptr += sizeof(uint16_t);

    *reinterpret_cast<uint8_t *>(ptr) = op.op_type;
    ptr += sizeof(uint8_t);

    uint16_t klen = op.key.size();
    *reinterpret_cast<uint16_t *>(ptr) = klen;
    ptr += sizeof(uint16_t);
    memcpy(ptr, op.key.data(), klen);
    ptr += klen;

    uint16_t vlen = op.value.size();
    *reinterpret_cast<uint16_t *>(ptr) = vlen;
    ptr += sizeof(uint16_t);
    if (vlen > 0) {
      memcpy(ptr, op.value.data(), vlen);
      ptr += vlen;
    }
  }

  // Propose log to Paxos using add_log_to_nc
  // Use scheduler's partition_id as the partition identifier
  uint32_t partition_id = scheduler_ ? scheduler_->GetPartitionId() : 0;

  // add_log_to_nc sends the log to Paxos for replication
  // Parameters: log buffer, size, partition_id, batch_size
  int batch_size = 100; // Default batch size for Luigi
  add_log_to_nc(reinterpret_cast<char *>(log_buffer.data()), log_buffer.size(),
                partition_id, batch_size);

  Log_debug("Luigi entry replicated for txn %lu via Paxos", entry->tid_);
  return true;
}

//=============================================================================
// LuigiServer Implementation
//=============================================================================

LuigiServer::LuigiServer(int shard_idx, const std::string &benchmark_type)
    : shard_idx_(shard_idx), benchmark_type_(benchmark_type) {
  // Get config from BenchmarkConfig singleton
  auto &cfg = BenchmarkConfig::getInstance();
  config_ = cfg.getConfig();

  receiver_ = new LuigiReceiver(config_->configFile);
}

LuigiServer::~LuigiServer() {
  if (receiver_) {
    receiver_->StopScheduler();
    delete receiver_;
    receiver_ = nullptr;
  }
}

void LuigiServer::Run() {
  auto &cfg = BenchmarkConfig::getInstance();

  std::cout << "\n=== Luigi Server Initialization ===\n";

  // 1. Initialize Luigi OWD service
  std::cout << "Initializing Luigi OWD service...\n";
  auto &owd = mako::luigi::LuigiOWD::getInstance();
  owd.init(config_->configFile, cfg.getCluster(), shard_idx_, config_->nshards);
  owd.start();

  // 2. Create state machine based on benchmark_type
  std::cout << "Creating " << benchmark_type_ << " state machine...\n";
  std::shared_ptr<LuigiStateMachine> state_machine;

  if (benchmark_type_ == "tpcc") {
    state_machine =
        std::make_shared<LuigiTPCCStateMachine>(shard_idx_,       // shard_id
                                                0,                // replica_id
                                                config_->nshards, // shard_num
                                                1                 // replica_num
        );
  } else if (benchmark_type_ == "micro") {
    state_machine = std::make_shared<LuigiMicroStateMachine>(
        shard_idx_, 0, config_->nshards, 1);
  } else {
    Log_error("Unknown benchmark type: %s", benchmark_type_.c_str());
    owd.stop();
    return;
  }

  // Initialize state machine tables
  state_machine->InitializeTables();

  // 3. Initialize scheduler (partition_id = shard_idx for Luigi)
  std::cout << "Initializing scheduler...\n";
  receiver_->InitScheduler(shard_idx_);

  auto *scheduler = receiver_->GetScheduler();
  if (!scheduler) {
    Log_error("Failed to create scheduler");
    owd.stop();
    return;
  }

  // Wire scheduler with state machine
  scheduler->SetStateMachine(state_machine);
  scheduler->EnableStateMachineMode(true);
  scheduler->SetWorkerCount(cfg.getNthreads());

  // 4. RPC connections are handled by eRPC/FastTransport infrastructure
  // Luigi uses the same transport as Mako for all coordination RPCs
  // (deadline agreement, watermark exchange, etc.)
  // No additional setup needed here - transport is initialized externally

  std::cout << "\n=== Luigi Server Ready ===\n";
  std::cout << "Shard:      " << shard_idx_ << "/" << config_->nshards << "\n";
  std::cout << "Benchmark:  " << benchmark_type_ << "\n";
  std::cout << "Workers:    " << cfg.getNthreads() << "\n";
  std::cout << "Listening for requests...\n\n";

  // 5. Event loop
  // For now, simple sleep loop. In production, this would integrate with
  // the eRPC transport event loop (rpc_server->run_event_loop_timeout())
  // or be driven by external request dispatch (like Mako's HelperQueue)
  volatile bool running = true;
  while (running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // Cleanup
  std::cout << "\nShutting down Luigi server...\n";
  receiver_->StopScheduler();
  owd.stop();

  Log_info("LuigiServer::Run() exiting for shard %d", shard_idx_);
}

//=============================================================================
// Global Luigi Server Management
//=============================================================================

namespace {
std::mutex g_luigi_servers_mu;
std::vector<LuigiServer *> g_luigi_servers;
} // namespace

void RegisterLuigiServer(LuigiServer *server) {
  std::lock_guard<std::mutex> lock(g_luigi_servers_mu);
  g_luigi_servers.push_back(server);
}

void UnregisterLuigiServer(LuigiServer *server) {
  std::lock_guard<std::mutex> lock(g_luigi_servers_mu);
  auto it = std::find(g_luigi_servers.begin(), g_luigi_servers.end(), server);
  if (it != g_luigi_servers.end()) {
    g_luigi_servers.erase(it);
  }
}

SchedulerLuigi *GetLocalLuigiScheduler() {
  std::lock_guard<std::mutex> lock(g_luigi_servers_mu);
  if (!g_luigi_servers.empty()) {
    return g_luigi_servers[0]->GetScheduler();
  }
  return nullptr;
}

//=============================================================================
// Coordination RPC Handlers (Leader-to-Leader)
//=============================================================================

void LuigiReceiver::HandleDeadlinePropose(char *reqBuf, char *respBuf,
                                          size_t &respLen) {
  auto *req = reinterpret_cast<luigi::DeadlineProposeRequest *>(reqBuf);
  auto *resp = reinterpret_cast<luigi::DeadlineProposeResponse *>(respBuf);

  Log_debug("HandleDeadlinePropose: tid=%lu, ts=%lu, src=%u, phase=%u",
            req->tid, req->proposed_ts, req->src_shard, req->phase);

  // Forward to scheduler
  uint64_t my_ts = 0;
  if (scheduler_) {
    my_ts = scheduler_->HandleRemoteDeadlineProposal(
        req->tid, req->src_shard, req->proposed_ts, req->phase);
  }

  // Build response
  resp->req_nr = req->req_nr;
  resp->tid = req->tid;
  resp->proposed_ts = my_ts;
  resp->shard_id = scheduler_ ? scheduler_->GetPartitionId() : 0;
  resp->status = 0;

  respLen = sizeof(luigi::DeadlineProposeResponse);
}

void LuigiReceiver::HandleDeadlineConfirm(char *reqBuf, char *respBuf,
                                          size_t &respLen) {
  auto *req = reinterpret_cast<luigi::DeadlineConfirmRequest *>(reqBuf);
  auto *resp = reinterpret_cast<luigi::DeadlineConfirmResponse *>(respBuf);

  Log_debug("HandleDeadlineConfirm: tid=%lu, new_ts=%lu, src=%u", req->tid,
            req->new_ts, req->src_shard);

  // Forward to scheduler
  bool success = false;
  if (scheduler_) {
    success = scheduler_->HandleRemoteDeadlineConfirm(req->tid, req->src_shard,
                                                      req->new_ts);
  }

  // Build response
  resp->req_nr = req->req_nr;
  resp->status = success ? 0 : -1;

  respLen = sizeof(luigi::DeadlineConfirmResponse);
}

void LuigiReceiver::HandleWatermarkExchange(char *reqBuf, char *respBuf,
                                            size_t &respLen) {
  auto *req = reinterpret_cast<luigi::WatermarkExchangeRequest *>(reqBuf);
  auto *resp = reinterpret_cast<luigi::WatermarkExchangeResponse *>(respBuf);

  Log_debug("HandleWatermarkExchange: src=%u, num_wm=%u", req->src_shard,
            req->num_watermarks);

  // Convert to vector and forward to scheduler
  if (scheduler_) {
    std::vector<int64_t> watermarks;
    for (uint16_t i = 0; i < req->num_watermarks; i++) {
      watermarks.push_back(static_cast<int64_t>(req->watermarks[i]));
    }
    scheduler_->HandleWatermarkExchange(req->src_shard, watermarks);
  }

  // Build response
  resp->req_nr = req->req_nr;
  resp->status = 0;

  respLen = sizeof(luigi::WatermarkExchangeResponse);
}

} // namespace janus
