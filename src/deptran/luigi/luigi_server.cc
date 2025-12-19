/**
 * LuigiServer: Standalone server implementation for Luigi protocol.
 */

#include "luigi_server.h"
#include "deptran/__dep__.h"
#include "deptran/rcc/tx.h"
#include "deptran/s_main.h"
#include "luigi_client.h"
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
  
  static std::atomic<int> request_count{0};
  int req_num = request_count.fetch_add(1);
  
  if (req_num % 50 == 0) {
    Log_info("[RECEIVER] Request #%d: type=%d", req_num, reqType);
  }

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
    Log_warn("[RECEIVER] Unrecognized request type: %d", reqType);
    break;
  }
  
  if (req_num % 50 == 0) {
    Log_info("[RECEIVER] Request #%d: Handled, response len=%zu", req_num, respLen);
  }

  return respLen;
}

//=============================================================================
// Luigi Scheduler Management
//=============================================================================

void LuigiReceiver::InitScheduler(uint32_t shard_id) {
  if (scheduler_ != nullptr) {
    Log_info("[RECEIVER-INIT] Scheduler already initialized for shard %d", shard_id);
    return; // Already initialized
  }

  Log_info("[RECEIVER-INIT] Creating SchedulerLuigi for shard %d", shard_id);
  scheduler_ = new SchedulerLuigi();
  Log_info("[RECEIVER-INIT] SchedulerLuigi created successfully");
  
  scheduler_->SetPartitionId(shard_id);

  // Set worker count based on warehouses (default 1)
  uint32_t worker_count = (config_.warehouses > 0) ? config_.warehouses : 1;
  scheduler_->SetWorkerCount(worker_count);
  
  Log_info("[RECEIVER-INIT] Scheduler configured: partition=%d, workers=%d", 
           shard_id, worker_count);

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

  // TEMPORARILY DISABLED: Server-side LuigiClient for leader agreement
  // Testing if FastTransport creation causes the crash
  Log_info("[RECEIVER-INIT] Skipping server-side client creation for debugging");
  scheduler_->SetLuigiClient(nullptr);
  
  /* ORIGINAL CODE - COMMENTED FOR DEBUGGING
  Log_info("[RECEIVER-INIT] Creating dedicated client transport for leader agreement...");
  std::string local_uri = config_.shard(shard_id, mako::convertCluster("localhost")).host;
  FastTransport* coordinator_transport = new FastTransport(
      config_.configFile,
      local_uri,
      "localhost",
      1, 20,  // Request types 1-20
      0,  // physPort
      0,  // numa
      shard_id,
      shard_id + 100);  // Unique ID for coordinator client
  
  Log_info("[RECEIVER-INIT] Creating server-side LuigiClient for leader agreement...");
  janus::LuigiClient* server_client = new janus::LuigiClient(
      config_.configFile,  // Config file path
      coordinator_transport,  // Dedicated client transport
      shard_id + 1000);  // Unique client ID for server
  scheduler_->SetLuigiClient(server_client);
  Log_info("[RECEIVER-INIT] Server-side LuigiClient created with dedicated transport");
  */

  // Start scheduler threads
  Log_info("[RECEIVER-INIT] Starting scheduler threads...");
  scheduler_->Start();
  Log_info("[RECEIVER-INIT] Scheduler threads started");

  // Transport and RPC setup handled externally (like Mako)
  Log_info("[RECEIVER-INIT] Luigi scheduler fully initialized for shard %d with %d workers", 
           shard_id, worker_count);
}

void LuigiReceiver::StopScheduler() {
  // Transport cleanup handled externally

  if (scheduler_ != nullptr) {
    uint32_t shard_id = scheduler_->GetPartitionId();
    scheduler_->Stop();
    delete scheduler_;
    scheduler_ = nullptr;
    Log_info("Luigi scheduler stopped for shard %d", shard_id);
  }
}

//=============================================================================
// Request Handlers
//=============================================================================

void LuigiReceiver::HandleDispatch(char *reqBuf, char *respBuf,
                                   size_t &respLen) {
  auto *req = reinterpret_cast<luigi::DispatchRequest *>(reqBuf);

  if (req->txn_id % 50 == 0) {
    Log_info("[RECEIVER-DISPATCH] TXN-%lu: HandleDispatch called (req_nr=%u, num_ops=%u)",
             req->txn_id, req->req_nr, req->num_ops);
  }

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

  // Extract working_set (TPC-C parameters)
  // NOTE: working_set_data is NOT at req->working_set_data offset!
  // It's serialized immediately AFTER the actual ops_data in the message.
  // Calculate actual position: header + actual_ops_data_length
  std::map<int32_t, std::string> working_set;
  size_t header_size = offsetof(luigi::DispatchRequest, ops_data);
  size_t ops_data_length = data_ptr - req->ops_data;  // Actual ops data consumed
  char *ws_ptr = reinterpret_cast<char*>(req) + header_size + ops_data_length;

  Log_info("[SERVER-DISPATCH] TXN-%lu: ws_ptr calculation: header_size=%zu, ops_data_length=%zu, ws_offset=%zu",
           req->txn_id, header_size, ops_data_length, header_size + ops_data_length);
  Log_info("[SERVER-DISPATCH] TXN-%lu: Parsing working_set with %u entries (ptr=%p, first 16 bytes: %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x)",
           req->txn_id, req->num_working_set, (void*)ws_ptr,
           (unsigned char)ws_ptr[0], (unsigned char)ws_ptr[1], (unsigned char)ws_ptr[2], (unsigned char)ws_ptr[3],
           (unsigned char)ws_ptr[4], (unsigned char)ws_ptr[5], (unsigned char)ws_ptr[6], (unsigned char)ws_ptr[7],
           (unsigned char)ws_ptr[8], (unsigned char)ws_ptr[9], (unsigned char)ws_ptr[10], (unsigned char)ws_ptr[11],
           (unsigned char)ws_ptr[12], (unsigned char)ws_ptr[13], (unsigned char)ws_ptr[14], (unsigned char)ws_ptr[15]);
  for (uint16_t i = 0; i < req->num_working_set && i < luigi::kMaxWorkingSetEntries; i++) {
    // Read var_id (4 bytes)
    int32_t var_id = *reinterpret_cast<int32_t *>(ws_ptr);
    ws_ptr += sizeof(int32_t);

    // Read value length (2 bytes)
    uint16_t vlen = *reinterpret_cast<uint16_t *>(ws_ptr);
    ws_ptr += sizeof(uint16_t);

    // Read value
    std::string value(ws_ptr, vlen);
    ws_ptr += vlen;

    working_set[var_id] = value;
    if (i < 5 || i == req->num_working_set - 1) {  // Log first 5 and last
      Log_info("  [SERVER-DISPATCH] TXN-%lu entry[%u]: var_id=%d, value='%s' (len=%u)",
               req->txn_id, i, var_id, value.c_str(), vlen);
    }
  }
  Log_info("[SERVER-DISPATCH] TXN-%lu: Parsed working_set size=%zu", req->txn_id, working_set.size());

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

  if (txn_id % 50 == 0) {
    Log_info("[RECEIVER-DISPATCH] TXN-%lu: Dispatching to scheduler (%zu ops, %zu shards)",
             txn_id, ops.size(), involved_shards.size());
  }

  // Dispatch to scheduler with async callback
  scheduler_->LuigiDispatchFromRequest(
      txn_id, req->expected_time, req->txn_type, ops, working_set, involved_shards,
      [this, txn_id](int status, uint64_t commit_ts,
                     const std::vector<std::string> &read_results) {
        if (txn_id % 50 == 0) {
          Log_info("[RECEIVER-CALLBACK] TXN-%lu: Scheduler callback (status=%d, commit_ts=%lu)",
                   txn_id, status, commit_ts);
        }
        StoreResult(txn_id, status, commit_ts, read_results);
      });

  if (txn_id % 50 == 0) {
    Log_info("[RECEIVER-DISPATCH] TXN-%lu: Queued to scheduler, returning QUEUED status", txn_id);
  }

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

  // DISABLED: Periodic cleanup (causes issues)
  // static int cleanup_counter = 0;
  // if (++cleanup_counter >= 100) {
  //   cleanup_counter = 0;
  //   lock.unlock();
  //   CleanupStaleResults();
  // }
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
    : shard_idx_(shard_idx), benchmark_type_(benchmark_type), config_(nullptr) {
  // Get config from BenchmarkConfig singleton
  auto &cfg = BenchmarkConfig::getInstance();
  
  // Use stored config file path to create our own Configuration object
  // (Don't use cfg.getConfig() as it may be a dangling pointer to a temporary)
  std::string config_file = cfg.getShardConfigFile();
  if (config_file.empty()) {
    Log_error("LuigiServer: config file path not set in BenchmarkConfig");
    throw std::runtime_error("Config file path not available");
  }
  
  // Create our own Configuration object that we own
  config_ = new transport::Configuration(config_file);
  
  receiver_ = new LuigiReceiver(config_file);
}

LuigiServer::~LuigiServer() {
  if (receiver_) {
    receiver_->StopScheduler();
    delete receiver_;
    receiver_ = nullptr;
  }
  if (config_) {
    delete config_;
    config_ = nullptr;
  }
}

void LuigiServer::Run() {
  auto &cfg = BenchmarkConfig::getInstance();

  std::cout << "\n=== Luigi Server Initialization ===\n";

  // NOTE: Luigi doesn't use Janus Config or TxLogServer
  // This file (luigi_server.cc) is currently unused in client-only mode

  // Note: Luigi OWD service should already be initialized in main before creating LuigiServer

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
  // Note: OWD is managed by main, not here

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
