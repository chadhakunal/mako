/**
 * @file luigi_benchmark_client.cc
 * @brief Implementation of Luigi benchmark client
 */

#include "deptran/luigi/luigi_benchmark_client.h"
#include "deptran/luigi/luigi_client.h"
#include "deptran/luigi/tpcc_txn_generator.h"  // For TPCC_VAR_* constants
#include "mako/benchmarks/benchmark_config.h"
#include "mako/lib/configuration.h"
#include "mako/lib/fasttransport.h"

#include <future>
#include <memory>
#include <numeric>

namespace mako {
namespace luigi {

// Constructor
LuigiBenchmarkClient::LuigiBenchmarkClient(
    const LuigiBenchmarkClient::Config &config)
    : config_(config), luigi_client_(nullptr), transport_(nullptr) {}

LuigiBenchmarkClient::~LuigiBenchmarkClient() {
  // LuigiClient needs to be destroyed before transport
  luigi_client_.reset();

  // Transport is owned by BenchmarkConfig/RpcSetup, do not delete it here
  transport_ = nullptr;
}

bool LuigiBenchmarkClient::Initialize() {
  try {
    // Get transport from BenchmarkConfig (created by setup_luigi_transport)
    auto &cfg = BenchmarkConfig::getInstance();
    auto &server_transports = cfg.getServerTransports();

    if (server_transports.empty()) {
      std::cerr << "No transports available in BenchmarkConfig. "
                << "Did you call setup_luigi_transport()?" << std::endl;
      return false;
    }

    // Use the first transport (or could use round-robin for load balancing)
    transport_ = server_transports[0];

    // Create LuigiClient with the transport
    luigi_client_ = std::make_unique<LuigiClient>(
        config_.config_file, transport_, 0 /* client_id */);

  } catch (const std::exception &e) {
    std::cerr << "Failed to initialize LuigiBenchmarkClient: " << e.what()
              << std::endl;
    return false;
  }

  // Pre-allocate thread stats
  thread_stats_.resize(config_.num_threads);

  return true;
}

mako::luigi::BenchmarkStats
LuigiBenchmarkClient::RunBenchmark(LuigiBenchmarkClient::BenchmarkType type) {
  // Create generator based on type
  switch (type) {
  case BenchmarkType::BM_MICRO:
    generator_ = std::make_unique<MicroTxnGenerator>(config_.gen_config);
    break;
  case BenchmarkType::BM_MICRO_SINGLE:
    generator_ = std::make_unique<SingleShardMicroTxnGenerator>(
        config_.gen_config, config_.shard_index);
    break;
  case BenchmarkType::BM_TPCC:
    generator_ = std::make_unique<TPCCTxnGenerator>(config_.gen_config);
    // Set shard index for shard-local warehouse assignment (like Mako)
    static_cast<TPCCTxnGenerator*>(generator_.get())->SetShardIndex(config_.shard_index);
    // Set cross-shard percentage
    static_cast<TPCCTxnGenerator*>(generator_.get())->SetRemoteItemPct(config_.cross_shard_pct);
    Log_info("[TPCC-GEN] Created generator: shard_index=%d, shard_num=%d, warehouses_per_shard=%d, total_warehouses=%d, cross_shard_pct=%d%%",
             config_.shard_index, config_.gen_config.shard_num, config_.gen_config.num_warehouses,
             config_.gen_config.shard_num * config_.gen_config.num_warehouses, config_.cross_shard_pct);
    break;
  }

  if (!generator_) {
    std::cerr << "Failed to create transaction generator" << std::endl;
    return BenchmarkStats{};
  }

  // Clear previous stats
  for (auto &ts : thread_stats_) {
    ts.records.clear();
    ts.committed = 0;
    ts.aborted = 0;
  }

  // Reserve space for latency records (estimate)
  size_t estimated_txns = config_.duration_sec * 10000 / config_.num_threads;
  for (auto &ts : thread_stats_) {
    ts.records.reserve(estimated_txns);
  }

  // Start benchmark
  running_ = true;
  start_time_us_ = GetTimestampUs();
  uint64_t target_end_time = start_time_us_ + config_.duration_sec * 1000000ULL;

  // Initialize per-worker in-flight transaction tracking
  in_flight_.clear();
  in_flight_.reserve(config_.num_threads * kMaxInFlightPerWorker);
  for (int i = 0; i < config_.num_threads * kMaxInFlightPerWorker; ++i) {
    in_flight_.push_back(std::make_shared<InFlightTxn>());
    in_flight_.back()->in_use = false;
    in_flight_.back()->completed.store(false);
  }

  std::cout << "Starting benchmark: type=" << static_cast<int>(type)
            << ", threads=" << config_.num_threads
            << ", duration=" << config_.duration_sec << "s" << std::endl;

  // Launch worker threads
  std::vector<std::thread> workers;
  for (int i = 0; i < config_.num_threads; ++i) {
    workers.emplace_back(&LuigiBenchmarkClient::WorkerThread, this, i);
  }

  // Wait for duration to elapse or Stop() called
  while (running_ && GetTimestampUs() < target_end_time) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Signal workers to stop
  running_ = false;
  end_time_us_ = GetTimestampUs();

  // Wait for all workers to finish
  for (auto &w : workers) {
    w.join();
  }

  std::cout << "Benchmark completed" << std::endl;

  // Calculate and return statistics
  return CalculateStats();
}

void LuigiBenchmarkClient::WorkerThread(int thread_id) {
  try {
    Log_info("[CLIENT-THREAD-%d] Worker thread started (ASYNC PIPELINING mode)", thread_id);
    int txn_dispatched = 0;
    int txn_completed = 0;

    // ASYNC PIPELINING LOOP:
    // 1. Send as many transactions as we can (up to kMaxInFlightPerWorker)
    // 2. Harvest any completed transactions (non-blocking)
    // 3. Repeat until benchmark ends
    while (running_) {
      // Harvest completed transactions (non-blocking)
      int harvested = HarvestCompletions(thread_id);
      txn_completed += harvested;

      // Try to fill up in-flight slots
      int in_flight = CountInFlight(thread_id);
      while (in_flight < kMaxInFlightPerWorker && running_) {
        int slot = FindFreeSlot(thread_id);
        if (slot < 0) break;  // No free slots

        if (DispatchOneTransactionAsync(thread_id, slot)) {
          txn_dispatched++;
          in_flight++;
        } else {
          // Failed to dispatch - break and try harvesting
          break;
        }
      }

      // Small yield to prevent CPU spinning when all slots are full
      if (in_flight >= kMaxInFlightPerWorker) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    }

    // Final harvest after benchmark stops
    while (CountInFlight(thread_id) > 0) {
      HarvestCompletions(thread_id);
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    Log_info("[CLIENT-THREAD-%d] Worker stopping (dispatched=%d, completed=%d)", 
             thread_id, txn_dispatched, txn_completed);
  } catch (int e) {
    Log_info("[CLIENT-THREAD-%d] Caught int exception: %d (coroutine unwinding)", thread_id, e);
  } catch (const std::exception& e) {
    Log_error("[CLIENT-THREAD-%d] Exception: %s", thread_id, e.what());
  } catch (...) {
    Log_error("[CLIENT-THREAD-%d] Unknown exception", thread_id);
  }
}

bool LuigiBenchmarkClient::DispatchOneTransaction(int thread_id) {
  // Generate transaction request
  LuigiTxnRequest req;
  try {
    std::lock_guard<std::mutex> lock(generator_mutex_);
    generator_->GetTxnReq(&req, 0, 0);
  } catch (int err) {
    Log_error("[CLIENT-DISPATCH] GetTxnReq threw int: %d", err);
    return false;
  } catch (const std::exception& e) {
    Log_error("[CLIENT-DISPATCH] GetTxnReq exception: %s", e.what());
    return false;
  } catch (...) {
    Log_error("[CLIENT-DISPATCH] GetTxnReq unknown exception");
    return false;
  }

  // Assign unique transaction ID
  req.txn_id = next_txn_id_.fetch_add(1);
  
  if (req.txn_id % 50 == 0) {
    Log_info("[CLIENT-DISPATCH] TXN-%lu: Generated (type=%d, %zu ops, %zu target shards)",
             req.txn_id, req.txn_type, req.ops.size(), req.target_shards.size());
  }

  // Record start time
  TxnRecord record;
  record.txn_id = req.txn_id;
  record.start_time_us = GetTimestampUs();
  record.txn_type = req.txn_type;

  // Dispatch via ShardClient
  bool committed = DispatchRequest(req);

  // Record end time
  record.end_time_us = GetTimestampUs();
  record.committed = committed;
  
  if (req.txn_id % 50 == 0) {
    Log_info("[CLIENT-DISPATCH] TXN-%lu: %s (latency=%lu us)",
             req.txn_id, committed ? "COMMITTED" : "ABORTED",
             record.end_time_us - record.start_time_us);
  }

  // Store record
  auto &ts = thread_stats_[thread_id];
  ts.records.push_back(record);
  if (committed) {
    ts.committed++;
  } else {
    ts.aborted++;
  }

  return committed;
}

bool LuigiBenchmarkClient::DispatchRequest(const LuigiTxnRequest &req) {
  if (!luigi_client_) {
    Log_error("[CLIENT-DISPATCH] TXN-%lu: No luigi_client available!", req.txn_id);
    return false;
  }

  if (req.txn_id % 50 == 0) {
    Log_info("[CLIENT-DISPATCH] TXN-%lu: Building dispatch request for %zu shards",
             req.txn_id, req.target_shards.size());
  }

  // Create promise for synchronous wait
  auto promise = std::make_shared<std::promise<bool>>();
  auto future = promise->get_future();

  // Build dispatch request
  std::map<int, LuigiDispatchBuilder *> requests_per_shard;

  // Send to all involved shards - each shard executes and responds independently
  // Client collects all responses to determine final status
  for (uint32_t shard_id : req.target_shards) {
    auto *builder = new LuigiDispatchBuilder();
    builder->SetTxnId(req.txn_id).SetReqNr(req.req_id).SetTxnType(req.txn_type);
    
    // Set target_server_id using Mako's formula from erpc_runner/configuration.cc:
    // target_server_id = sender_shard * num_warehouses + sender_client
    //                    - (sender_shard < target_shard ? 0 : num_warehouses)
    //
    // SPECIAL CASE: For same-shard operations, use a simple local partition ID
    // (helper queues are only for cross-shard communication)
    int warehouses_per_shard = config_.gen_config.num_warehouses;
    uint16_t sender_shard = config_.shard_index;
    uint16_t sender_client = 0;  // Use first warehouse as representative

    // IMPORTANT: target_server_id is used for ROUTING on the receiving shard
    // In Mako's queue model, each shard registers queues for OTHER shards' partitions
    // So target_server_id should be the SENDER's partition ID, not the destination's!
    //
    // Queue registration pattern (for 2 shards, 6 warehouses each):
    // - Shard 0 registers queues: 6-11 (for requests FROM shard 1)
    // - Shard 1 registers queues: 0-5 (for requests FROM shard 0)
    //
    // Therefore: When shard 0 sends to shard 1, use target_server_id=0 (sender's partition)
    uint16_t target_server_id;
    if (sender_shard == shard_id) {
      // Local same-shard request - use local partition ID
      target_server_id = sender_client;
    } else {
      // Cross-shard request - use SENDER's partition ID for routing
      target_server_id = sender_shard * warehouses_per_shard + sender_client;
    }

    Log_info("[CLIENT-DISPATCH] TXN-%lu: FROM shard_%u TO shard_%u, target_server_id=%u (sender partition for queue routing)",
             req.txn_id, sender_shard, shard_id, target_server_id);
    
    builder->SetTargetServer(target_server_id);

    // Calculate expected execution time
    uint64_t expected_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    expected_time += 5000; // +5ms buffer

    builder->SetExpectedTime(expected_time);

    // Add ops
    for (const auto &op : req.ops) {
      if (op.op_type == 1) { // READ
        builder->AddRead(op.table_id, op.key);
      } else { // WRITE
        builder->AddWrite(op.table_id, op.key, op.value);
      }
    }

    // Add working_set (TPC-C parameters)
    builder->SetWorkingSet(req.working_set);

    // Set involved shards for leader agreement (CRITICAL for multi-shard txns!)
    std::vector<uint32_t> involved_shards_vec(req.target_shards.begin(),
                                               req.target_shards.end());
    builder->SetInvolvedShards(involved_shards_vec);

    requests_per_shard[shard_id] = builder;
    
    if (req.txn_id % 50 == 0) {
      Log_info("[CLIENT-DISPATCH] TXN-%lu: Request for shard-%u ready (%zu ops)",
               req.txn_id, shard_id, req.ops.size());
    }
  }

  if (req.txn_id % 50 == 0) {
    Log_info("[CLIENT-DISPATCH] TXN-%lu: Invoking LuigiClient->InvokeDispatch()",
             req.txn_id);
  }

  // Call LuigiClient
  luigi_client_->InvokeDispatch(
      req.txn_id, requests_per_shard,
      [promise, txn_id = req.txn_id](char *respBuf) {
        // Success callback
        if (txn_id % 50 == 0) {
          Log_info("[CLIENT-RESPONSE] TXN-%lu: Success callback received", txn_id);
        }
        promise->set_value(true);
      },
      [promise, txn_id = req.txn_id]() {
        // Error callback
        if (txn_id % 50 == 0) {
          Log_error("[CLIENT-RESPONSE] TXN-%lu: Error callback received", txn_id);
        }
        promise->set_value(false);
      },
      2000 // timeout ms (increased to handle slow transactions)
  );

  // Wait for result (with shorter timeout to handle shard unavailability)
  if (future.wait_for(std::chrono::milliseconds(3000)) ==
      std::future_status::ready) {
    bool success = future.get();
    // Cleanup builders
    for (auto &pair : requests_per_shard) {
      delete pair.second;
    }
    return success;
  } else {
    // Timeout - shard may be unavailable
    if (req.txn_id % 50 == 0) {
      Log_error("[CLIENT-DISPATCH] TXN-%lu: TIMEOUT waiting for response", req.txn_id);
    }
    // Cleanup builders
    for (auto &pair : requests_per_shard) {
      delete pair.second;
    }
    return false;
  }
}

void LuigiBenchmarkClient::Stop() { running_ = false; }

// TEST FUNCTION: Send ONE hardcoded cross-shard NewOrder transaction
bool LuigiBenchmarkClient::TestOneCrossShardTransaction() {
  Log_info("=== TESTING: Sending ONE cross-shard NewOrder transaction ===");

  // Hardcoded NewOrder with 2 items:
  // - Item 0: warehouse 0 (shard 0)
  // - Item 1: warehouse 3 (shard 1) - this makes it cross-shard

  LuigiTxnRequest req;
  req.txn_id = next_txn_id_.fetch_add(1);
  req.txn_type = janus::LUIGI_TXN_TPCC_NEW_ORDER;

  // NewOrder parameters (using constants from tpcc_txn_generator.h):
  // w_id = 0, d_id = 0, c_id = 100, ol_cnt = 2
  req.working_set[janus::TPCC_VAR_W_ID] = "0";           // warehouse 0
  req.working_set[janus::TPCC_VAR_D_ID] = "0";           // district 0
  req.working_set[janus::TPCC_VAR_C_ID] = "100";         // customer 100
  req.working_set[janus::TPCC_VAR_OL_CNT] = "2";         // 2 order lines

  // Item 0: warehouse 0 (local to shard 0) - using proper TPCC_VAR_* macros
  req.working_set[janus::TPCC_VAR_I_ID(0)] = "50";           // item 50
  req.working_set[janus::TPCC_VAR_S_W_ID(0)] = "0";          // from warehouse 0
  req.working_set[janus::TPCC_VAR_OL_QUANTITY(0)] = "5";     // quantity 5

  // Item 1: warehouse 3 (shard 1 with 6 warehouses) - CROSS-SHARD!
  req.working_set[janus::TPCC_VAR_I_ID(1)] = "100";          // item 100
  req.working_set[janus::TPCC_VAR_S_W_ID(1)] = "3";          // from warehouse 3 - shard 1
  req.working_set[janus::TPCC_VAR_OL_QUANTITY(1)] = "3";     // quantity 3

  // Determine involved shards (0 and 1)
  req.target_shards = {0, 1};

  Log_info("[TEST] TXN-%lu: Cross-shard NewOrder - w0:d0:c100, 2 items (w0,w3)", req.txn_id);
  Log_info("[TEST] TXN-%lu: Item0: i_id=50 from w_id=0 (shard 0), qty=5", req.txn_id);
  Log_info("[TEST] TXN-%lu: Item1: i_id=100 from w_id=3 (shard 1), qty=3", req.txn_id);
  Log_info("[TEST] TXN-%lu: Involved shards: {0, 1}", req.txn_id);

  // Dispatch like normal
  return DispatchRequest(req);
}

mako::luigi::BenchmarkStats LuigiBenchmarkClient::CalculateStats() {
  BenchmarkStats stats;

  // Aggregate all latencies
  std::vector<uint64_t> all_latencies;

  for (const auto &ts : thread_stats_) {
    stats.committed_txns += ts.committed;
    stats.aborted_txns += ts.aborted;

    for (const auto &record : ts.records) {
      uint64_t latency = record.end_time_us - record.start_time_us;
      all_latencies.push_back(latency);
    }
  }

  stats.total_txns = stats.committed_txns + stats.aborted_txns;
  stats.duration_ms = (end_time_us_ - start_time_us_) / 1000;

  // Calculate throughput
  double duration_sec = stats.duration_ms / 1000.0;
  if (duration_sec > 0) {
    stats.throughput_tps = stats.committed_txns / duration_sec;
  }

  // Calculate latency percentiles
  if (!all_latencies.empty()) {
    std::sort(all_latencies.begin(), all_latencies.end());

    // Average
    uint64_t sum =
        std::accumulate(all_latencies.begin(), all_latencies.end(), 0ULL);
    stats.avg_latency_us = static_cast<double>(sum) / all_latencies.size();

    // Percentiles
    auto percentile = [&](double p) -> double {
      size_t idx = static_cast<size_t>(p * all_latencies.size());
      if (idx >= all_latencies.size())
        idx = all_latencies.size() - 1;
      return static_cast<double>(all_latencies[idx]);
    };

    stats.p50_latency_us = percentile(0.50);
    stats.p99_latency_us = percentile(0.99);
    stats.p999_latency_us = percentile(0.999);
  }

  return stats;
}

//=============================================================================
// Async Dispatch Helper Methods
//=============================================================================

int LuigiBenchmarkClient::FindFreeSlot(int thread_id) {
  int base = thread_id * kMaxInFlightPerWorker;
  for (int i = 0; i < kMaxInFlightPerWorker; ++i) {
    if (!in_flight_[base + i]->in_use) {
      return i;
    }
  }
  return -1;  // No free slots
}

int LuigiBenchmarkClient::HarvestCompletions(int thread_id) {
  int base = thread_id * kMaxInFlightPerWorker;
  int harvested = 0;
  
  for (int i = 0; i < kMaxInFlightPerWorker; ++i) {
    auto& slot = *in_flight_[base + i];
    if (slot.in_use && slot.completed.load(std::memory_order_acquire)) {
      // Record the completed transaction
      TxnRecord record;
      record.txn_id = slot.txn_id;
      record.start_time_us = slot.start_time_us;
      record.end_time_us = GetTimestampUs();
      record.committed = slot.committed.load();
      record.txn_type = slot.txn_type;
      
      auto& ts = thread_stats_[thread_id];
      ts.records.push_back(record);
      if (record.committed) {
        ts.committed++;
      } else {
        ts.aborted++;
      }
      
      // Free the slot
      slot.in_use = false;
      slot.completed.store(false, std::memory_order_release);
      harvested++;
    }
  }
  
  return harvested;
}

int LuigiBenchmarkClient::CountInFlight(int thread_id) {
  int base = thread_id * kMaxInFlightPerWorker;
  int count = 0;
  for (int i = 0; i < kMaxInFlightPerWorker; ++i) {
    if (in_flight_[base + i]->in_use) {
      count++;
    }
  }
  return count;
}

bool LuigiBenchmarkClient::DispatchOneTransactionAsync(int thread_id, int slot_index) {
  // Generate transaction request
  LuigiTxnRequest req;
  try {
    std::lock_guard<std::mutex> lock(generator_mutex_);
    generator_->GetTxnReq(&req, 0, 0);
  } catch (...) {
    return false;
  }

  // Assign unique transaction ID
  req.txn_id = next_txn_id_.fetch_add(1);
  
  // Set up the in-flight slot
  int base = thread_id * kMaxInFlightPerWorker;
  auto slot_ptr = in_flight_[base + slot_index];  // shared_ptr captures slot ownership
  slot_ptr->txn_id = req.txn_id;
  slot_ptr->start_time_us = GetTimestampUs();
  slot_ptr->txn_type = req.txn_type;
  slot_ptr->completed.store(false, std::memory_order_release);
  slot_ptr->committed.store(false, std::memory_order_release);
  slot_ptr->in_use = true;

  // Build dispatch request
  std::map<int, LuigiDispatchBuilder*> requests_per_shard;

  for (uint32_t shard_id : req.target_shards) {
    auto* builder = new LuigiDispatchBuilder();
    builder->SetTxnId(req.txn_id).SetReqNr(req.req_id).SetTxnType(req.txn_type);
    
    int warehouses_per_shard = config_.gen_config.num_warehouses;
    uint16_t sender_shard = config_.shard_index;
    uint16_t sender_client = 0;

    uint16_t target_server_id;
    if (sender_shard == shard_id) {
      target_server_id = sender_client;
    } else {
      target_server_id = sender_shard * warehouses_per_shard + sender_client;
    }
    builder->SetTargetServer(target_server_id);

    uint64_t expected_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    expected_time += 5000;
    builder->SetExpectedTime(expected_time);

    for (const auto& op : req.ops) {
      if (op.op_type == 1) {
        builder->AddRead(op.table_id, op.key);
      } else {
        builder->AddWrite(op.table_id, op.key, op.value);
      }
    }
    builder->SetWorkingSet(req.working_set);
    
    // Pass involved shards to server for cross-shard detection
    std::vector<uint32_t> involved_shards_vec(req.target_shards.begin(), req.target_shards.end());
    builder->SetInvolvedShards(involved_shards_vec);
    
    requests_per_shard[shard_id] = builder;
  }

  // Dispatch with async callbacks that set completion flag
  // Capture slot_ptr (shared_ptr) to keep InFlightTxn alive until callback completes
  // Capture requests_per_shard to ensure builders stay alive until RPC completes
  // NOTE: Now that HandleDispatchReply only calls callback when ALL responses received,
  // we can safely delete builders in callback without risk of double-free
  luigi_client_->InvokeDispatch(
      req.txn_id, requests_per_shard,
      [slot_ptr, requests_per_shard](char* respBuf) {
        // Success callback - set flags and cleanup builders
        slot_ptr->committed.store(true, std::memory_order_release);
        slot_ptr->completed.store(true, std::memory_order_release);
        // Cleanup builders (safe - callback fires only once per transaction now)
        for (auto& pair : requests_per_shard) {
          delete pair.second;
        }
      },
      [slot_ptr, requests_per_shard]() {
        // Error callback - set flags and cleanup builders
        slot_ptr->committed.store(false, std::memory_order_release);
        slot_ptr->completed.store(true, std::memory_order_release);
        // Cleanup builders (safe - callback fires only once per transaction now)
        for (auto& pair : requests_per_shard) {
          delete pair.second;
        }
      },
      2000  // timeout ms
  );

  return true;
}

} // namespace luigi
} // namespace mako
