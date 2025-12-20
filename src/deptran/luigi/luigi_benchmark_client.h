/**
 * @file luigi_benchmark_client.h
 * @brief Luigi benchmark client for Tiga-style stored-procedure benchmarks
 *
 * This client uses the transaction generators (MicroTxnGenerator,
 * TPCCTxnGenerator) and dispatches them through Luigi's timestamp-ordered
 * execution protocol.
 *
 * Usage:
 *   LuigiBenchmarkClient client(config_file, cluster, shard_idx);
 *   client.Initialize();
 *
 *   // Run micro benchmark
 *   auto micro_results = client.RunMicroBenchmark(duration_sec, num_threads);
 *
 *   // Run TPC-C benchmark
 *   auto tpcc_results = client.RunTPCCBenchmark(duration_sec, num_threads);
 */

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "luigi_client.h"

#include "micro_txn_generator.h"
#include "tpcc_txn_generator.h"
#include "txn_generator.h"

namespace mako {
namespace luigi {

using namespace janus;

// Benchmark result statistics
struct BenchmarkStats {
  uint64_t total_txns = 0;
  uint64_t committed_txns = 0;
  uint64_t aborted_txns = 0;
  double throughput_tps = 0.0; // transactions per second
  double avg_latency_us = 0.0; // average latency in microseconds
  double p50_latency_us = 0.0;
  double p99_latency_us = 0.0;
  double p999_latency_us = 0.0;
  uint64_t duration_ms = 0;

  void Print() const {
    printf("========== Benchmark Results ==========\n");
    printf("Duration:          %lu ms\n", duration_ms);
    printf("Total Txns:        %lu\n", total_txns);
    printf("Committed:         %lu\n", committed_txns);
    printf("Aborted:           %lu (%.2f%%)\n", aborted_txns,
           total_txns > 0 ? 100.0 * aborted_txns / total_txns : 0.0);
    printf("Throughput:        %.2f txns/sec\n", throughput_tps);
    printf("Avg Latency:       %.2f us\n", avg_latency_us);
    printf("P50 Latency:       %.2f us\n", p50_latency_us);
    printf("P99 Latency:       %.2f us\n", p99_latency_us);
    printf("P99.9 Latency:     %.2f us\n", p999_latency_us);
    printf("========================================\n");
    
    // Output Mako-compatible keywords for test scripts
    printf("agg_persist_throughput: %.2f txns/sec\n", throughput_tps);
    double abort_ratio = total_txns > 0 ? 100.0 * aborted_txns / total_txns : 0.0;
    printf("NewOrder_remote_abort_ratio: %.2f%%\n", abort_ratio);
  }
};

// Transaction completion record for latency tracking
struct TxnRecord {
  uint64_t txn_id;
  uint64_t start_time_us;
  uint64_t end_time_us;
  bool committed;
  int txn_type;
};

// Maximum number of in-flight transactions per worker for pipelining
constexpr int kMaxInFlightPerWorker = 16;

// In-flight transaction tracking for async dispatch
struct InFlightTxn {
  uint64_t txn_id = 0;
  uint64_t start_time_us = 0;
  int txn_type = 0;
  std::atomic<bool> completed{false};
  std::atomic<bool> committed{false};
  bool in_use = false;  // Whether this slot is currently tracking a txn
};

/**
 * @class LuigiBenchmarkClient
 * @brief Client for running Tiga-style benchmarks on Luigi
 *
 * This class provides:
 * - Transaction generation using MicroTxnGenerator or TPCCTxnGenerator
 * - Dispatch to Luigi via remoteLuigiDispatch
 * - Latency/throughput measurement
 * - Support for closed-loop and open-loop workloads
 */
class LuigiBenchmarkClient {
public:
  // Benchmark type enumeration
  enum class BenchmarkType { BM_MICRO, BM_MICRO_SINGLE, BM_TPCC };

  // Configuration for the benchmark
  struct Config {
    std::string config_file;   // Path to shard config
    std::string cluster;       // Cluster name
    int shard_index = 0;       // This client's shard index
    int par_id = 0;            // Partition id within shard
    int num_shards = 1;        // Total number of shards
    int num_threads = 1;       // Worker threads
    int duration_sec = 10;     // Benchmark duration
    bool is_open_loop = false; // Open-loop vs closed-loop
    int target_rate = 0;       // Target ops/sec for open-loop

    // Generator config
    janus::TxnGeneratorConfig gen_config;
  };

  LuigiBenchmarkClient(const Config &config);
  ~LuigiBenchmarkClient();

  // Initialize client (create ShardClient, generator, etc.)
  bool Initialize();

  // Run benchmark and return results
  BenchmarkStats RunBenchmark(BenchmarkType type);

  // Convenience methods
  BenchmarkStats RunMicroBenchmark() {
    return RunBenchmark(BenchmarkType::BM_MICRO);
  }

  BenchmarkStats RunSingleShardMicroBenchmark() {
    return RunBenchmark(BenchmarkType::BM_MICRO_SINGLE);
  }

  BenchmarkStats RunTPCCBenchmark() {
    return RunBenchmark(BenchmarkType::BM_TPCC);
  }

  // Stop benchmark early
  void Stop();

  // TEST: Send one hardcoded cross-shard transaction
  bool TestOneCrossShardTransaction();

  // Get the underlying LuigiClient (for wiring to scheduler)
  janus::LuigiClient* GetLuigiClient() { return luigi_client_.get(); }

private:
  // Worker thread function (closed-loop)
  void WorkerThread(int thread_id);

  // Generate and dispatch a single transaction
  bool DispatchOneTransaction(int thread_id);

  // Dispatch a LuigiTxnRequest via ShardClient
  bool DispatchRequest(const LuigiTxnRequest &req);

  // Calculate statistics from recorded transactions
  BenchmarkStats CalculateStats();

  // Get current time in microseconds
  static uint64_t GetTimestampUs() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
               now.time_since_epoch())
        .count();
  }

  // Configuration
  Config config_;

  // Transaction generator (polymorphic)
  std::unique_ptr<LuigiTxnGenerator> generator_;

  // LuigiClient (replaces ShardClient)
  std::unique_ptr<LuigiClient> luigi_client_;

  // Transport
  Transport *transport_ = nullptr; // Don't own, retrieved from BenchmarkConfig

  // Benchmark state
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> next_txn_id_{1};

  // Per-thread results
  struct ThreadStats {
    std::vector<TxnRecord> records;
    uint64_t committed = 0;
    uint64_t aborted = 0;
  };
  std::vector<ThreadStats> thread_stats_;

  // Timing
  uint64_t start_time_us_ = 0;
  uint64_t end_time_us_ = 0;

  // Mutex for generator access (generators may not be thread-safe)
  std::mutex generator_mutex_;

  // ============ Async Dispatch State ============
  // Per-worker in-flight transaction tracking for pipelining
  // Layout: in_flight_[thread_id * kMaxInFlightPerWorker + slot_index]
  // Using unique_ptr because std::atomic is non-movable
  std::vector<std::shared_ptr<InFlightTxn>> in_flight_;
  
  // Async dispatch helper methods
  // Find a free slot in the in-flight array for this worker
  int FindFreeSlot(int thread_id);
  
  // Check for completed transactions and harvest their results  
  int HarvestCompletions(int thread_id);
  
  // Count how many slots are currently in use for this worker
  int CountInFlight(int thread_id);
  
  // Async version of dispatch - returns immediately
  bool DispatchOneTransactionAsync(int thread_id, int slot_index);
};

// Helper function to create default configs
inline janus::TxnGeneratorConfig
CreateDefaultMicroConfig(int num_shards, int keys_per_shard = 100000) {
  janus::TxnGeneratorConfig cfg;
  cfg.shard_num = num_shards;
  cfg.key_num = keys_per_shard;
  cfg.read_ratio = 0.5; // 50% reads, 50% writes
  cfg.ops_per_txn = 10;
  return cfg;
}

inline janus::TxnGeneratorConfig
CreateDefaultTPCCConfig(int num_shards, int warehouses_per_shard = 1) {
  janus::TxnGeneratorConfig cfg;
  cfg.shard_num = num_shards;
  cfg.num_warehouses =
      warehouses_per_shard; // Note: assigning to num_warehouses
  cfg.num_districts_per_wh = 10;
  cfg.num_customers_per_district = 3000;
  cfg.num_items = 1000;  // Reduced for testing (was 100000)
  // Standard TPC-C mix
  cfg.new_order_weight = 45;
  cfg.payment_weight = 43;
  cfg.order_status_weight = 4;
  cfg.delivery_weight = 4;
  cfg.stock_level_weight = 4;
  return cfg;
}

} // namespace luigi
} // namespace mako
