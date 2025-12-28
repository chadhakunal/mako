/**
 * @file coordinator.cc
 * @brief Luigi Coordinator (benchmark client) with integrated OWD measurement
 *
 * Usage:
 *   ./luigi_coordinator -f config.yml -P c0 -b micro -d 30
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <getopt.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <signal.h>
#include <string>
#include <thread>
#include <vector>

#include "../__dep__.h"
#include "../config.h"
#include "commo.h"
#include "luigi.h" // For LuigiService base class
#include "luigi_entry.h"
#include "micro_txn_generator.h"
#include "rrr.hpp" // For rrr::Server
#include "tpcc_txn_generator.h"
#include "txn_generator.h"

namespace janus {
namespace luigi {

//=============================================================================
// Helpers
//=============================================================================

static uint64_t GetTimeMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

static uint64_t GetTimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

static std::string SerializeOps(const std::vector<LuigiOp> &ops) {
  std::string data;
  for (const auto &op : ops) {
    data += static_cast<char>(op.op_type);
    uint32_t key_len = op.key.size();
    data.append(reinterpret_cast<const char *>(&key_len), sizeof(key_len));
    data += op.key;
    uint32_t val_len = op.value.size();
    data.append(reinterpret_cast<const char *>(&val_len), sizeof(val_len));
    data += op.value;
  }
  return data;
}

//=============================================================================
// Benchmark Statistics
//=============================================================================

struct BenchmarkStats {
  uint64_t total_txns = 0;
  uint64_t committed_txns = 0;
  uint64_t aborted_txns = 0;
  double throughput_tps = 0.0;
  double avg_latency_us = 0.0;
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
  }
};

struct TxnRecord {
  uint64_t txn_id;
  uint64_t start_time_us;
  uint64_t end_time_us;
  bool committed;
  int txn_type;
};

// In-flight transaction state for async dispatch
struct InFlightTxn {
  uint64_t txn_id;
  uint64_t start_time_us;
  int txn_type;
  int tid; // Thread ID for stats recording
  std::atomic<size_t> pending_shards{0};
  std::atomic<bool> all_ok{true};
};

// FNV-1a hash function (matches generator's KeyToShard)
static uint32_t HashFNV1a(const std::string &key) {
  constexpr uint64_t fnv_offset = 14695981039346656037ULL;
  constexpr uint64_t fnv_prime = 1099511628211ULL;
  uint64_t hash = fnv_offset;
  for (char c : key) {
    hash ^= static_cast<uint8_t>(c);
    hash *= fnv_prime;
  }
  return static_cast<uint32_t>(hash);
}

//=============================================================================
// LuigiCoordinator - Benchmark Client with OWD Measurement
//=============================================================================

class LuigiCoordinator {
public:
  enum class BenchmarkType { BM_MICRO, BM_MICRO_SINGLE, BM_TPCC };

  // OWD constants
  static constexpr uint64_t OWD_PING_INTERVAL_MS = 100;

  // Flow control: limit in-flight transactions to avoid coroutine exhaustion
  // Lower values = lower latency but potentially lower throughput
  // Scale with number of threads: ~200 per thread
  static constexpr uint64_t MAX_IN_FLIGHT_PER_THREAD = 200;

  struct Config {
    int shard_index = 0;
    int num_shards = 1;
    int num_threads = 1;
    int duration_sec = 30;
    TxnGeneratorConfig gen_config;
    uint32_t worker_id_base = 0;
    uint64_t owd_default_ms = 50;
    uint64_t owd_headroom_ms = 10;
  };

  LuigiCoordinator(const Config &config) : config_(config) {}

  ~LuigiCoordinator() {
    Stop();
    StopOwdThread();
  }

  bool Initialize(std::shared_ptr<LuigiCommo> commo) {
    commo_ = commo;
    if (!commo_) {
      Log_error("LuigiCoordinator: commo is null");
      return false;
    }

    // Initialize OWD table
    for (int i = 0; i < config_.num_shards; i++) {
      owd_table_[i] = (i == 0) ? 0 : config_.owd_default_ms;
    }

    thread_stats_.resize(config_.num_threads);
    Log_info("LuigiCoordinator initialized: %d shards, %d threads",
             config_.num_shards, config_.num_threads);
    return true;
  }

  void StartOwdThread() {
    if (owd_running_.load())
      return;
    owd_running_.store(true);
    owd_thread_ = std::thread(&LuigiCoordinator::OwdLoop, this);
    Log_info("OWD measurement thread started");
  }

  void StopOwdThread() {
    if (!owd_running_.load())
      return;
    owd_running_.store(false);
    if (owd_thread_.joinable())
      owd_thread_.join();
  }

  BenchmarkStats RunBenchmark(BenchmarkType type) {
    current_type_ = type;

    {
      std::lock_guard<std::mutex> lock(gen_mutex_);
      switch (type) {
      case BenchmarkType::BM_MICRO:
      case BenchmarkType::BM_MICRO_SINGLE:
        generator_ = std::make_unique<MicroTxnGenerator>(config_.gen_config);
        break;
      case BenchmarkType::BM_TPCC:
        generator_ = std::make_unique<TPCCTxnGenerator>(config_.gen_config);
        break;
      }
    }

    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      for (size_t i = 0; i < thread_stats_.size(); i++) {
        thread_stats_[i].records.clear();
        thread_stats_[i].committed = thread_stats_[i].aborted = 0;
      }
    }
    next_txn_id_.store(1);
    dispatched_txns_.store(0);
    completed_txns_.store(0);

    running_.store(true);
    start_time_us_ = GetTimestampUs();

    std::vector<std::thread> workers;
    for (int i = 0; i < config_.num_threads; i++) {
      workers.emplace_back(&LuigiCoordinator::WorkerThread, this, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(config_.duration_sec));

    running_.store(false);
    for (auto &w : workers)
      w.join();

    // Wait for all in-flight transactions to complete (with timeout)
    {
      std::unique_lock<std::mutex> lock(completion_mutex_);
      auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(10);
      Log_info("Waiting for completion: dispatched=%lu, completed=%lu",
               dispatched_txns_.load(), completed_txns_.load());
      while (completed_txns_.load() < dispatched_txns_.load()) {
        if (completion_cv_.wait_until(lock, deadline) ==
            std::cv_status::timeout) {
          Log_warn(
              "Timeout waiting for in-flight transactions: %lu/%lu completed",
              completed_txns_.load(), dispatched_txns_.load());
          break;
        }
      }
    }

    end_time_us_ = GetTimestampUs();
    Log_info("Benchmark complete: dispatched=%lu, completed=%lu",
             dispatched_txns_.load(), completed_txns_.load());

    // Log cross-shard transaction statistics for TPC-C
    if (current_type_ == BenchmarkType::BM_TPCC) {
      uint64_t single_shard = single_shard_txns_.load();
      uint64_t cross_shard = cross_shard_txns_.load();
      uint64_t total = single_shard + cross_shard;
      if (total > 0) {
        double cross_pct = (100.0 * cross_shard) / total;
        std::cerr << "\n=== TPC-C Cross-Shard Statistics ===" << std::endl;
        std::cerr << "Single-shard txns: " << single_shard << std::endl;
        std::cerr << "Cross-shard txns:  " << cross_shard << std::endl;
        std::cerr << "Cross-shard %:     " << std::fixed << std::setprecision(1)
                  << cross_pct << "%" << std::endl;
        std::cerr << "===================================\n" << std::endl;
      }
    }

    return CalculateStats();
  }

  void Stop() { running_.store(false); }

private:
  //=========================================================================
  // OWD Measurement
  //=========================================================================

  void OwdLoop() {
    while (owd_running_.load()) {
      for (int i = 0; i < config_.num_shards; i++) {
        if (!owd_running_.load())
          break;
        if (i == 0)
          continue; // Skip local shard
        PingShard(i);
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(OWD_PING_INTERVAL_MS));
    }
  }

  void PingShard(int shard_idx) {
    uint64_t send_time = GetTimeMillis();

    commo_->OwdPingAsync(
        shard_idx, send_time,
        [this, shard_idx, send_time](bool ok, rrr::i32 status) {
          if (ok) {
            uint64_t rtt = GetTimeMillis() - send_time;
            uint64_t owd = rtt / 2;

            std::lock_guard<std::mutex> lock(owd_mutex_);
            // Exponential moving average (alpha=0.3)
            owd_table_[shard_idx] =
                static_cast<uint64_t>(0.7 * owd_table_[shard_idx] + 0.3 * owd);
          }
        });
  }

  uint64_t GetMaxOwd(const std::vector<uint32_t> &shards) {
    std::lock_guard<std::mutex> lock(owd_mutex_);
    uint64_t max_owd = 0;
    for (uint32_t s : shards) {
      auto it = owd_table_.find(s);
      uint64_t owd =
          (it != owd_table_.end()) ? it->second : config_.owd_default_ms;
      max_owd = std::max(max_owd, owd);
    }
    return max_owd;
  }

  uint64_t GetExpectedTimestamp(const std::vector<uint32_t> &shards) {
    return GetTimeMillis() + GetMaxOwd(shards) + config_.owd_headroom_ms;
  }

  //=========================================================================
  // Benchmark Worker
  //=========================================================================

  void WorkerThread(int tid) {
    // Calculate max in-flight based on number of threads
    uint64_t max_in_flight = MAX_IN_FLIGHT_PER_THREAD * config_.num_threads;

    while (running_.load()) {
      // Flow control: wait if too many transactions in flight
      uint64_t in_flight = dispatched_txns_.load() - completed_txns_.load();
      if (in_flight >= max_in_flight) {
        // Wait for some transactions to complete
        std::unique_lock<std::mutex> lock(completion_mutex_);
        completion_cv_.wait_for(
            lock, std::chrono::milliseconds(1), [this, max_in_flight]() {
              return (dispatched_txns_.load() - completed_txns_.load()) <
                     max_in_flight;
            });
        continue;
      }

      if (!DispatchOne(tid)) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }
  }

  bool DispatchOne(int tid) {
    LuigiTxnRequest req;
    {
      std::lock_guard<std::mutex> lock(gen_mutex_);
      if (!generator_)
        return false;
      uint64_t id = next_txn_id_.fetch_add(1);
      req.txn_id = (static_cast<uint64_t>(tid) << 48) | id;
      generator_->GetTxnReq(&req, static_cast<uint32_t>(req.txn_id), tid);
    }

    uint32_t worker_id = config_.worker_id_base + tid;
    uint64_t start = GetTimestampUs();

    // Helper to extract warehouse ID from TPC-C keys
    // TPC-C keys format: wh_W, dist_W_D, cust_W_D_C, stock_W_I, etc.
    auto ExtractWarehouseId = [](const std::string &key) -> int {
      size_t pos = key.find('_');
      if (pos == std::string::npos)
        return -1;

      size_t start = pos + 1;
      size_t end = key.find('_', start);
      if (end == std::string::npos)
        end = key.length();

      try {
        return std::stoi(key.substr(start, end - start));
      } catch (...) {
        return -1;
      }
    };

    // Determine involved shards
    std::vector<uint32_t> shards;
    std::string debug_info;
    for (const auto &op : req.ops) {
      uint32_t s;

      // Use warehouse-based sharding for TPC-C to preserve locality
      if (current_type_ == BenchmarkType::BM_TPCC) {
        int w_id = ExtractWarehouseId(op.key);
        if (w_id >= 0 && config_.num_shards > 1) {
          s = w_id % config_.num_shards;
        } else {
          s = 0; // Fallback for non-TPC-C keys or single shard
        }
      } else {
        // Use FNV1a hash for micro benchmarks
        s = config_.num_shards > 1 ? HashFNV1a(op.key) % config_.num_shards : 0;
      }

      debug_info += op.key + "->s" + std::to_string(s) + " ";
      if (std::find(shards.begin(), shards.end(), s) == shards.end())
        shards.push_back(s);
    }

    if (req.ops.size() > 0) {
      Log_debug("DispatchOne: txn=%lu ops=%zu shards=%zu keys=[%s]", req.txn_id,
                req.ops.size(), shards.size(), debug_info.c_str());
    }

    // NOTE: Removed BM_MICRO_SINGLE override to enable multi-shard watermark
    // testing The BM_MICRO benchmark should allow transactions to span multiple
    // shards if (current_type_ == BenchmarkType::BM_MICRO_SINGLE) {
    //   shards = {static_cast<uint32_t>(config_.shard_index)};
    // }

    uint64_t expected = GetExpectedTimestamp(shards);

    std::string ops_data = SerializeOps(req.ops);
    std::vector<rrr::i32> involved_i32(shards.begin(), shards.end());

    // Create in-flight transaction state with shared_ptr for callback lifetime
    auto in_flight = std::make_shared<InFlightTxn>();
    in_flight->txn_id = req.txn_id;
    in_flight->start_time_us = start;
    in_flight->txn_type = req.txn_type;
    in_flight->tid = tid;
    in_flight->pending_shards.store(shards.size());
    in_flight->all_ok.store(true);

    dispatched_txns_.fetch_add(1);

    // Track cross-shard statistics for TPC-C
    if (current_type_ == BenchmarkType::BM_TPCC) {
      if (shards.size() == 1) {
        single_shard_txns_.fetch_add(1);
      } else {
        cross_shard_txns_.fetch_add(1);
      }
    }

    // Dispatch to all shards asynchronously (non-blocking)
    for (uint32_t shard : shards) {
      // Capture variables needed for watermark-based commit
      commo_->DispatchAsync(
          shard, req.txn_id, expected, worker_id, involved_i32, ops_data,
          [this, in_flight, shard, expected, worker_id,
           shards](bool ok, rrr::i32 status, rrr::i64 commit_ts,
                   std::string results) {
            // RPC callback - runs on reactor thread
            Log_debug("Dispatch callback: txn=%lu shard=%u ok=%d status=%d "
                      "pending=%zu",
                      in_flight->txn_id, shard, ok, status,
                      in_flight->pending_shards.load());

            if (!ok || status != 0) {
              in_flight->all_ok.store(false);
            }

            // Decrement pending count; if this is the last shard, check
            // watermarks
            if (in_flight->pending_shards.fetch_sub(1) == 1) {
              // All shards have responded
              bool execution_ok = in_flight->all_ok.load();

              if (!execution_ok) {
                // Transaction failed - abort immediately
                CompleteTransaction(in_flight, false);
                return;
              }

              // Check if we can commit based on watermarks
              // Use 'expected' as the timestamp (this is when the txn was
              // scheduled)
              if (CanCommit(expected, worker_id, shards)) {
                // Watermarks already advanced - commit immediately
                Log_debug(
                    "Coordinator: txn %lu commit immediately (watermarks ok)",
                    in_flight->txn_id);
                CompleteTransaction(in_flight, true);
              } else {
                // Add to pending commits - will commit when watermarks advance
                Log_debug("Coordinator: txn %lu waiting for watermarks "
                          "(expected=%lu)",
                          in_flight->txn_id, expected);

                std::lock_guard<std::mutex> lock(pending_mutex_);
                PendingCommit pending;
                pending.txn_id = in_flight->txn_id;
                pending.timestamp = expected;
                pending.worker_id = worker_id;
                pending.involved_shards = shards;
                pending.in_flight = in_flight;
                pending_commits_[in_flight->txn_id] = pending;
              }
            }
          });
    }

    // Return immediately - don't wait for responses
    return true;
  }

  BenchmarkStats CalculateStats() {
    BenchmarkStats stats;
    std::vector<uint64_t> latencies;

    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      for (size_t i = 0; i < thread_stats_.size(); i++) {
        stats.committed_txns += thread_stats_[i].committed;
        stats.aborted_txns += thread_stats_[i].aborted;
        for (const auto &r : thread_stats_[i].records)
          latencies.push_back(r.end_time_us - r.start_time_us);
      }
    }

    stats.total_txns = stats.committed_txns + stats.aborted_txns;
    stats.duration_ms = (end_time_us_ - start_time_us_) / 1000;

    if (stats.duration_ms > 0)
      stats.throughput_tps = 1000.0 * stats.committed_txns / stats.duration_ms;

    if (!latencies.empty()) {
      std::sort(latencies.begin(), latencies.end());
      stats.avg_latency_us =
          std::accumulate(latencies.begin(), latencies.end(), 0.0) /
          latencies.size();
      size_t n = latencies.size();
      stats.p50_latency_us = latencies[n * 50 / 100];
      stats.p99_latency_us = latencies[n * 99 / 100];
      stats.p999_latency_us = latencies[std::min(n - 1, n * 999 / 1000)];
    }

    return stats;
  }

  // Configuration
  Config config_;
  std::unique_ptr<LuigiTxnGenerator> generator_;
  std::shared_ptr<LuigiCommo> commo_;

  // OWD state
  std::atomic<bool> owd_running_{false};
  std::thread owd_thread_;
  mutable std::mutex owd_mutex_;
  std::map<int, uint64_t> owd_table_;

  // Benchmark state
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> next_txn_id_{1};
  BenchmarkType current_type_;

  struct ThreadStats {
    std::vector<TxnRecord> records;
    uint64_t committed = 0, aborted = 0;
    // Removed per-thread mutex - now using shared stats_mutex_
  };
  std::vector<ThreadStats> thread_stats_;
  uint64_t start_time_us_ = 0, end_time_us_ = 0;
  std::mutex gen_mutex_;
  std::mutex
      stats_mutex_; // Shared mutex for all thread stats (fixes double-lock bug)

  // Async dispatch tracking
  std::atomic<uint64_t> dispatched_txns_{0};
  std::atomic<uint64_t> completed_txns_{0};
  std::atomic<uint64_t> single_shard_txns_{
      0}; // Track single-shard txns for TPC-C
  std::atomic<uint64_t> cross_shard_txns_{
      0}; // Track cross-shard txns for TPC-C
  std::condition_variable completion_cv_;
  std::mutex completion_mutex_;

  //=========================================================================
  // Watermark Tracking (for commit decisions per Luigi protocol)
  //=========================================================================

  // Watermarks from each shard: watermarks_[shard_id][worker_id] = timestamp
  std::map<uint32_t, std::vector<uint64_t>> remote_watermarks_;
  std::mutex watermark_mutex_;

  // Pending commits waiting for watermarks to advance
  struct PendingCommit {
    uint64_t txn_id;
    uint64_t timestamp; // Transaction agreed timestamp
    uint32_t worker_id;
    std::vector<uint32_t> involved_shards;
    std::shared_ptr<InFlightTxn> in_flight; // For stats recording
  };
  std::map<uint64_t, PendingCommit> pending_commits_;
  std::mutex pending_mutex_;

public:
  //=========================================================================
  // Watermark Management (called from RPC handler)
  //=========================================================================

  void OnWatermarkUpdate(uint32_t shard_id,
                         const std::vector<int64_t> &watermarks) {
    {
      std::lock_guard<std::mutex> lock(watermark_mutex_);
      remote_watermarks_[shard_id].clear();
      for (int64_t wm : watermarks) {
        remote_watermarks_[shard_id].push_back(static_cast<uint64_t>(wm));
      }
      Log_debug("Coordinator: updated watermarks from shard %u (size=%zu)",
                shard_id, watermarks.size());
    }

    // Check if any pending commits can now proceed
    CheckPendingCommits();
  }

  bool CanCommit(uint64_t timestamp, uint32_t worker_id,
                 const std::vector<uint32_t> &involved_shards) {
    std::lock_guard<std::mutex> lock(watermark_mutex_);

    // Development mode: if no watermarks received yet from any shard,
    // allow immediate commit (matches previous behavior)
    // In production, this should be disabled after watermark wiring is complete
    bool any_watermarks_received = false;
    for (uint32_t shard : involved_shards) {
      if (remote_watermarks_.find(shard) != remote_watermarks_.end() &&
          !remote_watermarks_[shard].empty()) {
        any_watermarks_received = true;
        break;
      }
    }

    if (!any_watermarks_received) {
      // No watermarks received yet - allow immediate commit (dev mode)
      Log_debug("Coordinator: CanCommit dev mode - no watermarks received, "
                "allowing commit");
      return true;
    }

    // Check: timestamp <= watermark[shard][worker] for ALL involved shards
    for (uint32_t shard : involved_shards) {
      auto it = remote_watermarks_.find(shard);
      if (it == remote_watermarks_.end()) {
        // No watermarks received from this shard yet
        return false;
      }

      if (worker_id >= it->second.size()) {
        // Worker watermark not available
        return false;
      }

      if (timestamp > it->second[worker_id]) {
        // Watermark hasn't advanced past this timestamp
        return false;
      }
    }

    return true; // All shards have advanced past this timestamp
  }

  void CheckPendingCommits() {
    std::lock_guard<std::mutex> lock(pending_mutex_);

    auto it = pending_commits_.begin();
    while (it != pending_commits_.end()) {
      auto &[txn_id, info] = *it;

      if (CanCommit(info.timestamp, info.worker_id, info.involved_shards)) {
        Log_debug("Coordinator: txn %lu can now commit (watermarks advanced)",
                  txn_id);

        // Record stats and complete
        CompleteTransaction(info.in_flight, true);
        it = pending_commits_.erase(it);
      } else {
        ++it;
      }
    }
  }

private:
  void CompleteTransaction(std::shared_ptr<InFlightTxn> in_flight,
                           bool committed) {
    uint64_t end = GetTimestampUs();

    // Record stats (using shared mutex to prevent double-lock bug)
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      TxnRecord record;
      record.txn_id = in_flight->txn_id;
      record.start_time_us = in_flight->start_time_us;
      record.end_time_us = end;
      record.committed = committed;
      record.txn_type = in_flight->txn_type;
      thread_stats_[in_flight->tid].records.push_back(record);
      if (committed)
        thread_stats_[in_flight->tid].committed++;
      else
        thread_stats_[in_flight->tid].aborted++;
    }

    // Signal completion for benchmark end waiting
    completed_txns_.fetch_add(1);
    Log_debug("Transaction complete: txn=%lu completed=%lu dispatched=%lu",
              in_flight->txn_id, completed_txns_.load(),
              dispatched_txns_.load());
    completion_cv_.notify_all();
  }
};

} // namespace luigi
} // namespace janus

//=============================================================================
// Main
//=============================================================================

using namespace std;
using namespace janus;
using namespace janus::luigi;

static volatile bool g_running = true;
static void signal_handler(int) { g_running = false; }

static void print_usage(const char *prog) {
  cerr << "Usage: " << prog << " -f config.yml [options]\n"
       << "  -f FILE   Config file (required)\n"
       << "  -b TYPE   Benchmark: micro, micro_single, tpcc (default: micro)\n"
       << "  -t N      Worker threads (default: 1)\n"
       << "  -d SEC    Duration (default: 30)\n"
       << "  -w MS     OWD default in milliseconds (default: 50)\n"
       << "  -x MS     OWD headroom in milliseconds (default: 10)\n"
       << "  -r PCT    Remote item percentage for cross-shard transactions "
          "(default: 1)\n";
}

int main(int argc, char **argv) {
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  string benchmark_type = "micro";
  int num_threads = 1;
  int duration_sec = 30;
  uint64_t owd_default_ms = 50;
  uint64_t owd_headroom_ms = 10;
  uint32_t remote_item_pct = 1; // Cross-shard transaction percentage

  int opt;
  while ((opt = getopt(argc, argv, "f:P:b:t:d:w:x:r:h")) != -1) {
    switch (opt) {
    case 'f':
    case 'P':
      break;
    case 'b':
      benchmark_type = optarg;
      break;
    case 't':
      num_threads = atoi(optarg);
      break;
    case 'd':
      duration_sec = atoi(optarg);
      break;
    case 'w':
      owd_default_ms = atoi(optarg);
      break;
    case 'x':
      owd_headroom_ms = atoi(optarg);
      break;
    case 'r':
      remote_item_pct = atoi(optarg);
      break;
    case 'h':
      print_usage(argv[0]);
      return 0;
    default:
      print_usage(argv[0]);
      return 1;
    }
  }

  optind = 1;
  if (Config::CreateConfig(argc, argv) != 0) {
    cerr << "Error: Failed to load config\n";
    return 1;
  }

  auto config = Config::GetConfig();
  int num_shards = config->GetNumPartition();

  cout << "=== Luigi Coordinator ===\n"
       << "Shards: " << num_shards << ", Threads: " << num_threads
       << ", Duration: " << duration_sec << "s, Benchmark: " << benchmark_type
       << "\n\n";

  // Create a PollThread for RPC callbacks (required for async operations)
  auto poll = rrr::PollThread::create();
  auto commo = make_shared<LuigiCommo>(rusty::Some(poll));

  LuigiCoordinator::Config coord_config;
  coord_config.num_shards = num_shards;
  coord_config.num_threads = num_threads;
  coord_config.duration_sec = duration_sec;
  coord_config.owd_default_ms = owd_default_ms;
  coord_config.owd_headroom_ms = owd_headroom_ms;
  coord_config.gen_config.shard_num = num_shards;
  coord_config.gen_config.key_num = 100000;
  coord_config.gen_config.read_ratio = 0.5;
  coord_config.gen_config.ops_per_txn = 10;
  coord_config.gen_config.num_warehouses =
      num_threads * num_shards; // Scale warehouses with threads (like Mako)
  coord_config.gen_config.remote_item_pct = remote_item_pct;

  LuigiCoordinator coordinator(coord_config);
  if (!coordinator.Initialize(commo)) {
    cerr << "Failed to initialize coordinator\n";
    return 1;
  }

  coordinator.StartOwdThread();

  BenchmarkStats stats;
  if (benchmark_type == "micro") {
    stats = coordinator.RunBenchmark(LuigiCoordinator::BenchmarkType::BM_MICRO);
  } else if (benchmark_type == "micro_single") {
    stats = coordinator.RunBenchmark(
        LuigiCoordinator::BenchmarkType::BM_MICRO_SINGLE);
  } else if (benchmark_type == "tpcc") {
    stats = coordinator.RunBenchmark(LuigiCoordinator::BenchmarkType::BM_TPCC);
  } else {
    cerr << "Unknown benchmark: " << benchmark_type << "\n";
    return 1;
  }

  cerr << "DEBUG: About to print stats, total_txns=" << stats.total_txns
       << " committed=" << stats.committed_txns << "\n";
  stats.Print();
  return 0;
}
