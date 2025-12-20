#include "luigi_owd.h"
#include "lib/configuration.h"
#include "lib/fasttransport.h"
#include "luigi_client.h"
#include "luigi_owd.h"

#include <algorithm>
#include <future>
#include <iostream>

namespace mako {
namespace luigi {

uint64_t getCurrentTimeMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

LuigiOWD::LuigiOWD()
    : num_shards_(0), local_shard_idx_(0), initialized_(false),
      running_(false) {}

LuigiOWD::~LuigiOWD() { stop(); }

LuigiOWD &LuigiOWD::getInstance() {
  static LuigiOWD instance;
  return instance;
}

void LuigiOWD::init(const std::string &config_file, const std::string &cluster,
                    int local_shard_idx, int num_shards) {
  if (initialized_.load()) {
    return; // Already initialized
  }

  config_file_ = config_file;
  cluster_ = cluster;
  local_shard_idx_ = local_shard_idx;
  num_shards_ = num_shards;

  // Initialize OWD table with default values
  {
    std::lock_guard<std::mutex> lock(owd_mutex_);
    for (int i = 0; i < num_shards_; i++) {
      if (i == local_shard_idx_) {
        owd_table_[i] = 0; // Local shard has zero OWD
      } else {
        owd_table_[i] = DEFAULT_INITIAL_OWD_MS;
      }
    }
  }

  // Create dedicated LuigiClient for OWD pings only if there are multiple
  // shards
  if (num_shards_ > 1) {
    try {
      // 1. Parse configuration
      transport_config_ =
          std::make_unique<transport::Configuration>(config_file_);

      // 2. Resolve local URI
      std::string local_uri =
          transport_config_
              ->shard(local_shard_idx_, mako::convertCluster(cluster_))
              .host;

      // 3. Create FastTransport
      // Note: nr_req_types=1, physPort=0, numa=0
      transport_ = new FastTransport(config_file_, local_uri, cluster_, 1, 0,
                                     0, // physPort
                                     0, // numa
                                     local_shard_idx_,
                                     -1 // partition id -1 for OWD service
      );

      // 4. Create LuigiClient
      luigi_client_ =
          std::make_unique<janus::LuigiClient>(config_file_, transport_,
                                               0 // client_id=0
          );
    } catch (const std::exception &e) {
      std::cerr << "[LuigiOWD] Failed to initialize client: " << e.what()
                << std::endl;
    }
  }

  initialized_.store(true);

  std::cout << "[LuigiOWD] Initialized for " << num_shards_
            << " shards (local=" << local_shard_idx_ << ")" << std::endl;
}

void LuigiOWD::start() {
  if (!initialized_.load()) {
    std::cerr << "[LuigiOWD] Cannot start - not initialized" << std::endl;
    return;
  }

  if (running_.load()) {
    return; // Already running
  }

  // DISABLED: OWD background ping thread disabled for now
  // Just return fixed 100ms delay from getMaxOWD() instead
  std::cout << "[LuigiOWD] Background ping thread DISABLED - using fixed 100ms delay" << std::endl;
  running_.store(true);  // Set to true so stop() works properly
  return;

  // Original ping thread (disabled):
  /*
  running_.store(true);
  ping_thread_ = std::thread(&LuigiOWD::pingLoop, this);

  std::cout << "[LuigiOWD] Started background ping thread (interval="
            << PING_INTERVAL_MS << "ms)" << std::endl;
  */
}

void LuigiOWD::stop() {
  if (!running_.load()) {
    return;
  }

  running_.store(false);

  if (ping_thread_.joinable()) {
    ping_thread_.join();
  }

  std::cout << "[LuigiOWD] Stopped background ping thread" << std::endl;
}

void LuigiOWD::pingLoop() {
  while (running_.load()) {
    // Ping all remote shards
    for (int shard_idx = 0; shard_idx < num_shards_; shard_idx++) {
      if (shard_idx == local_shard_idx_) {
        continue; // Skip local shard
      }

      if (!running_.load()) {
        break;
      }

      pingShard(shard_idx);
    }

    // Sleep for ping interval
    std::this_thread::sleep_for(std::chrono::milliseconds(PING_INTERVAL_MS));
  }
}

void LuigiOWD::pingShard(int shard_idx) {
  if (!luigi_client_) {
    return;
  }

  // Record start time
  auto start_time = std::chrono::steady_clock::now();

  // Create promise for synchronous wait
  auto promise = std::make_shared<std::promise<bool>>();
  auto future = promise->get_future();

  // Ping via LuigiClient
  luigi_client_->InvokeOwdPing(
      0, // txn_nr (irrelevant for ping)
      shard_idx, [promise](char *respBuf) { promise->set_value(true); },
      [promise]() { promise->set_value(false); },
      PING_INTERVAL_MS * 5 // Timeout (5x interval)
  );

  // Wait for result
  bool success = false;
  if (future.wait_for(std::chrono::milliseconds(PING_INTERVAL_MS * 5)) ==
      std::future_status::ready) {
    success = future.get();
  }

  // Record end time
  auto end_time = std::chrono::steady_clock::now();

  // Only update OWD if ping succeeded
  if (success) {
    // Calculate RTT in milliseconds
    uint64_t rtt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          end_time - start_time)
                          .count();

    updateOWD(shard_idx, rtt_ms);
  }
}

uint64_t LuigiOWD::getOWD(int shard_idx) const {
  std::lock_guard<std::mutex> lock(owd_mutex_);
  auto it = owd_table_.find(shard_idx);
  if (it != owd_table_.end()) {
    return it->second;
  }
  return DEFAULT_INITIAL_OWD_MS;
}

uint64_t LuigiOWD::getMaxOWD(const std::vector<int> &shard_indices) const {
  // If a fixed OWD is configured (e.g., via --owd-ms for geo-distributed testing),
  // return that value for all remote shards
  if (fixed_owd_ms_ > 0) {
    // Check if any of the requested shards is remote
    for (int shard_idx : shard_indices) {
      if (shard_idx != local_shard_idx_) {
        return fixed_owd_ms_;
      }
    }
    return 0;  // All local shards
  }

  // Default: return 1ms for localhost testing (OWD pinging disabled)
  // For production, would use the commented-out code below with actual measurements
  return 1;

  // Original implementation (disabled - requires working OWD ping thread):
  // uint64_t max_owd = 0;
  // std::lock_guard<std::mutex> lock(owd_mutex_);
  //
  // for (int shard_idx : shard_indices) {
  //   auto it = owd_table_.find(shard_idx);
  //   if (it != owd_table_.end()) {
  //     max_owd = std::max(max_owd, it->second);
  //   } else {
  //     max_owd = std::max(max_owd, DEFAULT_INITIAL_OWD_MS);
  //   }
  // }
  //
  // return max_owd;
}

uint64_t
LuigiOWD::getExpectedTimestamp(const std::vector<int> &involved_shards) const {
  uint64_t max_owd = getMaxOWD(involved_shards);
  return getCurrentTimeMillis() + max_owd + HEADROOM_MS;
}

uint64_t LuigiOWD::getExpectedTimestamp(uint64_t shard_bitmask) const {
  std::vector<int> involved_shards;
  for (int i = 0; i < 64 && i < num_shards_; i++) {
    if (shard_bitmask & (1ULL << i)) {
      involved_shards.push_back(i);
    }
  }
  return getExpectedTimestamp(involved_shards);
}

void LuigiOWD::updateOWD(int shard_idx, uint64_t rtt_ms) {
  if (shard_idx == local_shard_idx_) {
    return; // Local shard always has OWD = 0
  }

  // OWD is estimated as RTT / 2
  uint64_t owd_ms = rtt_ms / 2;

  // Minimum OWD of 1ms
  if (owd_ms < 1) {
    owd_ms = 1;
  }

  std::lock_guard<std::mutex> lock(owd_mutex_);

  // Exponential moving average: new = 0.8 * old + 0.2 * measured
  auto it = owd_table_.find(shard_idx);
  if (it != owd_table_.end()) {
    it->second = (it->second * 8 + owd_ms * 2) / 10;
  } else {
    owd_table_[shard_idx] = owd_ms;
  }
}

std::vector<int> LuigiOWD::getRemoteShards() const {
  std::vector<int> remote_shards;
  for (int i = 0; i < num_shards_; i++) {
    if (i != local_shard_idx_) {
      remote_shards.push_back(i);
    }
  }
  return remote_shards;
}

} // namespace luigi
} // namespace mako
