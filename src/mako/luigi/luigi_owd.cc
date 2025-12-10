#include "luigi_owd.h"
#include "lib/shardClient.h"

#include <algorithm>
#include <iostream>

namespace mako {
namespace luigi {

uint64_t getCurrentTimeMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

LuigiOWD::LuigiOWD()
    : num_shards_(0)
    , local_shard_idx_(0)
    , initialized_(false)
    , running_(false)
{
}

LuigiOWD::~LuigiOWD() {
    stop();
}

LuigiOWD& LuigiOWD::getInstance() {
    static LuigiOWD instance;
    return instance;
}

void LuigiOWD::init(const std::string& config_file,
                    const std::string& cluster,
                    int local_shard_idx,
                    int num_shards) {
    if (initialized_.load()) {
        return;  // Already initialized
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
                owd_table_[i] = 0;  // Local shard has zero OWD
            } else {
                owd_table_[i] = DEFAULT_INITIAL_OWD_MS;
            }
        }
    }

    // Create dedicated ShardClient for OWD pings
    // Use partition ID -1 to distinguish from worker threads
    shard_client_ = std::make_unique<ShardClient>(
        config_file_, cluster_, local_shard_idx_, -1);

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
        return;  // Already running
    }

    running_.store(true);
    ping_thread_ = std::thread(&LuigiOWD::pingLoop, this);
    
    std::cout << "[LuigiOWD] Started background ping thread (interval=" 
              << PING_INTERVAL_MS << "ms)" << std::endl;
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
                continue;  // Skip local shard
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
    if (!shard_client_) {
        return;
    }

    // Record start time
    auto start_time = std::chrono::steady_clock::now();
    
    // Ping using warmup mechanism
    int result = shard_client_->checkRemoteShardReady(shard_idx);
    
    // Record end time
    auto end_time = std::chrono::steady_clock::now();
    
    // Calculate RTT in milliseconds
    uint64_t rtt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time
    ).count();
    
    // Only update OWD if ping succeeded
    if (result == 0) {  // ErrorCode::SUCCESS
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

uint64_t LuigiOWD::getMaxOWD(const std::vector<int>& shard_indices) const {
    uint64_t max_owd = 0;
    std::lock_guard<std::mutex> lock(owd_mutex_);
    
    for (int shard_idx : shard_indices) {
        auto it = owd_table_.find(shard_idx);
        if (it != owd_table_.end()) {
            max_owd = std::max(max_owd, it->second);
        } else {
            max_owd = std::max(max_owd, DEFAULT_INITIAL_OWD_MS);
        }
    }
    
    return max_owd;
}

uint64_t LuigiOWD::getExpectedTimestamp(const std::vector<int>& involved_shards) const {
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
        return;  // Local shard always has OWD = 0
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
