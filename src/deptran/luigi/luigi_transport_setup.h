#pragma once

#include <string>

/**
 * Luigi transport setup - delegates to Mako's eRPC setup
 * 
 * This creates FastTransport threads that listen for incoming RPC requests
 * and enqueue them into HelperQueues.
 * 
 * After calling this, you must call setup_luigi_helper() to create threads
 * that pull from those queues and handle the requests.
 */

namespace janus {
namespace luigi {

/**
 * Setup Luigi transport infrastructure (creates eRPC servers).
 * 
 * This creates:
 * - FastTransport threads (listen on network ports)
 * - HelperQueues (where incoming requests are enqueued)
 * 
 * After this, call setup_luigi_helper() to create request handlers.
 * 
 * @param config_file Path to shard configuration YAML
 * @param cluster Cluster role (e.g., "localhost", "p1", "p2")
 * @param shard_index Index of this shard
 * @param num_shards Total number of shards
 * @param num_erpc_servers Number of eRPC server threads to create
 * @param warehouses Scale factor (warehouses per shard)
 * @return true if successful, false otherwise
 */
bool setup_luigi_transport(const std::string &config_file,
                           const std::string &cluster, int shard_index,
                           int num_shards, int num_erpc_servers,
                           int warehouses);

/**
 * Stop all eRPC servers previously started by setup_luigi_transport().
 * Call this AFTER stop_luigi_helper().
 */
void stop_luigi_transport();

} // namespace luigi
} // namespace janus

// Helper functions (defined in luigi_helper.cc)
namespace janus {
class LuigiReceiver;
namespace luigi {

/**
 * Setup Luigi helper servers (request handlers).
 * Must be called AFTER setup_luigi_transport().
 */
void setup_luigi_helper(
    janus::LuigiReceiver* receiver,
    const std::string& config_file,
    int shard_idx);

/**
 * Stop Luigi helper servers.
 */
void stop_luigi_helper();

} // namespace luigi
} // namespace janus
