#include "luigi_transport_setup.h"

#include "mako/lib/configuration.h"
#include "mako/benchmarks/benchmark_config.h"
#include "mako/benchmarks/rpc_setup.h"
#include "deptran/__dep__.h"

namespace janus {
namespace luigi {

// Static storage for Configuration to prevent dangling pointer
static transport::Configuration* g_luigi_yaml_config = nullptr;

// IMPORTANT NOTE:
// Luigi currently does NOT integrate with Mako's helper server architecture!
// Luigi benchmark is CLIENT-ONLY - it sends requests but doesn't handle incoming ones.
// For Luigi to work as both client and server (like Mako), it would need to:
// 1. Create helper threads that pull from HelperQueues
// 2. Have those threads call LuigiReceiver->ReceiveRequest()
// 
// Current setup only creates transports, which is sufficient for CLIENT-ONLY mode
// where Luigi processes only send requests and wait for responses.

bool setup_luigi_transport(const std::string &config_file,
                           const std::string &cluster, int shard_index,
                           int num_shards, int num_erpc_servers,
                           int warehouses) {

  Log_info("Luigi transport setup: config=%s, cluster=%s, shard=%d, "
           "num_shards=%d, erpc_servers=%d, warehouses=%d",
           config_file.c_str(), cluster.c_str(), shard_index, num_shards,
           num_erpc_servers, warehouses);

  try {
    // Get BenchmarkConfig singleton
    auto &cfg = BenchmarkConfig::getInstance();

    // Parse YAML config - store in static to prevent dangling pointer
    if (g_luigi_yaml_config) {
      delete g_luigi_yaml_config;
    }
    g_luigi_yaml_config = new transport::Configuration(config_file);

    // Populate BenchmarkConfig with Luigi's parameters
    cfg.setConfig(g_luigi_yaml_config);
    cfg.setShardConfigFile(config_file);  // Store path for later use
    cfg.setCluster(cluster);
    cfg.setShardIndex(shard_index);
    cfg.setNshards(num_shards);
    cfg.setNumErpcServer(num_erpc_servers);
    cfg.setScaleFactor(warehouses); // Warehouses per shard

    // Delegate to Mako's battle-tested setup function
    // This creates FastTransport threads but NO servers/receivers!
    Log_info("Calling mako::setup_erpc_server()...");
    mako::setup_erpc_server();

    Log_info("Luigi transport setup complete. Transports created: %zu",
             cfg.getServerTransports().size());
    Log_info("NOTE: This is CLIENT-ONLY mode - no servers created to handle incoming requests");

    return true;

  } catch (const std::exception &e) {
    Log_error("Luigi transport setup failed: %s", e.what());
    return false;
  }
}

void stop_luigi_transport() {
  Log_info("Stopping Luigi transport infrastructure...");
  mako::stop_erpc_server();
  
  // Clean up static config
  if (g_luigi_yaml_config) {
    delete g_luigi_yaml_config;
    g_luigi_yaml_config = nullptr;
  }
  
  Log_info("Luigi transport stopped.");
}

} // namespace luigi
} // namespace janus
