#include "luigi_transport_setup.h"

#include "mako/lib/configuration.h"
#include "mako/benchmarks/benchmark_config.h"
#include "mako/benchmarks/rpc_setup.h"
#include "deptran/__dep__.h"

namespace janus {
namespace luigi {

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

    // Parse YAML config first
    transport::Configuration yaml_config(config_file);

    // Populate BenchmarkConfig with Luigi's parameters
    cfg.setConfig(&yaml_config);
    cfg.setCluster(cluster);
    cfg.setShardIndex(shard_index);
    cfg.setNshards(num_shards);
    cfg.setNumErpcServer(num_erpc_servers);
    cfg.setScaleFactor(warehouses); // Warehouses per shard

    // Delegate to Mako's battle-tested setup function
    Log_info("Calling mako::setup_erpc_server()...");
    mako::setup_erpc_server();

    Log_info("Luigi transport setup complete. Transports created: %zu",
             cfg.getServerTransports().size());

    return true;

  } catch (const std::exception &e) {
    Log_error("Luigi transport setup failed: %s", e.what());
    return false;
  }
}

void stop_luigi_transport() {
  Log_info("Stopping Luigi transport infrastructure...");
  mako::stop_erpc_server();
  Log_info("Luigi transport stopped.");
}

} // namespace luigi
} // namespace janus
