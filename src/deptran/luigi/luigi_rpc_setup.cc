#include "luigi_rpc_setup.h"
#include "luigi_service.h"
#include "luigi_scheduler.h"

#include "deptran/__dep__.h"  // For logging
#include "deptran/rcc_rpc.h"  // For LuigiLeaderProxy

#include "rrr/rrr.hpp"

namespace janus {

LuigiRpcSetup::LuigiRpcSetup() {}

LuigiRpcSetup::~LuigiRpcSetup() {
  Shutdown();
}

bool LuigiRpcSetup::SetupService(rrr::Server* rpc_server, SchedulerLuigi* scheduler) {
  if (rpc_server == nullptr || scheduler == nullptr) {
    Log_error("LuigiRpcSetup::SetupService: null server or scheduler");
    return false;
  }
  
  if (service_ != nullptr) {
    Log_warn("LuigiRpcSetup::SetupService: service already set up");
    return true;
  }
  
  // Create the service implementation
  service_ = new LuigiLeaderServiceImpl(scheduler);
  
  // Register with the RPC server
  int ret = rpc_server->reg(service_);
  if (ret != 0) {
    Log_error("LuigiRpcSetup::SetupService: failed to register service (ret=%d)", ret);
    delete service_;
    service_ = nullptr;
    return false;
  }
  
  Log_info("Luigi RPC service registered successfully");
  return true;
}

int LuigiRpcSetup::ConnectToLeaders(
    const std::map<uint32_t, std::string>& shard_addresses,
    rusty::Arc<rrr::PollThread> poll_thread,
    SchedulerLuigi* scheduler) {
  
  if (!poll_thread || scheduler == nullptr) {
    Log_error("LuigiRpcSetup::ConnectToLeaders: null poll_thread or scheduler");
    return 0;
  }
  
  int connected = 0;
  
  for (const auto& [shard_id, addr] : shard_addresses) {
    Log_info("Luigi: connecting to shard %u at %s", shard_id, addr.c_str());
    
    // Create RPC client using the factory method
    auto client = rrr::Client::create(poll_thread);
    int ret = client->connect(addr.c_str());
    
    if (ret != 0) {
      Log_warn("Luigi: failed to connect to shard %u at %s (ret=%d)",
               shard_id, addr.c_str(), ret);
      client->close();
      continue;
    }
    
    // Create proxy - need to get raw pointer for proxy constructor
    auto* proxy = new LuigiLeaderProxy(const_cast<rrr::Client*>(client.get()));
    
    // Add to scheduler
    scheduler->AddLeaderProxy(shard_id, proxy);
    
    // Track for cleanup
    rpc_clients_.push_back(client);
    proxies_.push_back(proxy);
    
    Log_info("Luigi: connected to shard %u", shard_id);
    connected++;
  }
  
  Log_info("Luigi: connected to %d/%zu leader shards",
           connected, shard_addresses.size());
  
  return connected;
}

void LuigiRpcSetup::Shutdown() {
  // Clean up proxies (they don't own the clients)
  for (auto* proxy : proxies_) {
    delete proxy;
  }
  proxies_.clear();
  
  // Clean up RPC clients - Arc handles reference counting
  for (auto& client : rpc_clients_) {
    client->close();
  }
  rpc_clients_.clear();
  
  // Service is owned by the RPC server after registration
  // but we need to clean it up if never registered
  // For now, we assume the server takes ownership
  service_ = nullptr;
  
  Log_info("Luigi RPC shutdown complete");
}

} // namespace janus
