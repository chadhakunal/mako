#pragma once

#include <map>
#include <string>
#include <memory>
#include <vector>

#include "rusty/arc.hpp"

// Forward declarations
namespace rrr {
class Server;
class Client;
class PollThread;
}

namespace janus {

class SchedulerLuigi;
class LuigiLeaderServiceImpl;
class LuigiLeaderProxy;

/**
 * LuigiRpcSetup: Helper class to set up Luigi RPC infrastructure.
 * 
 * This handles:
 * 1. Creating and registering the LuigiLeaderService with the RPC server
 * 2. Creating LuigiLeaderProxy connections to other shard leaders
 * 
 * Usage:
 *   LuigiRpcSetup setup;
 *   setup.SetupService(rpc_server, scheduler);
 *   setup.ConnectToLeaders(shard_addresses, poll_thread, scheduler);
 */
class LuigiRpcSetup {
 public:
  LuigiRpcSetup();
  ~LuigiRpcSetup();
  
  /**
   * Register the LuigiLeaderService with an RPC server.
   * 
   * @param rpc_server The rrr::Server to register with
   * @param scheduler The Luigi scheduler that will handle RPC callbacks
   * @return true on success
   */
  bool SetupService(rrr::Server* rpc_server, SchedulerLuigi* scheduler);
  
  /**
   * Connect to other shard leaders and create proxies.
   * 
   * @param shard_addresses Map of shard_id -> "host:port" for other leaders
   * @param poll_thread The poll thread for async I/O (as Arc)
   * @param scheduler The scheduler to add proxies to
   * @return Number of successful connections
   */
  int ConnectToLeaders(
      const std::map<uint32_t, std::string>& shard_addresses,
      rusty::Arc<rrr::PollThread> poll_thread,
      SchedulerLuigi* scheduler);
  
  /**
   * Disconnect all proxies and clean up.
   */
  void Shutdown();
  
 private:
  LuigiLeaderServiceImpl* service_ = nullptr;
  std::vector<rusty::Arc<rrr::Client>> rpc_clients_;
  std::vector<LuigiLeaderProxy*> proxies_;
};

} // namespace janus
