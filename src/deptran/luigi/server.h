#pragma once

/**
 * LuigiServer: Main server class for Luigi protocol
 */

#include <memory>
#include <string>

namespace rrr {
class Server;
}

namespace janus {

class SchedulerLuigi;
class LuigiServiceImpl;
class LuigiCommo;
class LuigiStateMachine;

class LuigiServer {
public:
  explicit LuigiServer(int partition_id);
  ~LuigiServer();

  void Initialize();
  void StartListener(const std::string &bind_addr);
  void ConnectAndStart();
  void Stop();

  void SetStateMachine(std::shared_ptr<LuigiStateMachine> sm) {
    state_machine_ = sm;
  }

  SchedulerLuigi *GetScheduler() { return scheduler_; }
  LuigiCommo *GetCommo() { return commo_.get(); }
  uint32_t GetPartitionId() const { return partition_id_; }

private:
  uint32_t partition_id_;
  int shard_idx_;

  SchedulerLuigi *scheduler_ = nullptr;
  std::shared_ptr<LuigiStateMachine> state_machine_;
  std::shared_ptr<LuigiCommo> commo_;
  std::unique_ptr<LuigiServiceImpl> service_;
  std::unique_ptr<rrr::Server> rpc_server_;
};

} // namespace janus
