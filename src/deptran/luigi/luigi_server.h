#pragma once

/**
 * LuigiServer: Standalone server for Luigi timestamp-ordered execution
 * protocol.
 *
 * This is a complete separation from Mako's ShardReceiver, handling only
 * Luigi-specific request types while using Mako's eRPC transport.
 */

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "lib/transport.h"
#include "lib/configuration.h"

#include "luigi_common.h"
#include "luigi_entry.h"

// Forward declarations
namespace rrr {
class Server;
class PollThread;
} // namespace rrr
namespace rusty {
template <typename T> class Arc;
}
namespace mako {
class HelperQueue;
}

namespace janus {

class SchedulerLuigi;

/**
 * LuigiReceiver: Handles incoming Luigi protocol requests.
 *
 * Implements TransportReceiver interface for Mako's eRPC transport.
 * Manages Luigi scheduler lifecycle and request dispatching.
 */
class LuigiReceiver : public TransportReceiver {
public:
  explicit LuigiReceiver(const std::string &config_file);
  ~LuigiReceiver();

  //=========================================================================
  // Registration and Setup
  //=========================================================================

  //=========================================================================
  // TransportReceiver Interface
  //=========================================================================

  size_t ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) override;
  void ReceiveResponse(uint8_t reqType, char *respBuf) override {}
  bool Blocked() override { return false; }

  //=========================================================================
  // Luigi Scheduler Management
  //=========================================================================

  /**
   * Initialize the Luigi scheduler for this partition.
   * Sets up read/write callbacks and replication hooks.
   */
  void InitScheduler(uint32_t partition_id);

  /**
   * Stop the scheduler and clean up resources.
   */
  void StopScheduler();

  /**
   * Get the Luigi scheduler (for local dispatch).
   */
  SchedulerLuigi *GetScheduler() { return scheduler_; }

protected:
  //=========================================================================
  // Request Handlers
  //=========================================================================

  void HandleDispatch(char *reqBuf, char *respBuf, size_t &respLen);
  void HandleStatusCheck(char *reqBuf, char *respBuf, size_t &respLen);
  void HandleOwdPing(char *reqBuf, char *respBuf, size_t &respLen);
  void HandleDeadlinePropose(char *reqBuf, char *respBuf, size_t &respLen);
  void HandleDeadlineConfirm(char *reqBuf, char *respBuf, size_t &respLen);
  void HandleWatermarkExchange(char *reqBuf, char *respBuf, size_t &respLen);

  //=========================================================================
  // Result Storage (for async polling)
  //=========================================================================

  struct TxnResult {
    int status;
    uint64_t commit_timestamp;
    std::vector<std::string> read_results;
    std::chrono::steady_clock::time_point completion_time;
  };

  void StoreResult(uint64_t txn_id, int status, uint64_t commit_ts,
                   const std::vector<std::string> &read_results);
  void CleanupStaleResults(int ttl_seconds = 60);

  /**
   * Replicate a Luigi entry via Paxos.
   * Called from scheduler's replication callback.
   */
  bool ReplicateEntry(const std::shared_ptr<LuigiLogEntry> &entry);

private:
  transport::Configuration config_;

  // Luigi scheduler
  SchedulerLuigi *scheduler_ = nullptr;

  // Async result storage
  std::unordered_map<uint64_t, TxnResult> completed_txns_;
  mutable std::shared_mutex results_mutex_;
};

/**
 * LuigiServer: Wrapper that creates and manages a LuigiReceiver.
 *
 * Similar to Mako's ShardServer, but only handles Luigi requests.
 */
class LuigiServer {
public:
  LuigiServer(int shard_idx, const std::string &benchmark_type = "tpcc");
  ~LuigiServer();

  /**
   * Start the server's event loop.
   * Initializes OWD, scheduler, state machine, and runs until stopped.
   */
  void Run();

  /**
   * Get the scheduler for local dispatch.
   */
  SchedulerLuigi *GetScheduler() {
    return receiver_ ? receiver_->GetScheduler() : nullptr;
  }

private:
  transport::Configuration *config_;
  LuigiReceiver *receiver_ = nullptr;
  std::string benchmark_type_;
  int shard_idx_;
};

//=============================================================================
// Global Luigi Server Management
//=============================================================================

/**
 * Register a Luigi server instance for global management.
 * Used for setting up cross-shard RPC connections.
 */
void RegisterLuigiServer(LuigiServer *server);

/**
 * Unregister a Luigi server instance.
 */
void UnregisterLuigiServer(LuigiServer *server);

/**
 * Get the Luigi scheduler for local dispatch (multi-shard mode).
 * Returns nullptr if no Luigi servers are registered.
 */
SchedulerLuigi *GetLocalLuigiScheduler();

/**
 * Setup Luigi RPC for all registered servers.
 * This is the Luigi-equivalent of mako::setup_luigi_rpc().
 * Should be called after server transports are initialized.
 */

} // namespace janus
