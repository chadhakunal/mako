#pragma once

#include <string>
#include "mako/lib/helper_queue.h"

namespace janus {

class LuigiReceiver;

namespace luigi {

/**
 * LuigiHelperServer: Pulls requests from HelperQueue and handles them
 * 
 * Similar to Mako's ShardServer, but for Luigi protocol.
 * Each helper server:
 * - Pulls requests from a HelperQueue (populated by FastTransport)
 * - Calls LuigiReceiver->ReceiveRequest()
 * - Sends responses back via the queue
 */
class LuigiHelperServer {
public:
  LuigiHelperServer(
      const std::string& config_file,
      int shard_idx,
      LuigiReceiver* receiver,
      mako::HelperQueue* queue,
      mako::HelperQueue* queue_response);
  
  ~LuigiHelperServer();
  
  // Run the helper server event loop (blocks until stopped)
  void Run();
  
  // Stop the helper server
  void Stop();

private:
  std::string config_file_;
  int shard_idx_;
  LuigiReceiver* receiver_;
  mako::HelperQueue* queue_;
  mako::HelperQueue* queue_response_;
  bool running_;
};

/**
 * Setup Luigi helper servers for the current shard.
 * 
 * Creates helper threads that pull requests from HelperQueues
 * and dispatch them to the LuigiReceiver.
 * 
 * Must be called AFTER setup_luigi_transport() (which creates the queues).
 * 
 * @param receiver The LuigiReceiver to handle requests
 * @param config_file Path to shard configuration
 * @param shard_idx Index of this shard
 */
void setup_luigi_helper(
    LuigiReceiver* receiver,
    const std::string& config_file,
    int shard_idx);

/**
 * Stop all Luigi helper servers.
 */
void stop_luigi_helper();

} // namespace luigi
} // namespace janus
