/**
 * Luigi helper server setup - mirrors Mako's helper_server architecture
 * 
 * This creates threads that:
 * 1. Pull requests from HelperQueues (populated by FastTransport)
 * 2. Call LuigiReceiver->ReceiveRequest() to handle them
 * 3. Send responses back
 */

#include "luigi_helper.h"
#include "luigi_server.h"
#include "mako/benchmarks/benchmark_config.h"
#include "mako/lib/common.h"
#include "mako/lib/transport_request_handle.h"
#include "deptran/__dep__.h"

#include <mutex>
#include <thread>
#include <vector>

namespace janus {
namespace luigi {

namespace {
  std::mutex g_helper_mu;
  std::vector<LuigiHelperServer*> g_helper_servers;
  std::vector<std::thread> g_helper_threads;
}

//=============================================================================
// LuigiHelperServer: Handles requests from a HelperQueue
//=============================================================================

LuigiHelperServer::LuigiHelperServer(
    const std::string& config_file,
    int shard_idx,
    LuigiReceiver* receiver,
    mako::HelperQueue* queue,
    mako::HelperQueue* queue_response)
    : config_file_(config_file),
      shard_idx_(shard_idx),
      receiver_(receiver),
      queue_(queue),
      queue_response_(queue_response),
      running_(false) {}

LuigiHelperServer::~LuigiHelperServer() {
  Stop();
}

void LuigiHelperServer::Run() {
  running_ = true;
  
  Log_info("[HELPER-SERVER] Starting for shard %d", shard_idx_);
  int requests_handled = 0;
  
  try {
    while (running_) {
      queue_->suspend();
      
      while (running_) {
        erpc::ReqHandle *handle;
        size_t msg_size;
        if (!queue_->fetch_one_req(&handle, msg_size)) {
          break;
        }
        
        if (!handle) {
          Log_error("[HELPER-SERVER] Invalid handle pointer");
          continue;
        }
        
        // Cast to transport-agnostic interface
        mako::TransportRequestHandle* req_handle = 
            reinterpret_cast<mako::TransportRequestHandle*>(handle);
        
        requests_handled++;
        if (requests_handled % 50 == 0) {
          Log_info("[HELPER-SERVER] Shard-%d: Pulled request #%d from queue (type=%d, size=%zu)",
                   shard_idx_, requests_handled, req_handle->GetRequestType(), msg_size);
        }
        
        // Call Luigi's request handler
        size_t resp_len = receiver_->ReceiveRequest(
            req_handle->GetRequestType(),
            req_handle->GetRequestBuffer(),
            req_handle->GetResponseBuffer());
        
        if (requests_handled % 50 == 0) {
          Log_info("[HELPER-SERVER] Shard-%d: Request handled, sending response (len=%zu)",
                   shard_idx_, resp_len);
        }
        
        // Enqueue response
        req_handle->EnqueueResponse(resp_len);
      }
    }
  } catch (int e) {
    // Boost coroutines throw int for stack unwinding - this is normal
    Log_info("[HELPER-SERVER] Shard-%d: Coroutine unwinding (caught int=%d)", shard_idx_, e);
  } catch (const std::exception& e) {
    Log_error("[HELPER-SERVER] Shard-%d: Exception: %s", shard_idx_, e.what());
  } catch (...) {
    Log_error("[HELPER-SERVER] Shard-%d: Unknown exception", shard_idx_);
  }
  
  Log_info("[HELPER-SERVER] Stopped for shard %d (handled %d requests)", 
           shard_idx_, requests_handled);
}

void LuigiHelperServer::Stop() {
  running_ = false;
}

//=============================================================================
// Setup/Teardown Functions
//=============================================================================

void setup_luigi_helper(
    LuigiReceiver* receiver,
    const std::string& config_file,
    int shard_idx)
{
  auto &cfg = BenchmarkConfig::getInstance();
  auto &queue_holders = cfg.getQueueHolders();
  auto &queue_holders_response = cfg.getQueueHoldersResponse();
  
  Log_info("[SETUP-HELPER] Setting up Luigi helper servers for shard %d", shard_idx);
  Log_info("[SETUP-HELPER] Available queues: %zu", queue_holders.size());
  
  // Create helper server for each remote partition
  // (queues were already created by setup_erpc_server())
  for (auto& entry : queue_holders) {
    int par_id = entry.first;
    mako::HelperQueue* queue = entry.second;
    mako::HelperQueue* queue_response = queue_holders_response[par_id];
    
    if (!queue || !queue_response) {
      Log_warn("[SETUP-HELPER] No queue for par_id %d", par_id);
      continue;
    }
    
    Log_info("[SETUP-HELPER] Creating helper for par_id %d", par_id);
    
    // Create helper server
    auto* helper = new LuigiHelperServer(
        config_file, shard_idx, receiver, queue, queue_response);
    
    {
      std::lock_guard<std::mutex> lock(g_helper_mu);
      g_helper_servers.push_back(helper);
    }
    
    // Start helper thread
    std::thread helper_thread([helper, par_id]() {
      Log_info("[HELPER-THREAD-%d] Thread started", par_id);
      helper->Run();
      Log_info("[HELPER-THREAD-%d] Thread exiting", par_id);
    });
    
    pthread_setname_np(helper_thread.native_handle(), 
                       ("luigi_help_" + std::to_string(par_id)).c_str());
    
    {
      std::lock_guard<std::mutex> lock(g_helper_mu);
      g_helper_threads.push_back(std::move(helper_thread));
    }
    
    Log_info("[SETUP-HELPER] Helper server created and started for par_id %d", par_id);
  }
  
  Log_info("[SETUP-HELPER] Luigi helper setup complete: %zu servers created", 
           g_helper_servers.size());
}

void stop_luigi_helper() {
  Log_info("Stopping Luigi helper servers...");
  
  std::lock_guard<std::mutex> lock(g_helper_mu);
  
  // 1. Signal stop
  for (auto* helper : g_helper_servers) {
    helper->Stop();
  }

  // 2. Join threads
  for (auto& t : g_helper_threads) {
    if (t.joinable()) {
      t.join();
    }
  }
  g_helper_threads.clear();

  // 3. Delete objects
  for (auto* helper : g_helper_servers) {
    delete helper;
  }
  g_helper_servers.clear();
  
  Log_info("Luigi helper servers stopped");
}

} // namespace luigi
} // namespace janus
