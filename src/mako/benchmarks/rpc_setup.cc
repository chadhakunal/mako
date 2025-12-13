// Implementation of TPCC setup utilities extracted from tpcc.cc

#include "rpc_setup.h"

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <unistd.h>

#include "benchmark_config.h"
#include "lib/common.h"
#include "lib/fasttransport.h"
#include "lib/server.h"
#include "deptran/s_main.h"
#include "deptran/luigi/luigi_scheduler.h"
#include "deptran/luigi/luigi_entry.h"
#include "benchmarks/sto/Interface.hh"
#include "spinbarrier.h"

#include "deptran/config.h"


using namespace std;
using namespace mako;

namespace mako {

// Local Luigi scheduler for single-shard mode (no helper servers needed)
std::unique_ptr<janus::SchedulerLuigi> g_local_luigi_scheduler;
bool g_local_luigi_scheduler_started = false;

// (Removed duplicate definitions; only the mako namespace block at the top remains)

} // end namespace mako

namespace {

std::mutex g_helper_mu;
std::vector<mako::ShardServer *> g_helper_servers;

} // end anonymous namespace
std::map<int, abstract_ordered_index*> g_local_open_tables;
abstract_db* g_local_db = nullptr;

static inline size_t NumWarehouses() {
  return (size_t) BenchmarkConfig::getInstance().getScaleFactor();
}

static inline size_t NumWarehousesTotal() {
  auto &cfg = BenchmarkConfig::getInstance();
  return cfg.getNshards() * ((size_t) cfg.getScaleFactor());
}

// Thread entry: server-side helper processing
void helper_server(
  int g_wid,
  std::string cluster,
  int running_shardIndex,
  int num_warehouses,
  transport::Configuration *config,
  abstract_db *db,
  mako::HelperQueue *queue,
  mako::HelperQueue *queue_response,
  std::map<int, abstract_ordered_index *> open_tables,
  spin_barrier *barrier_ready)
{
  scoped_db_thread_ctx ctx(db, true, 1);
  TThread::set_mode(1);
#if defined(DISABLE_MULTI_VERSION)
  TThread::disable_multiversion();
#else
  TThread::enable_multiverison();
#endif
  int shardIdx = (g_wid - 1) / num_warehouses;
  int par_id = (g_wid - 1) % num_warehouses;
  TThread::set_shard_index(running_shardIndex);
  TThread::set_pid(par_id);
  TThread::set_nshards(config->nshards);

  mako::ShardServer *ss = new mako::ShardServer(
    config->configFile, running_shardIndex, shardIdx, par_id);
  ss->Register(db, queue, queue_response, open_tables);
  {
    std::lock_guard<std::mutex> lock(g_helper_mu);
    g_helper_servers.push_back(ss);
  }

  // Signal that this helper is ready before starting the event loop
  if (barrier_ready) {
    barrier_ready->count_down();
  }

  ss->Run(); // event-driven
}

// Thread entry: eRPC server
void erpc_server(
  std::string cluster,
  int running_shardIndex,
  int num_warehouses,
  transport::Configuration *config,
  int alpha,
  std::vector<FastTransport*> &server_transports,
  std::atomic<int> &set_server_transport)
{
  std::string local_uri = config->shard(running_shardIndex, mako::convertCluster(cluster)).host;
  int base = 5;
  int id = num_warehouses + base + alpha;
  server_transports[alpha] = new FastTransport(
    config->configFile,
    local_uri,
    cluster,
    1, 12,
    0, // physPort
    0, // numa node
    running_shardIndex,
    id);

  // Set up helper queues for this server transport
  std::unordered_map<uint16_t, mako::HelperQueue*> local_queue_holders;
  std::unordered_map<uint16_t, mako::HelperQueue*> local_queue_holders_response;

  for (int i = 0; i < (int)NumWarehousesTotal(); i++) {
    if (i / (int)NumWarehouses() == running_shardIndex)
      continue;
    if (i % (int)BenchmarkConfig::getInstance().getNumErpcServer() == alpha) {
      auto *it = new mako::HelperQueue(i, true);
      local_queue_holders[i] = it;
      auto *it_res = new mako::HelperQueue(i, false);
      local_queue_holders_response[i] = it_res;
    }
  }

  server_transports[alpha]->SetHelperQueues(local_queue_holders);
  server_transports[alpha]->SetHelperQueuesResponse(local_queue_holders_response);
  set_server_transport.fetch_add(1);
  server_transports[alpha]->Run();
  Notice("the erpc_server is terminated on shardIdx:%d, alpha:%d!", running_shardIndex, alpha);
}

} // anonymous namespace

void mako::setup_helper(
  abstract_db *db,
  const std::map<int, abstract_ordered_index *> &open_tables)
{
  auto &cfg = BenchmarkConfig::getInstance();
  auto &queue_holders = cfg.getQueueHolders();
  auto &queue_holders_response = cfg.getQueueHoldersResponse();

  // Count the number of helper threads that will be created
  int num_helpers = 0;
  for (int i = 0; i < (int)NumWarehousesTotal(); i++) {
    if (i / (int)NumWarehouses() == (int)cfg.getShardIndex())
      continue;
    num_helpers++;
  }

  // Create barrier to wait for all helpers to be ready
  spin_barrier barrier_ready(num_helpers+1);

  for (int i = 0; i < (int)NumWarehousesTotal(); i++) {
    if (i / (int)NumWarehouses() == (int)cfg.getShardIndex())
      continue;

    auto t = std::thread(
      helper_server,
      i + 1,
      cfg.getCluster(),
      (int)cfg.getShardIndex(),
      (int)NumWarehouses(),
      cfg.getConfig(),
      db,
      queue_holders[i],
      queue_holders_response[i],
      open_tables,
      &barrier_ready);
    pthread_setname_np(t.native_handle(), ("helper_" + std::to_string(i)).c_str());
    t.detach();
  }

  // Wait for all helper threads to finish initialization before returning
  barrier_ready.count_down();
  barrier_ready.wait_for();
}

void mako::setup_update_table(int table_id, abstract_ordered_index *table)
{
  if (!table)
    return;

  std::lock_guard<std::mutex> lock(g_helper_mu);
  for (auto *server : g_helper_servers) {
    server->UpdateTable(table_id, table);
  }
}

void mako::stop_helper()
{
  auto &cfg = BenchmarkConfig::getInstance();
  auto &queue_holders = cfg.getQueueHolders();
  for (auto &entry : queue_holders) {
    if (entry.second) {
      entry.second->request_stop();
    } 
  }
  {
    std::lock_guard<std::mutex> lock(g_helper_mu);
    g_helper_servers.clear();
  }
}

void mako::initialize_per_thread(abstract_db *db_) {
  scoped_db_thread_ctx ctx(db_, false);
}

void mako::setup_erpc_server()
{
  auto &cfg = BenchmarkConfig::getInstance();
  auto &server_transports = cfg.getServerTransports();
  auto &queue_holders = cfg.getQueueHolders();
  auto &queue_holders_response = cfg.getQueueHoldersResponse();
  auto &set_server_transport = cfg.getServerTransportReadyCounter();

  // Use existing state; server threads will populate queues.
  if (server_transports.size() < cfg.getNumErpcServer())
    server_transports.resize(cfg.getNumErpcServer());
  for (int i = 0; i < (int)cfg.getNumErpcServer(); ++i) {
    auto t = std::thread(
      erpc_server,
      cfg.getCluster(),
      (int)cfg.getShardIndex(),
      (int)NumWarehouses(),
      cfg.getConfig(),
      i,
      std::ref(server_transports),
      std::ref(set_server_transport));
    pthread_setname_np(t.native_handle(), "erpc_server");
    t.detach();
  }

  while (set_server_transport.load() < (int)cfg.getNumErpcServer()) {
    sleep(0);
  }

  for (int i = 0; i < (int)NumWarehousesTotal(); i++) {
    if (i / (int)NumWarehouses() == (int)cfg.getShardIndex())
      continue;
    auto idx = i % (int)cfg.getNumErpcServer();
    queue_holders[i] = server_transports[idx]->GetHelperQueue(i);
    queue_holders_response[i] = server_transports[idx]->GetHelperQueueResponse(i);
  }
}

void mako::stop_erpc_server()
{
  auto &cfg = BenchmarkConfig::getInstance();
  auto &server_transports = cfg.getServerTransports();

  // Use actual vector size to avoid out-of-bounds access
  // In multi-shard mode, server_transports may be empty while getNumErpcServer() > 0
  size_t actual_count = server_transports.size();
  std::cerr << "[STOP_SERVER] Stopping " << actual_count << " server transports" << std::endl;

  for (size_t i = 0; i < actual_count; ++i) {
    if (server_transports[i]) {
      std::cerr << "[STOP_SERVER] Stopping server transport " << i << std::endl;
      server_transports[i]->Stop();
      std::cerr << "[STOP_SERVER] Server transport " << i << " stopped" << std::endl;
    }
  }
  std::cerr << "[STOP_SERVER] All server transports stopped" << std::endl;
}

void mako::setup_luigi_rpc()
{
  auto &cfg = BenchmarkConfig::getInstance();
  
  // Only setup Luigi RPC if Luigi mode is enabled
  if (!cfg.getUseLuigi()) {
    return;
  }
  
  auto &server_transports = cfg.getServerTransports();
  if (server_transports.empty()) {
    Notice("setup_luigi_rpc: No server transports available, skipping");
    return;
  }
  
  // Get RPC server and poll thread from the first transport
  FastTransport* transport = server_transports[0];
  if (!transport) {
    Warning("setup_luigi_rpc: First transport is null");
    return;
  }
  
  rrr::Server* rpc_server = transport->GetRpcServer();
  auto poll_thread_opt = transport->GetPollThread();
  
  if (!rpc_server) {
    Notice("setup_luigi_rpc: No RPC server available (eRPC mode?), skipping");
    return;
  }
  
  if (poll_thread_opt.is_none()) {
    Warning("setup_luigi_rpc: No poll thread available");
    return;
  }
  
  rusty::Arc<rrr::PollThread> poll_thread = poll_thread_opt.unwrap();
  
  // Build shard addresses map: shard_id -> "host:port"
  std::map<uint32_t, std::string> shard_addresses;
  transport::Configuration* config = cfg.getConfig();
  if (config) {
    std::string cluster = cfg.getCluster();
    int cluster_role = mako::convertCluster(cluster);
    
    for (int shard_idx = 0; shard_idx < config->nshards; shard_idx++) {
      // Skip our own shard
      if (shard_idx == (int)cfg.getShardIndex()) {
        continue;
      }
      
      std::string host = config->shard(shard_idx, cluster_role).host;
      std::string port = config->shard(shard_idx, cluster_role).port;
      shard_addresses[shard_idx] = host + ":" + port;
    }
  }
  
  // Setup Luigi RPC for each helper server
  {
    std::lock_guard<std::mutex> lock(g_helper_mu);
    for (auto* server : g_helper_servers) {
      server->SetupLuigiRpc(rpc_server, poll_thread.clone(), shard_addresses);
    }
    
    // For single-shard mode, we don't need a full scheduler - transactions
    // will be executed directly via local_luigi_execute()
    if (g_helper_servers.empty() && cfg.getUseLuigi()) {
      Notice("Single-shard Luigi mode: using direct local execution");
    }
  }
  
  Notice("Luigi RPC setup complete: %zu shard addresses configured", shard_addresses.size());
}

//=========================================================================
// Get Luigi scheduler for local dispatch (multi-shard mode only)
// For single-shard mode, use local_luigi_execute() instead
//=========================================================================
janus::SchedulerLuigi* mako::get_local_luigi_scheduler() {
  std::lock_guard<std::mutex> lock(g_helper_mu);
  
  // Only return scheduler from helper servers (multi-shard mode)
  if (!g_helper_servers.empty()) {
    return g_helper_servers[0]->GetLuigiScheduler();
  }
  
  // For single-shard mode, return nullptr - use local_luigi_execute() instead
  return nullptr;
}

//=========================================================================
// Direct local Luigi execution for single-shard mode
// Executes operations directly on local tables without scheduler overhead
//=========================================================================
int mako::local_luigi_execute(
    const std::vector<janus::LuigiOp>& ops,
    uint64_t expected_time,
    uint64_t& out_commit_ts,
    std::vector<std::string>& out_read_results) {
  
  std::lock_guard<std::mutex> lock(g_helper_mu);
  
  if (g_local_open_tables.empty()) {
    return -1;  // Tables not registered
  }
  
  if (g_local_db == nullptr) {
    return -4;  // DB not registered
  }
  
  out_read_results.clear();
  out_commit_ts = expected_time;  // Use expected time as commit timestamp
  
  // Start STO transaction context (required for shard_put/shard_get)
  g_local_db->shard_reset();
  
  // Execute operations in timestamp order
  for (const auto& op : ops) {
    auto it = g_local_open_tables.find(op.table_id);
    if (it == g_local_open_tables.end() || it->second == nullptr) {
      static bool logged = false;
      if (!logged) {
        fprintf(stderr, "[Luigi] Table %d not found in local_open_tables (size=%zu)\n",
                op.table_id, g_local_open_tables.size());
        logged = true;
      }
      g_local_db->shard_abort_txn(nullptr);
      return -2;  // Table not found
    }
    
    if (op.op_type == mako::LUIGI_OP_READ) {
      std::string value;
      bool found = it->second->shard_get(lcdf::Str(op.key), value);
      out_read_results.push_back(found ? value : "");
    } else if (op.op_type == mako::LUIGI_OP_WRITE) {
      try {
        it->second->shard_put(lcdf::Str(op.key), op.value);
        out_read_results.push_back("");  // Empty for writes
      } catch (...) {
        static bool logged = false;
        if (!logged) {
          fprintf(stderr, "[Luigi] Write to table %d failed with exception\n", op.table_id);
          logged = true;
        }
        g_local_db->shard_abort_txn(nullptr);
        return -3;  // Write failed
      }
    }
  }
  
  // Commit the transaction: validate, install at timestamp, unlock
  int validate_status = g_local_db->shard_validate();
  if (validate_status != 0) {
    g_local_db->shard_abort_txn(nullptr);
    return -5;  // Validation failed
  }
  
  g_local_db->shard_install((uint32_t)out_commit_ts);
  g_local_db->shard_serialize_util((uint32_t)out_commit_ts);
  g_local_db->shard_unlock(true);
  
  return 0;  // Success
}

//=========================================================================
// Register tables with local Luigi scheduler (for single-shard mode)
//=========================================================================
void mako::setup_luigi_tables(const std::map<int, abstract_ordered_index*>& open_tables, abstract_db* db) {
  std::lock_guard<std::mutex> lock(g_helper_mu);
  g_local_open_tables = open_tables;
  g_local_db = db;
  Notice("Registered %zu tables with local Luigi scheduler", open_tables.size());
}

//=========================================================================
// Stop local Luigi scheduler
//=========================================================================
void mako::stop_local_luigi() {
  std::lock_guard<std::mutex> lock(g_helper_mu);
  if (g_local_luigi_scheduler) {
    g_local_luigi_scheduler->Stop();
    g_local_luigi_scheduler.reset();
    Notice("Local Luigi scheduler stopped");
  }
}
