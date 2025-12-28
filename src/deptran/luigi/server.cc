/**
 * @file server.cc
 * @brief LuigiServer implementation and main entry point
 *
 * Usage: ./luigi_server -f config.yml -P process_name
 */

#include "server.h"

#include "commo.h"
#include "executor.h"
#include "scheduler.h"
#include "service.h"
#include "state_machine.h"

#include "../__dep__.h"
#include "../config.h"
#include "rrr.hpp"

#include <getopt.h>
#include <iostream>
#include <memory>
#include <signal.h>
#include <string>
#include <thread>

namespace janus {

//=============================================================================
// LuigiServer Implementation
//=============================================================================

LuigiServer::LuigiServer(int partition_id)
    : partition_id_(partition_id), shard_idx_(partition_id) {}

LuigiServer::~LuigiServer() {
  Stop();
  delete scheduler_;
}

void LuigiServer::Initialize() {
  // Scheduler initialized here, commo created later
  scheduler_ = new SchedulerLuigi();
  scheduler_->partition_id_ = partition_id_;
  scheduler_->SetPartitionId(partition_id_);
}

void LuigiServer::StartListener(const std::string &bind_addr) {
  // Create RPC service
  service_ = std::make_unique<LuigiServiceImpl>(this);

  // Create and start RRR server (listener only)
  auto poll = PollThread::create();
  base::ThreadPool *thread_pool = new base::ThreadPool(1);
  rpc_server_ = std::make_unique<rrr::Server>(poll, thread_pool);
  rpc_server_->reg(service_.get());
  rpc_server_->start(bind_addr.c_str());

  Log_info("Luigi server listening on %s for shard %d", bind_addr.c_str(),
           partition_id_);
}

void LuigiServer::ConnectAndStart() {
  // Now create communicator (all other sites should be listening)
  commo_ = std::make_shared<LuigiCommo>(rusty::None);
  scheduler_->commo_ = commo_.get();

  // Initialize worker count (default: 1 worker per shard, could be
  // configurable) This ensures watermarks_ vector is properly sized
  scheduler_->SetWorkerCount(1);

  // Detect follower sites for multi-replica mode
  // Look for other sites serving the same shard (partition_id)
  auto config = Config::GetConfig();
  std::vector<uint32_t> follower_sites;
  auto all_sites = config->SitesByPartitionId(partition_id_);

  // Get our own site info to exclude it from followers
  auto my_sites = config->GetMyServers();
  uint32_t my_site_id = my_sites.empty() ? UINT32_MAX : my_sites[0].id;

  for (auto &site : all_sites) {
    // Skip self - compare with our actual site ID, not partition_id
    if (site.id != my_site_id) {
      follower_sites.push_back(site.id);
      Log_info("Luigi server shard %d: detected follower site %u (my_site=%u)",
               partition_id_, site.id, my_site_id);
    }
  }

  if (!follower_sites.empty()) {
    scheduler_->SetFollowerSites(follower_sites);
    Log_info("Luigi server shard %d: multi-replica mode with %zu followers "
             "(%zu total replicas)",
             partition_id_, follower_sites.size(), follower_sites.size() + 1);
  } else {
    Log_info("Luigi server shard %d: single-replica mode", partition_id_);
  }

  // Start scheduler threads (which may broadcast watermarks etc)
  scheduler_->Start();

  Log_info("Luigi server ready for shard %d", partition_id_);
}

void LuigiServer::Stop() {
  if (scheduler_) {
    scheduler_->Stop();
  }
  if (rpc_server_) {
    rpc_server_.reset();
  }
}

} // namespace janus

//=============================================================================
// Main Entry Point
//=============================================================================

using namespace std;
using namespace janus;

static vector<unique_ptr<LuigiServer>> g_servers;
static volatile bool g_running = true;

static void signal_handler(int sig) {
  cout << "\nReceived signal " << sig << ", shutting down...\n";
  g_running = false;
}

static void print_usage(const char *prog) {
  cerr << "Usage: " << prog << " -f config.yml -P process_name [options]\n"
       << "  -f FILE   Config file (required)\n"
       << "  -P NAME   Process name (required)\n"
       << "  -b TYPE   Benchmark type: micro, tpcc (default: micro)\n"
       << "  -w N      Number of warehouses for TPC-C (default: num_shards)\n";
}

int main(int argc, char **argv) {
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  // Parse command-line options
  string benchmark_type = "micro";
  int num_warehouses = 0; // 0 means auto-detect from num_shards

  int opt;
  while ((opt = getopt(argc, argv, "f:P:b:w:h")) != -1) {
    switch (opt) {
    case 'f':
    case 'P':
      break; // Handled by Config::CreateConfig
    case 'b':
      benchmark_type = optarg;
      break;
    case 'w':
      num_warehouses = atoi(optarg);
      break;
    case 'h':
      print_usage(argv[0]);
      return 0;
    default:
      break;
    }
  }

  optind = 1; // Reset for Config::CreateConfig
  int ret = Config::CreateConfig(argc, argv);
  if (ret != 0) {
    cerr << "Error: Failed to parse config\n";
    print_usage(argv[0]);
    return 1;
  }

  auto config = Config::GetConfig();
  auto my_sites = config->GetMyServers();

  if (my_sites.empty()) {
    cerr << "Error: No server sites found for process '" << config->proc_name_
         << "'\n";
    return 1;
  }

  int num_shards = config->GetNumPartition();

  // Default warehouses to num_shards if not specified
  if (num_warehouses <= 0) {
    num_warehouses = num_shards;
  }

  cout << "=== Luigi Server ===\n"
       << "Process: " << config->proc_name_ << ", Sites: " << my_sites.size()
       << ", Benchmark: " << benchmark_type
       << ", Warehouses: " << num_warehouses << "\n";

  // Phase 1: Create servers and start listeners (no connections yet)
  for (auto &site : my_sites) {
    cout << "  - Site " << site.id << ": shard=" << site.partition_id_
         << " addr=" << site.GetBindAddress() << "\n";

    auto server = make_unique<LuigiServer>(site.partition_id_);

    shared_ptr<LuigiStateMachine> state_machine;
    if (benchmark_type == "tpcc") {
      auto tpcc_sm = make_shared<LuigiTPCCStateMachine>(
          site.partition_id_, 0, num_shards, 1);
      tpcc_sm->SetConfig(num_warehouses, 10, 3000, 100000); // warehouses, districts, customers, items
      state_machine = tpcc_sm;
    } else {
      state_machine = make_shared<LuigiMicroStateMachine>(
          site.partition_id_, 0, num_shards, 1);
    }
    state_machine->InitializeTables();
    server->SetStateMachine(state_machine);

    server->Initialize();
    server->StartListener(site.GetBindAddress());

    g_servers.push_back(std::move(server));
  }

  // Brief pause to let all listeners start
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Phase 2: Create commos and start schedulers (now all sites are listening)
  for (auto &server : g_servers) {
    server->ConnectAndStart();
  }

  cout << "\n=== Luigi Server Ready ===\n";

  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  cout << "\nShutting down...\n";
  for (auto &server : g_servers) {
    server->Stop();
  }
  g_servers.clear();

  return 0;
}
