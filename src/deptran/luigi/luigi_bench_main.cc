/**
 * @file luigi_bench_main.cc
 * @brief Main entry point for Luigi stored-procedure benchmark
 *
 * This benchmark tests Luigi's timestamp-ordered execution protocol
 * using Tiga-style stored procedures (not STO-style wrappers).
 *
 * Compatible with Mako CI configuration files!
 *
 * Usage (Mako CI compatible):
 *   ./luigi_bench --shard-config <shard_config.yml> --shard-index <idx> \
 *                 -P <cluster> --num-threads <n> --duration <sec>
 *
 * Or standalone:
 *   ./luigi_bench --config <shard_config> --shard <shard_idx> \
 *                 --cluster <cluster_name> --benchmark <micro|tpcc> \
 *                 --threads <num_threads> --duration <seconds>
 *
 * Examples:
 *   # Mako CI style (uses same args as dbtest)
 *   ./luigi_bench --shard-config src/mako/config/local-shards2-warehouses6.yml
 * \
 *                 --shard-index 0 -P localhost --num-threads 6 --duration 30
 *
 *   # Run micro benchmark for 30 seconds with 4 threads
 *   ./luigi_bench --config config/shards.yml --shard 0 --cluster dc0 \
 *                 --benchmark micro --threads 4 --duration 30
 *
 *   # Run TPC-C benchmark
 *   ./luigi_bench --config config/shards.yml --shard 0 --cluster dc0 \
 *                 --benchmark tpcc --threads 8 --duration 60
 */

#include <cstdlib>
#include <getopt.h>
#include <iostream>
#include <string>

#include "deptran/luigi/luigi_benchmark_client.h"
#include "deptran/luigi/luigi_owd.h"
#include "deptran/luigi/luigi_transport_setup.h"
#include "deptran/luigi/luigi_server.h"
#include "deptran/luigi/luigi_scheduler.h"  // For SetLuigiClient
#include "deptran/luigi/luigi_state_machine.h"  // For state machine mode
#include "mako/lib/configuration.h"
#include "mako/benchmarks/benchmark_config.h"

#include <thread>

using namespace mako::luigi;
using namespace janus::luigi; // For transport setup functions

void PrintUsage(const char *prog) {
  std::cerr
      << "Usage: " << prog << " [options]\n"
      << "\nMako CI Compatible Options (same as dbtest):\n"
      << "  -q, --shard-config <file>  Shard configuration file (required)\n"
      << "  -g, --shard-index <idx>    Shard index (default: 0)\n"
      << "  -P <cluster>               Cluster/process name (localhost, p1, "
         "p2, learner)\n"
      << "  -t, --num-threads <n>      Number of worker threads (default: 1)\n"
      << "\nStandalone Options:\n"
      << "  -c, --config <file>        Alias for --shard-config\n"
      << "  -C, --cluster <name>       Alias for -P (default: localhost)\n"
      << "  -b, --benchmark <type>     Benchmark type: micro, micro_single, "
         "tpcc (default: tpcc)\n"
      << "  -d, --duration <sec>       Benchmark duration in seconds (default: "
         "30)\n"
      << "  -k, --keys <n>             Number of keys per shard for micro "
         "(default: 100000)\n"
      << "  -r, --read-ratio <r>       Read ratio for micro benchmark "
         "(default: 0.5)\n"
      << "  -o, --ops <n>              Operations per transaction for micro "
         "(default: 10)\n"
      << "  --owd-ms <ms>              One-way delay for geo-distributed testing "
         "(default: 1)\n"
      << "  -h, --help                 Show this help message\n"
      << "\nThe benchmark reads warehouses count from the YAML config file.\n";
}

int main(int argc, char *argv[]) {
try {
  // Default configuration
  LuigiBenchmarkClient::Config config;
  config.config_file = "";
  config.cluster = "localhost"; // Default to localhost like Mako CI
  config.shard_index = 0;
  config.par_id = 0;
  config.num_shards = 0; // 0 means read from config file
  config.num_threads = 1;
  config.duration_sec = 30; // Default 30s like typical CI tests

  std::string benchmark_type = "tpcc"; // Default to TPC-C like Mako CI
  int keys_per_shard = 100000;
  int warehouses = 0; // 0 means read from config file
  double read_ratio = 0.5;
  int ops_per_txn = 10;
  bool test_one_txn = false; // TEST MODE: send only one cross-shard transaction
  bool server_only = false;  // SERVER-ONLY MODE: no benchmark client, just wait
  uint64_t owd_ms = 0;       // One-way delay for geo-distributed testing (0 = use default 1ms)

  // Parse command line arguments - support both Mako CI style and standalone
  static struct option long_options[] = {
      // Mako CI compatible options
      {"shard-config", required_argument, 0, 'q'}, // -q like dbtest
      {"shard-index", required_argument, 0, 'g'},  // -g like dbtest
      {"num-threads", required_argument, 0, 't'},  // -t like dbtest
      // Standalone options
      {"config", required_argument, 0, 'c'}, // Alias for -q
      {"shard", required_argument, 0,
       'G'}, // Alias for -g (uppercase to avoid conflict)
      {"cluster", required_argument, 0, 'C'},
      {"benchmark", required_argument, 0, 'b'},
      {"threads", required_argument, 0, 'T'}, // Alias for -t
      {"duration", required_argument, 0, 'd'},
      {"keys", required_argument, 0, 'k'},
      {"warehouses", required_argument, 0, 'w'},
      {"read-ratio", required_argument, 0, 'r'},
      {"ops", required_argument, 0, 'o'},
      {"owd-ms", required_argument, 0, 'O'},  // One-way delay for geo testing
      {"test-one", no_argument, 0, '1'},  // TEST: send one transaction
      {"server-only", no_argument, 0, 'S'},  // SERVER-ONLY: no benchmark client
      {"help", no_argument, 0, 'h'},
      {0, 0, 0, 0}};

  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "q:g:t:c:G:C:b:T:d:k:w:r:o:O:P:1Sh",
                            long_options, &option_index)) != -1) {
    switch (opt) {
    case 'q': // --shard-config (Mako CI style)
    case 'c': // --config (standalone alias)
      config.config_file = optarg;
      break;
    case 'g': // --shard-index (Mako CI style)
    case 'G': // --shard (standalone alias)
      config.shard_index = std::atoi(optarg);
      break;
    case 'P': // -P cluster (Mako CI style, like dbtest)
    case 'C': // --cluster (standalone alias)
      config.cluster = optarg;
      break;
    case 'b': // --benchmark
      benchmark_type = optarg;
      break;
    case 't': // --num-threads (Mako CI style)
    case 'T': // --threads (standalone alias)
      config.num_threads = std::atoi(optarg);
      break;
    case 'd': // --duration
      config.duration_sec = std::atoi(optarg);
      break;
    case 'k': // --keys
      keys_per_shard = std::atoi(optarg);
      break;
    case 'w': // --warehouses
      warehouses = std::atoi(optarg);
      break;
    case 'r': // --read-ratio
      read_ratio = std::atof(optarg);
      break;
    case 'o': // --ops
      ops_per_txn = std::atoi(optarg);
      break;
    case 'O': // --owd-ms
      owd_ms = std::atoi(optarg);
      break;
    case '1': // --test-one
      test_one_txn = true;
      break;
    case 'S': // --server-only
      server_only = true;
      break;
    case 'h':
    default:
      PrintUsage(argv[0]);
      return opt == 'h' ? 0 : 1;
    }
  }

  // Validate required options
  if (config.config_file.empty()) {
    std::cerr << "Error: --shard-config or --config is required\n";
    PrintUsage(argv[0]);
    return 1;
  }

  // Parse the YAML config to extract shard count and warehouses
  try {
    transport::Configuration yaml_config(config.config_file);

    // Get number of shards from config if not specified
    if (config.num_shards == 0) {
      config.num_shards = yaml_config.nshards;
      std::cout << "Read num_shards=" << config.num_shards
                << " from config file\n";
    }

    // Get warehouses from config if not specified (for TPC-C)
    if (warehouses == 0 && yaml_config.warehouses > 0) {
      warehouses = yaml_config.warehouses;
      std::cout << "Read warehouses=" << warehouses << " from config file\n";
    }
  } catch (const std::exception &e) {
    std::cerr << "Warning: Could not parse YAML config: " << e.what() << "\n";
    std::cerr << "Using command-line defaults.\n";
  }

  // Fall back to defaults if still not set
  if (config.num_shards == 0)
    config.num_shards = 1;
  if (warehouses == 0)
    warehouses = config.num_threads; // Common Mako pattern

  // Setup generator config based on benchmark type
  if (benchmark_type == "micro" || benchmark_type == "micro_single") {
    config.gen_config =
        CreateDefaultMicroConfig(config.num_shards, keys_per_shard);
    config.gen_config.read_ratio = read_ratio;
    config.gen_config.ops_per_txn = ops_per_txn;
  } else if (benchmark_type == "tpcc") {
    config.gen_config = CreateDefaultTPCCConfig(config.num_shards, warehouses);
  } else {
    std::cerr << "Error: Unknown benchmark type '" << benchmark_type << "'\n";
    std::cerr << "Valid types: micro, micro_single, tpcc\n";
    return 1;
  }

  // Initialize Luigi OWD service (for calculating expected timestamps)
  std::cout << "\n[TRACE] Step 1: Initializing Luigi OWD service..." << std::endl;
  std::cout << "[TRACE]   - Config: " << config.config_file << std::endl;
  std::cout << "[TRACE]   - Shard: " << config.shard_index << "/" << config.num_shards << std::endl;
  auto &luigiOwd = LuigiOWD::getInstance();
  luigiOwd.init(config.config_file, config.cluster, config.shard_index,
                config.num_shards);
  // Set fixed OWD if configured (for geo-distributed testing with tc)
  if (owd_ms > 0) {
    luigiOwd.setFixedOWD(owd_ms);
    std::cout << "[TRACE]   - Fixed OWD: " << owd_ms << "ms (geo-distributed mode)" << std::endl;
  }
  luigiOwd.start();
  std::cout << "[TRACE] Step 1: OWD service started" << std::endl;

  // Setup transport infrastructure (delegates to Mako's setup_erpc_server)
  std::cout << "\n[TRACE] Step 2: Setting up transport infrastructure..." << std::endl;
  std::cout << "[TRACE]   - eRPC servers: " << config.num_threads << std::endl;
  int num_erpc_servers = config.num_threads; // One eRPC server per thread
  if (!luigi::setup_luigi_transport(config.config_file, config.cluster,
                                    config.shard_index, config.num_shards,
                                    num_erpc_servers, warehouses)) {
    std::cerr << "Failed to setup transport infrastructure" << std::endl;
    luigiOwd.stop();
    return 1;
  }
  std::cout << "[TRACE] Step 2: Transport infrastructure ready" << std::endl;

  // Create Luigi server and receiver for handling incoming requests
  std::cout << "\n[TRACE] Step 3: Creating Luigi server for shard " << config.shard_index << "..." << std::endl;
  
  // Create receiver (will be shared across helper threads)
  // Declared here so it's in scope for cleanup at the end
  std::cout << "[TRACE]   - Creating LuigiReceiver..." << std::endl;
  janus::LuigiReceiver* luigi_receiver = new janus::LuigiReceiver(config.config_file);
  
  std::cout << "[TRACE]   - Initializing Luigi scheduler..." << std::endl;
  luigi_receiver->InitScheduler(config.shard_index);
  std::cout << "[TRACE]   - Scheduler initialized" << std::endl;

  // Setup state machine for the benchmark type
  // This is REQUIRED for TPC-C which uses stored procedure model (working_set, not ops)
  std::cout << "[TRACE]   - Setting up state machine for " << benchmark_type << "..." << std::endl;
  auto* scheduler = luigi_receiver->GetScheduler();
  if (scheduler) {
    std::shared_ptr<janus::LuigiStateMachine> state_machine;
    if (benchmark_type == "tpcc") {
      state_machine = std::make_shared<janus::LuigiTPCCStateMachine>(
          config.shard_index,     // shard_id
          0,                      // replica_id
          config.num_shards,      // shard_num
          1                       // replica_num
      );
    } else if (benchmark_type == "micro" || benchmark_type == "micro_single") {
      state_machine = std::make_shared<janus::LuigiMicroStateMachine>(
          config.shard_index,     // shard_id
          0,                      // replica_id
          config.num_shards,      // shard_num
          1                       // replica_num
      );
    }

    if (state_machine) {
      // Configure TPC-C parameters (warehouses, districts, customers, items)
      if (auto *tpcc_sm = dynamic_cast<janus::LuigiTPCCStateMachine*>(state_machine.get())) {
        tpcc_sm->SetConfig(
            config.gen_config.num_warehouses,  // num_warehouses
            10,                                 // districts_per_warehouse
            3000,                               // customers_per_district
            1000                                // num_items (reduced for testing)
        );
        std::cout << "[TRACE]   - Configured TPC-C with " << config.gen_config.num_warehouses << " warehouses" << std::endl;
      }

      state_machine->InitializeTables();
      state_machine->PopulateData();  // Populate tables with TPC-C data
      scheduler->SetStateMachine(state_machine);
      scheduler->EnableStateMachineMode(true);
      std::cout << "[TRACE]   - State machine initialized: " << state_machine->RTTI() << std::endl;
      std::cout << "[TRACE]   - Tables populated with data" << std::endl;
    }
  }

  // Setup Luigi helper threads - these pull from HelperQueues and call receiver->ReceiveRequest()
  // This is the Luigi equivalent of Mako's setup_helper()
  std::cout << "[TRACE] Step 4: Setting up Luigi helper threads..." << std::endl;
  janus::luigi::setup_luigi_helper(luigi_receiver, config.config_file, config.shard_index);
  
  std::cout << "[TRACE] Step 4: Luigi server ready - handling incoming requests" << std::endl;

  // Create and initialize benchmark client
  std::cout << "\n[TRACE] Step 5: Creating benchmark client..." << std::endl;
  LuigiBenchmarkClient client(config);
  std::cout << "[TRACE]   - Initializing client..." << std::endl;
  if (!client.Initialize()) {
    std::cerr << "Failed to initialize benchmark client" << std::endl;
    luigi::stop_luigi_transport();
    luigiOwd.stop();
    return 1;
  }

  // Set local receiver for handling local shard requests directly (no RPC)
  client.GetLuigiClient()->SetLocalReceiver(luigi_receiver);

  std::cout << "[TRACE] Step 5: Client initialized and ready" << std::endl;

  // Print configuration
  std::cout << "\n========== Luigi Benchmark Configuration ==========\n";
  std::cout << "Config file:    " << config.config_file << "\n";
  std::cout << "Cluster:        " << config.cluster << "\n";
  std::cout << "Shard index:    " << config.shard_index << "\n";
  std::cout << "Num shards:     " << config.num_shards << "\n";
  std::cout << "Threads:        " << config.num_threads << "\n";
  std::cout << "Duration:       " << config.duration_sec << "s\n";
  std::cout << "Benchmark:      " << benchmark_type << "\n";
  if (benchmark_type == "micro" || benchmark_type == "micro_single") {
    std::cout << "Keys/shard:     " << keys_per_shard << "\n";
    std::cout << "Read ratio:     " << read_ratio << "\n";
    std::cout << "Ops/txn:        " << ops_per_txn << "\n";
  } else {
    std::cout << "Warehouses:     " << warehouses << "\n";
  }
  std::cout << "====================================================\n\n";

  // Wait for all shards to be ready using NFS barrier (like Mako)
  auto& benchCfg = BenchmarkConfig::getInstance();
  std::cout << "\n[SYNC] Waiting for all shards to be ready (nshards="
            << benchCfg.getNshards() << ", shard_idx=" << benchCfg.getShardIndex() << ")..." << std::endl;
  benchCfg.waitMultiShardBarrier();
  std::cout << "[SYNC] All shards ready!" << std::endl;

  // Run benchmark or test mode
  if (server_only) {
    // Server-only mode: just wait for termination
    std::cout << "\n[SERVER-ONLY MODE] Running as server only, no transactions will be sent." << std::endl;
    std::cout << "[SERVER-ONLY MODE] Waiting for " << config.duration_sec << " seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(config.duration_sec));
    std::cout << "[SERVER-ONLY MODE] Done waiting." << std::endl;
  } else if (test_one_txn) {
    std::cout << "\n[TEST MODE] Sending ONE cross-shard transaction..." << std::endl;
    bool success = client.TestOneCrossShardTransaction();
    std::cout << "[TEST MODE] Transaction " << (success ? "SUCCEEDED" : "FAILED") << std::endl;

    // Wait a bit for async processing to complete
    std::cout << "[TEST MODE] Waiting 5 seconds for processing..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
  } else {
    std::cout << "\n[TRACE] Step 6: Starting benchmark execution..." << std::endl;
    std::cout << "[TRACE]   - Benchmark type: " << benchmark_type << std::endl;
    std::cout << "[TRACE]   - Threads: " << config.num_threads << std::endl;
    std::cout << "[TRACE]   - Duration: " << config.duration_sec << "s" << std::endl;
    std::cout << "[TRACE] ========================================" << std::endl;

    BenchmarkStats stats;
    if (benchmark_type == "micro") {
      stats = client.RunMicroBenchmark();
    } else if (benchmark_type == "micro_single") {
      stats = client.RunSingleShardMicroBenchmark();
    } else if (benchmark_type == "tpcc") {
      stats = client.RunTPCCBenchmark();
    }

    std::cout << "\n[TRACE] Step 6: Benchmark execution completed" << std::endl;

    // Print results
    stats.Print();
  }

  // Cleanup
  std::cout << "Stopping Luigi helper threads..." << std::endl;
  janus::luigi::stop_luigi_helper();
  
  std::cout << "Stopping transport..." << std::endl;
  luigi::stop_luigi_transport();
  
    std::cout << "Stopping Luigi OWD..." << std::endl;
    luigiOwd.stop();
    
    delete luigi_receiver;

    return 0;
    
  } catch (int e) {
    std::cerr << "\n[MAIN] Caught int exception: " << e << " (likely Boost coroutine stack unwinding)\n";
    std::cerr << "[MAIN] This is expected behavior for Boost coroutines\n";
    return 0;  // Normal exit
  } catch (const std::exception& e) {
    std::cerr << "\n[MAIN] Exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cerr << "\n[MAIN] Unknown exception\n";
    return 1;
  }
}
