// Initialize the local Luigi scheduler for single-shard mode
void init_local_luigi_scheduler();
// RPC setup helpers extracted from tpcc.cc
#ifndef MAKO_BENCHMARKS_RPC_SETUP_H
#define MAKO_BENCHMARKS_RPC_SETUP_H

#include <thread>
#include <vector>
#include <unordered_map>
#include <map>
#include <atomic>
#include <string>

#include "bench.h"
#include <unordered_map>

// Decoupled setup helpers for RPC benchmark: eRPC server and helper threads.
// These functions mirror the original logic in tpcc.cc but are now reusable.

namespace mako {

// Launch helper threads for all remote warehouses across shards.
void setup_helper(
  abstract_db *db,
  const std::map<int, abstract_ordered_index *> &open_tables);

// Add or update a table mapping for already running helper threads.
void setup_update_table(int table_id, abstract_ordered_index *table);

// Signal helper threads to stop processing requests.
void stop_helper();

// Launch eRPC server threads and wire up per-warehouse queues.
void setup_erpc_server();

// Stop all eRPC servers previously started by setup_erpc_server().
void stop_erpc_server();

// Initialize per thread
void initialize_per_thread(abstract_db *db) ;

// Setup Luigi RPC for multi-shard agreement protocol
// Call this after both setup_erpc_server() and setup_helper() complete
void setup_luigi_rpc();

// Register tables with local Luigi scheduler (for single-shard mode)
void setup_luigi_tables(const std::map<int, abstract_ordered_index*>& open_tables, abstract_db* db);

// Stop local Luigi scheduler
void stop_local_luigi();

} // namespace mako

// Forward declaration for Luigi scheduler access
namespace janus {
class SchedulerLuigi;
struct LuigiOp;
}

namespace mako {

// Get Luigi scheduler for local dispatch (multi-shard mode only)
// Returns nullptr for single-shard mode - use local_luigi_execute() instead
janus::SchedulerLuigi* get_local_luigi_scheduler();

// Direct local Luigi execution for single-shard mode
// Executes operations directly on local tables without scheduler overhead
int local_luigi_execute(
    const std::vector<janus::LuigiOp>& ops,
    uint64_t expected_time,
    uint64_t& out_commit_ts,
    std::vector<std::string>& out_read_results);

} // namespace mako

#endif // MAKO_BENCHMARKS_RPC_SETUP_H
