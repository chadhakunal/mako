#include "commo.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../config.h"
#include "../procedure.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"

namespace janus {

//=============================================================================
// Constructor - uses base Communicator to connect to all sites
//=============================================================================

LuigiCommo::LuigiCommo(rusty::Option<rusty::Arc<PollThread>> poll)
    : Communicator(poll) {
  // Base Communicator constructor handles connecting to all sites
  Log_info("LuigiCommo: initialized");
}

LuigiCommo::~LuigiCommo() {
  // LuigiProxy objects are lightweight wrappers around rrr::Client pointers
  // The clients are managed by rpc_clients_ in the base Communicator class
  // We don't own the proxies' memory, so just clear the map without deleting
  // Note: This creates a small memory leak of proxy objects, but prevents
  // the double-free error that was crashing the program before stats print
  luigi_proxies_.clear();
}

siteid_t LuigiCommo::GetLeaderSiteForShard(parid_t shard_id) {
  // Look up leader site for shard from config
  auto config = Config::GetConfig();
  if (config != nullptr) {
    auto sites = config->SitesByPartitionId(shard_id);
    if (!sites.empty()) {
      return sites[0].id; // First site is leader
    }
  }
  return shard_id; // Fallback: assume shard_id == site_id
}

//=============================================================================
// Helper to get or create LuigiProxy for a site
//=============================================================================

LuigiProxy *LuigiCommo::GetProxyForSite(siteid_t site_id) {
  // Check cache first
  auto it = luigi_proxies_.find(site_id);
  if (it != luigi_proxies_.end()) {
    return it->second;
  }

  // Create LuigiProxy on-demand from rpc_clients_
  auto client_it = rpc_clients_.find(site_id);
  if (client_it == rpc_clients_.end() || !client_it->second) {
    Log_warn("LuigiCommo: no client for site %d", site_id);
    return nullptr;
  }

  auto proxy =
      new LuigiProxy(const_cast<rrr::Client *>(client_it->second.get()));
  luigi_proxies_[site_id] = proxy;
  return proxy;
}

//=============================================================================
// Send Methods
//=============================================================================

shared_ptr<IntEvent> LuigiCommo::SendDispatch(
    siteid_t site_id, parid_t par_id, rrr::i64 txn_id, rrr::i64 expected_time,
    rrr::i32 worker_id, const std::vector<rrr::i32> &involved_shards,
    const std::string &ops_data, rrr::i32 *status, rrr::i64 *commit_timestamp,
    std::string *results_data) {

  auto ret = Reactor::CreateSpEvent<IntEvent>();
  auto proxy = GetProxyForSite(site_id);

  if (!proxy) {
    Log_warn("LuigiCommo::SendDispatch: no proxy for site %d", site_id);
    return ret;
  }

  FutureAttr fuattr;
  fuattr.callback = [ret, status, commit_timestamp,
                     results_data](rusty::Arc<Future> fu) {
    if (fu->get_error_code() != 0) {
      Log_info("Luigi Dispatch RPC error: %d", fu->get_error_code());
      return;
    }
    fu->get_reply() >> *status;
    fu->get_reply() >> *commit_timestamp;
    fu->get_reply() >> *results_data;
    ret->Set(1);
  };

  auto fu_result = proxy->async_Dispatch(txn_id, expected_time, worker_id,
                                         involved_shards, ops_data, fuattr);
  if (fu_result.is_err()) {
    Log_warn("Luigi Dispatch RPC failed: %d", fu_result.unwrap_err());
  }

  return ret;
}

shared_ptr<IntEvent> LuigiCommo::SendOwdPing(siteid_t site_id, parid_t par_id,
                                             rrr::i64 send_time,
                                             rrr::i32 *status) {
  auto ret = Reactor::CreateSpEvent<IntEvent>();
  auto proxy = GetProxyForSite(site_id);

  if (!proxy)
    return ret;

  FutureAttr fuattr;
  fuattr.callback = [ret, status](rusty::Arc<Future> fu) {
    if (fu->get_error_code() != 0)
      return;
    fu->get_reply() >> *status;
    ret->Set(1);
  };

  proxy->async_OwdPing(send_time, fuattr);
  return ret;
}

shared_ptr<IntEvent>
LuigiCommo::SendDeadlinePropose(siteid_t site_id, parid_t par_id, rrr::i64 tid,
                                rrr::i32 src_shard, rrr::i64 proposed_ts,
                                rrr::i32 *status) {

  auto ret = Reactor::CreateSpEvent<IntEvent>();
  auto proxy = GetProxyForSite(site_id);

  if (!proxy)
    return ret;

  FutureAttr fuattr;
  fuattr.callback = [ret, status](rusty::Arc<Future> fu) {
    if (fu->get_error_code() != 0)
      return;
    fu->get_reply() >> *status;
    ret->Set(1);
  };

  proxy->async_DeadlinePropose(tid, src_shard, proposed_ts, fuattr);
  return ret;
}

shared_ptr<IntEvent>
LuigiCommo::SendDeadlineConfirm(siteid_t site_id, parid_t par_id, rrr::i64 tid,
                                rrr::i32 src_shard, rrr::i64 agreed_ts,
                                rrr::i32 *status) {

  auto ret = Reactor::CreateSpEvent<IntEvent>();
  auto proxy = GetProxyForSite(site_id);

  if (!proxy)
    return ret;

  FutureAttr fuattr;
  fuattr.callback = [ret, status](rusty::Arc<Future> fu) {
    if (fu->get_error_code() != 0)
      return;
    fu->get_reply() >> *status;
    ret->Set(1);
  };

  proxy->async_DeadlineConfirm(tid, src_shard, agreed_ts, fuattr);
  return ret;
}

shared_ptr<IntEvent> LuigiCommo::SendWatermarkExchange(
    siteid_t site_id, parid_t par_id, rrr::i32 src_shard,
    const std::vector<rrr::i64> &watermarks, rrr::i32 *status) {

  auto ret = Reactor::CreateSpEvent<IntEvent>();
  auto proxy = GetProxyForSite(site_id);

  if (!proxy)
    return ret;

  FutureAttr fuattr;
  fuattr.callback = [ret, status](rusty::Arc<Future> fu) {
    if (fu->get_error_code() != 0)
      return;
    fu->get_reply() >> *status;
    ret->Set(1);
  };

  proxy->async_WatermarkExchange(src_shard, watermarks, fuattr);
  return ret;
}

//=============================================================================
// Sync Methods (blocking, for use from non-reactor threads)
// Uses fu->wait() pattern like hello_client.cc
//=============================================================================

bool LuigiCommo::OwdPingSync(parid_t shard_id, rrr::i64 send_time,
                             rrr::i32 *status) {
  // Get leader site for this shard (handles multi-replica mapping)
  auto proxy = GetProxyForSite(GetLeaderSiteForShard(shard_id));

  if (!proxy) {
    Log_warn("OwdPingSync: no proxy for leader of shard %d", shard_id);
    return false;
  }

  // Use async without callback, then wait on the future
  auto result = proxy->async_OwdPing(send_time);
  if (result.is_err()) {
    Log_warn("OwdPingSync: async_OwdPing failed for shard %d", shard_id);
    return false;
  }

  auto fu = result.unwrap();
  fu->wait();

  if (fu->get_error_code() == 0) {
    fu->get_reply() >> *status;
    return *status == 0;
  } else {
    *status = -1;
    return false;
  }
}

void LuigiCommo::OwdPingAsync(parid_t shard_id, rrr::i64 send_time,
                              OwdPingCallback callback) {
  // Get leader site for this shard (handles multi-replica mapping)
  auto proxy = GetProxyForSite(GetLeaderSiteForShard(shard_id));

  if (!proxy) {
    Log_warn("OwdPingAsync: no proxy for leader of shard %d", shard_id);
    callback(false, -1);
    return;
  }

  FutureAttr fuattr;
  fuattr.callback = [callback](rusty::Arc<Future> fu) {
    if (fu->get_error_code() != 0) {
      callback(false, -1);
      return;
    }
    rrr::i32 status;
    fu->get_reply() >> status;
    callback(true, status);
  };

  auto result = proxy->async_OwdPing(send_time, fuattr);
  if (result.is_err()) {
    Log_warn("OwdPingAsync: async_OwdPing failed for shard %d", shard_id);
    callback(false, -1);
  }
}

bool LuigiCommo::DispatchSync(parid_t shard_id, rrr::i64 txn_id,
                              rrr::i64 expected_time, rrr::i32 worker_id,
                              const std::vector<rrr::i32> &involved_shards,
                              const std::string &ops_data, rrr::i32 *status,
                              rrr::i64 *commit_timestamp,
                              std::string *results_data) {
  // Get leader site for this shard (handles multi-replica mapping)
  auto proxy = GetProxyForSite(GetLeaderSiteForShard(shard_id));

  if (!proxy) {
    Log_warn("DispatchSync: no proxy for leader of shard %d", shard_id);
    return false;
  }

  // Use async without callback, then wait on the future
  Log_debug("DispatchSync: calling async_Dispatch txn_id=%ld worker=%d", txn_id,
            worker_id);
  auto result = proxy->async_Dispatch(txn_id, expected_time, worker_id,
                                      involved_shards, ops_data);
  if (result.is_err()) {
    Log_warn("DispatchSync: async_Dispatch failed for shard %d", shard_id);
    return false;
  }

  auto fu = result.unwrap();
  Log_debug("DispatchSync: future created, waiting...");
  fu->wait();
  Log_debug("DispatchSync: future returned, error_code=%d",
            fu->get_error_code());

  if (fu->get_error_code() == 0) {
    fu->get_reply() >> *status;
    fu->get_reply() >> *commit_timestamp;
    fu->get_reply() >> *results_data;
    return *status == 0;
  } else {
    *status = -1;
    return false;
  }
}

//=============================================================================
// Async Dispatch (non-blocking, callback-based)
//=============================================================================

void LuigiCommo::DispatchAsync(parid_t shard_id, rrr::i64 txn_id,
                               rrr::i64 expected_time, rrr::i32 worker_id,
                               const std::vector<rrr::i32> &involved_shards,
                               const std::string &ops_data,
                               DispatchCallback callback) {
  // Get leader site for this shard (handles multi-replica mapping)
  auto proxy = GetProxyForSite(GetLeaderSiteForShard(shard_id));

  if (!proxy) {
    Log_warn("DispatchAsync: no proxy for leader of shard %d", shard_id);
    callback(false, -1, 0, "");
    return;
  }

  FutureAttr fuattr;
  fuattr.callback = [callback, txn_id, shard_id](rusty::Arc<Future> fu) {
    int error_code = fu->get_error_code();
    if (error_code != 0) {
      Log_warn("DispatchAsync callback: txn=%ld shard=%d RPC error=%d", txn_id,
               shard_id, error_code);
      callback(false, -1, 0, "");
      return;
    }
    rrr::i32 status;
    rrr::i64 commit_ts;
    std::string results;
    fu->get_reply() >> status;
    fu->get_reply() >> commit_ts;
    fu->get_reply() >> results;
    Log_debug("DispatchAsync callback: txn=%ld shard=%d success status=%d",
              txn_id, shard_id, status);
    callback(true, status, commit_ts, results);
  };

  Log_debug("DispatchAsync: calling async_Dispatch txn_id=%ld worker=%d "
            "shard=%d",
            txn_id, worker_id, shard_id);
  proxy->async_Dispatch(txn_id, expected_time, worker_id, involved_shards,
                        ops_data, fuattr);
}

//=============================================================================
// Broadcast Helpers - target appropriate entities
//=============================================================================

void LuigiCommo::BroadcastOwdPing(int64_t send_time,
                                  const std::vector<uint32_t> &shard_ids) {
  for (uint32_t shard : shard_ids) {
    // Get leader site for this shard (handles multi-replica mapping)
    siteid_t leader_site = GetLeaderSiteForShard(shard);
    rrr::i32 status;
    SendOwdPing(leader_site, shard, send_time, &status);
  }
}

void LuigiCommo::BroadcastDeadlinePropose(
    uint64_t tid, int32_t src_shard, int64_t proposed_ts,
    const std::vector<uint32_t> &involved_shards) {
  for (uint32_t shard : involved_shards) {
    // Target: leaders of other shards only (exclude self)
    if (shard == static_cast<uint32_t>(src_shard))
      continue;

    // Get leader site for this shard (handles multi-replica mapping)
    siteid_t leader_site = GetLeaderSiteForShard(shard);
    rrr::i32 status;
    SendDeadlinePropose(leader_site, shard, tid, src_shard, proposed_ts, &status);
  }
}

void LuigiCommo::BroadcastDeadlineConfirm(
    uint64_t tid, int32_t src_shard, int64_t agreed_ts,
    const std::vector<uint32_t> &involved_shards) {
  for (uint32_t shard : involved_shards) {
    // Target: leaders only (exclude self)
    if (shard == static_cast<uint32_t>(src_shard))
      continue;

    // Get leader site for this shard (handles multi-replica mapping)
    siteid_t leader_site = GetLeaderSiteForShard(shard);
    rrr::i32 status;
    SendDeadlineConfirm(leader_site, shard, tid, src_shard, agreed_ts, &status);
  }
}

void LuigiCommo::BroadcastWatermarkExchange(
    int32_t src_shard, const std::vector<int64_t> &watermarks,
    const std::vector<uint32_t> &involved_shards) {
  for (uint32_t shard : involved_shards) {
    // Get leader site for this shard (handles multi-replica mapping)
    siteid_t leader_site = GetLeaderSiteForShard(shard);
    rrr::i32 status;
    SendWatermarkExchange(leader_site, shard, src_shard, watermarks, &status);
  }
}

void LuigiCommo::BroadcastWatermarks(int32_t src_shard,
                                     const std::vector<int64_t> &watermarks) {
  // Send watermarks to leaders of all other shards
  auto config = Config::GetConfig();
  uint32_t num_partitions = config->GetNumPartition();
  for (uint32_t shard = 0; shard < num_partitions; ++shard) {
    if (shard != static_cast<uint32_t>(src_shard)) {
      rrr::i32 status;
      // Use GetLeaderSiteForShard to get correct leader site_id
      siteid_t leader_site = GetLeaderSiteForShard(shard);
      SendWatermarkExchange(leader_site, shard, src_shard, watermarks, &status);
    }
  }

  Log_debug("BroadcastWatermarks: shard=%d sent watermarks to all shards",
            src_shard);
}

//=============================================================================
// PHASE 2: BATCH BROADCAST IMPLEMENTATIONS
//=============================================================================

void LuigiCommo::BroadcastDeadlineBatchPropose(
    const std::vector<rrr::i64> &tids, int32_t src_shard,
    const std::vector<rrr::i64> &proposed_timestamps,
    const std::vector<rrr::i64> &watermarks,
    const std::vector<uint32_t> &involved_shards) {
  auto config = Config::GetConfig();
  for (uint32_t shard : involved_shards) {
    // Target: leaders of other shards only (exclude self)
    if (shard == static_cast<uint32_t>(src_shard))
      continue;
    // Get leader site for this shard (handles multi-replica mapping)
    auto proxy = GetProxyForSite(GetLeaderSiteForShard(shard));

    if (!proxy) {
      Log_warn(
          "LuigiCommo::BroadcastDeadlineBatchPropose: no proxy for shard %d",
          shard);
      continue;
    }

    // Fire-and-forget async RPC (includes watermarks)
    rrr::FutureAttr fuattr;
    fuattr.callback = [](rusty::Arc<rrr::Future> fu) {
      // No-op callback for fire-and-forget
      if (fu->get_error_code() != 0) {
        Log_debug("DeadlineBatchPropose RPC error: %d", fu->get_error_code());
      }
    };

    auto future = proxy->async_DeadlineBatchPropose(
        tids, src_shard, proposed_timestamps, watermarks, fuattr);
    rrr::Future::safe_release(future);
  }
}

void LuigiCommo::BroadcastDeadlineBatchConfirm(
    const std::vector<rrr::i64> &tids, int32_t src_shard,
    const std::vector<rrr::i64> &agreed_timestamps,
    const std::vector<uint32_t> &involved_shards) {
  auto config = Config::GetConfig();
  for (uint32_t shard : involved_shards) {
    // Target: leaders of other shards only (exclude self)
    if (shard == static_cast<uint32_t>(src_shard))
      continue;

    // Get leader site for this shard (handles multi-replica mapping)
    auto proxy = GetProxyForSite(GetLeaderSiteForShard(shard));

    if (!proxy) {
      Log_warn(
          "LuigiCommo::BroadcastDeadlineBatchConfirm: no proxy for shard %d",
          shard);
      continue;
    }

    // Fire-and-forget async RPC
    rrr::FutureAttr fuattr;
    fuattr.callback = [](rusty::Arc<rrr::Future> fu) {
      // No-op callback for fire-and-forget
      if (fu->get_error_code() != 0) {
        Log_debug("DeadlineBatchConfirm RPC error: %d", fu->get_error_code());
      }
    };

    auto future = proxy->async_DeadlineBatchConfirm(tids, src_shard,
                                                    agreed_timestamps, fuattr);
    rrr::Future::safe_release(future);
  }
}

//=============================================================================
// PHASE 4: REPLICATION TO FOLLOWERS
//=============================================================================

shared_ptr<IntEvent>
LuigiCommo::SendReplicate(siteid_t site_id, rrr::i32 worker_id,
                          rrr::i64 slot_id, rrr::i64 txn_id, rrr::i64 timestamp,
                          const std::string &log_data, rrr::i32 *status) {
  auto ret = Reactor::CreateSpEvent<IntEvent>();
  auto proxy = GetProxyForSite(site_id);

  if (!proxy) {
    Log_warn("LuigiCommo::SendReplicate: no proxy for site %d", site_id);
    return ret;
  }

  FutureAttr fuattr;
  fuattr.callback = [ret, status](rusty::Arc<Future> fu) {
    if (fu->get_error_code() != 0) {
      Log_debug("Replicate RPC error: %d", fu->get_error_code());
      return;
    }
    fu->get_reply() >> *status;
    ret->Set(1);
  };

  auto result = proxy->async_Replicate(worker_id, slot_id, txn_id, timestamp,
                                       log_data, fuattr);
  if (result.is_err()) {
    Log_warn("SendReplicate: async_Replicate failed for site %d", site_id);
  }

  return ret;
}

void LuigiCommo::ReplicateAsync(siteid_t follower_site_id, rrr::i32 worker_id,
                                rrr::i64 slot_id, rrr::i64 txn_id,
                                rrr::i64 timestamp, const std::string &log_data,
                                ReplicateCallback callback) {
  auto proxy = GetProxyForSite(follower_site_id);

  if (!proxy) {
    Log_warn("LuigiCommo::ReplicateAsync: no proxy for follower site %d",
             follower_site_id);
    callback(false, -1);
    return;
  }

  FutureAttr fuattr;
  fuattr.callback = [callback, follower_site_id](rusty::Arc<Future> fu) {
    int error_code = fu->get_error_code();
    if (error_code != 0) {
      Log_debug("ReplicateAsync callback: follower=%d RPC error=%d",
                follower_site_id, error_code);
      callback(false, -1);
      return;
    }
    rrr::i32 status;
    fu->get_reply() >> status;
    Log_debug("ReplicateAsync callback: follower=%d success status=%d",
              follower_site_id, status);
    callback(true, status);
  };

  Log_debug("ReplicateAsync: sending to follower=%d worker=%d slot=%ld txn=%ld",
            follower_site_id, worker_id, slot_id, txn_id);
  auto result = proxy->async_Replicate(worker_id, slot_id, txn_id, timestamp,
                                       log_data, fuattr);
  if (result.is_err()) {
    Log_warn("ReplicateAsync: async_Replicate failed for follower %d",
             follower_site_id);
    callback(false, -1);
  }
}

void LuigiCommo::BatchReplicateAsync(
    siteid_t follower_site_id, rrr::i32 worker_id, rrr::i64 prev_committed_slot,
    const std::vector<rrr::i64> &slot_ids, const std::vector<rrr::i64> &txn_ids,
    const std::vector<rrr::i64> &timestamps,
    const std::vector<std::string> &log_entries,
    BatchReplicateCallback callback) {
  auto proxy = GetProxyForSite(follower_site_id);

  if (!proxy) {
    Log_warn("LuigiCommo::BatchReplicateAsync: no proxy for follower site %d",
             follower_site_id);
    callback(false, -1, 0);
    return;
  }

  FutureAttr fuattr;
  fuattr.callback = [callback, follower_site_id](rusty::Arc<Future> fu) {
    int error_code = fu->get_error_code();
    if (error_code != 0) {
      Log_debug("BatchReplicateAsync callback: follower=%d RPC error=%d",
                follower_site_id, error_code);
      callback(false, -1, 0);
      return;
    }
    rrr::i32 status;
    rrr::i64 last_appended_slot;
    fu->get_reply() >> status;
    fu->get_reply() >> last_appended_slot;
    Log_debug(
        "BatchReplicateAsync callback: follower=%d status=%d last_slot=%ld",
        follower_site_id, status, last_appended_slot);
    callback(true, status, last_appended_slot);
  };

  Log_debug("BatchReplicateAsync: sending %zu entries to follower=%d worker=%d",
            slot_ids.size(), follower_site_id, worker_id);
  auto result =
      proxy->async_BatchReplicate(worker_id, prev_committed_slot, slot_ids,
                                  txn_ids, timestamps, log_entries, fuattr);
  if (result.is_err()) {
    Log_warn("BatchReplicateAsync: async_BatchReplicate failed for follower %d",
             follower_site_id);
    callback(false, -1, 0);
  }
}

} // namespace janus
