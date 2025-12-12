
#include <iostream>
#include <thread>
#include <chrono>
#include "lib/fasttransport.h"
#include "lib/promise.h"
#include "lib/client.h"
#include "lib/shardClient.h"
#include "lib/configuration.h"
#include "lib/common.h"
#include "benchmarks/sto/Interface.hh"

namespace mako
{
    using namespace std;

    /**
     * file: configuration fileName
     * shardIndex: at which shard the running client locates
     * par_id: to distinguish the running thread
     */
    ShardClient::ShardClient(std::string file,
                             std::string cluster,
                             int shardIndex,
                             int par_id) : config(file), cluster(cluster), shardIndex(shardIndex), par_id(par_id)
    {
        clusterRole = mako::convertCluster(cluster);
        std::string local_uri = config.shard(shardIndex, clusterRole).host;
        int id=par_id;
        // 0. initialize transport
        transport = new FastTransport(file,
                                      local_uri, // local_uri
                                      cluster,
                                      1, 0,       // nr_req_types (for client, setup to 0)
                                      0,       // physPort
                                      0, // shardIndex % 2 // numa node
                                      shardIndex,
                                      id);

        // 1. initialize Client
        client = new mako::Client(config.configFile,
                                    transport,
                                    0); // 0 => generate a random client-id

        tid=0;
        int_received.resize(TThread::get_nshards());
        stopped = false;
        isBreakTimeout = false;
        isBlocking = true; // If there is a timeout, we can't abort it, we should retry it util it is successful.
    }

    void ShardClient::stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        auto *ftport = static_cast<FastTransport *>(transport);
        ftport->Stop();
    }

    void ShardClient::setBreakTimeout(bool bt=false) {
        FastTransport *ftport= (FastTransport *)transport;
        ftport->setBreakTimeout(bt);
        isBreakTimeout=bt;
    }

    void ShardClient::setBlocking(bool pd=false) {
        isBlocking=pd;
    }

    bool ShardClient::getBreakTimeout() {
        return isBreakTimeout;
    }


    void ShardClient::GetCallback(char *respBuf) {
        /* Replies back from a shard. */
        auto *resp = reinterpret_cast<mako::get_response_t *>(respBuf);
        if (waiting != NULL) {
            Promise *w = waiting;
            waiting = NULL;
            w->Reply(resp->status, std::string(resp->value, resp->len));
        } else {
            Debug("Waiting is null!");
        }
    }

    void ShardClient::ScanCallback(char *respBuf) {
        /* Replies back from a shard. */
        auto *resp = reinterpret_cast<mako::scan_response_t *>(respBuf);
        if (waiting != NULL) {
            Promise *w = waiting;
            waiting = NULL;
            w->Reply(resp->status, std::string(resp->value, resp->len));
        } else {
            Debug("Waiting is null!");
        }
    }

    void ShardClient::BasicCallBack(char *respBuf) {
        /* Replies back from a shard. */
        auto *resp = reinterpret_cast<mako::basic_response_t *>(respBuf);
        if (waiting != NULL) {
            Promise *w = waiting;
            waiting = NULL;
            w->Reply(resp->status);
        } else {
            Debug("Waiting is null!");
        }
    }

    void ShardClient::GiveUpTimeout() {
        Debug("GiveupTimeout called.");
        if (waiting != nullptr) {
            Promise *w = waiting;
            waiting = nullptr;
            w->Reply(MakoErrorCode::TIMEOUT);
        }
    }

    void ShardClient::SendToAllStatusCallBack(char *respBuf) {
        auto *resp = reinterpret_cast<mako::basic_response_t *>(respBuf);
        status_received.push_back((int) resp->status);
    }

    void ShardClient::SendToAllIntCallBack(char *respBuf) {
        auto *resp = reinterpret_cast<mako::get_int_response_t *>(respBuf);
        status_received.push_back((int) resp->status);
        if (resp->shard_index>=TThread::get_nshards()||resp->shard_index<0){
            Warning("In SendToAllIntCallBack, the shard_idx is overflow: %d", resp->shard_index);
        }else{
            int_received[resp->shard_index] = resp->result;
        }
    }

    void ShardClient::SendToAllGiveUpTimeout() {
        status_received.push_back((int) MakoErrorCode::TIMEOUT);
    }

    bool ShardClient::is_all_response_ok() {
        bool ok = true;
        for (auto code: status_received) ok &= (code == MakoErrorCode::OK);
        status_received.clear();
        for (int i=0;i<(int)int_received.size(); i++)
            int_received[i] = 0;
        return ok ? MakoErrorCode::OK : MakoErrorCode::ERROR;
    }

    void ShardClient::calculate_num_response_waiting(int shards_to_send_bits) {
        int num_response_waiting = 0;
        for (int dstShardIndex = 0; dstShardIndex < config.nshards; dstShardIndex++) {
            if (dstShardIndex == shardIndex) continue;
            if ((shards_to_send_bits >> dstShardIndex) % 2 == 0) continue;
            num_response_waiting ++;
        }
        client->SetNumResponseWaiting(num_response_waiting);
    }

    // without skipping
    void ShardClient::calculate_num_response_waiting_no_skip(int shards_to_send_bits) {
        int num_response_waiting = 0;
        for (int dstShardIndex = 0; dstShardIndex < config.nshards; dstShardIndex++) {
            if ((shards_to_send_bits >> dstShardIndex) % 2 == 0) continue;
            num_response_waiting ++;
        }
        client->SetNumResponseWaiting(num_response_waiting);
    }


    int ShardClient::remoteScan(int remote_table_id, std::string start_key, std::string end_key, std::string &value) {

        int table_id = remote_table_id;
        int dstShardIndex = (remote_table_id - 1)/ mako::NUM_TABLES_PER_SHARD;

        TThread::readset_shard_bits |= (1 << dstShardIndex);
        Promise promise(GET_TIMEOUT);
        waiting = &promise;

        const int timeout = promise.GetTimeout();
        uint16_t server_id = shardIndex*config.warehouses+par_id;

        client->SetNumResponseWaiting(1);

        client->InvokeScan(++tid,  // txn_nr
                    dstShardIndex,  // shardIdx
                    server_id,
                    start_key, 
                    end_key,
                    table_id,
                    bind(&ShardClient::ScanCallback, this,
                        placeholders::_1),
                    bind(&ShardClient::GiveUpTimeout, this),
                timeout);
        value = promise.GetValue();
        int ret = promise.GetReply();
        if (ret>0){
            TThread::trans_nosend_abort |= (1 << dstShardIndex);
        }
        return ret;
    }

    void ShardClient::statistics() {
        //Warning("Info for current shardClient, shardIdx: %d, cluster: %s, par_id: %d", shardIndex, cluster.c_str(), par_id);
        transport->Statistics();
    }

    int ShardClient::remoteGet(int remote_table_id, std::string key, std::string &value) {
        
        int table_id = remote_table_id;
        int dstShardIndex = (remote_table_id - 1)/ mako::NUM_TABLES_PER_SHARD;

        TThread::readset_shard_bits |= (1 << dstShardIndex) ;
        Promise promise(GET_TIMEOUT);
        waiting = &promise;

        client->SetNumResponseWaiting(1);

        const int timeout = promise.GetTimeout();
        uint16_t server_id = shardIndex*config.warehouses+par_id;

        client->InvokeGet(++tid,  // txn_nr
                    dstShardIndex,  // shardIdx
                    server_id,
                    key, 
                    table_id,
                    bind(&ShardClient::GetCallback, this,
                        placeholders::_1),
                    bind(&ShardClient::GiveUpTimeout, this),
                timeout);
        //Warning("remoteGET: key:%s,table_id:%d,key_len:%d",mako::printStringAsBit(key).c_str(),table_id,key.length());
        value = promise.GetValue();
        int ret = promise.GetReply();
        if (ret>0){
            TThread::trans_nosend_abort |= (1 << dstShardIndex);
        }
        return ret;
    }

    int ShardClient::remoteBatchLock(
        vector<int> &remote_table_id_batch,
        vector<string> &key_batch,
        vector<string> &value_batch
    ) {
        if (remote_table_id_batch.empty())
            return MakoErrorCode::OK;

        map<int, BatchLockRequestWrapper> request_batch_per_shard;
        uint16_t server_id = shardIndex * config.warehouses + par_id;
        int shards_to_send_bits = 0;
        for (int i = 0; i < remote_table_id_batch.size(); i++) {
            int remote_table_id = remote_table_id_batch[i];
            int table_id = remote_table_id;
            int dst_shard_idx = (remote_table_id - 1)/ mako::NUM_TABLES_PER_SHARD;

            // after combine remoteLock + remoteValidate, this step might need to be skipped
            TThread::writeset_shard_bits |= (1 << dst_shard_idx) ;
            
            shards_to_send_bits |= (1 << dst_shard_idx);
            request_batch_per_shard[dst_shard_idx].add_request(key_batch[i], value_batch[i], table_id, server_id);
        }

        Promise promise(BASIC_TIMEOUT);
        waiting = &promise;
        
        const int timeout = promise.GetTimeout();
        calculate_num_response_waiting(shards_to_send_bits);
        client->InvokeBatchLock(
            ++tid,
            server_id,
            request_batch_per_shard,
            bind(&ShardClient::SendToAllStatusCallBack, this, placeholders::_1),
            bind(&ShardClient::SendToAllGiveUpTimeout, this),
            timeout
        );

        return is_all_response_ok();
    }

    int ShardClient::remoteLock(int remote_table_id, std::string key, std::string &value) {
        Panic("Deprecated!");

        int table_id = remote_table_id;
        int dstShardIndex = (remote_table_id - 1)/ mako::NUM_TABLES_PER_SHARD;
        
        TThread::writeset_shard_bits |= (1 << dstShardIndex) ;
        Promise promise(BASIC_TIMEOUT);
        waiting = &promise;

        client->SetNumResponseWaiting(1);

        const int timeout = promise.GetTimeout();
        uint16_t server_id = shardIndex*config.warehouses+par_id;

        client->InvokeLock(++tid,  // txn_nr
                    dstShardIndex,  // shardIdx
                    server_id,
                    key,
                    value,
                    table_id,
                    bind(&ShardClient::BasicCallBack, this,
                        placeholders::_1),
                    bind(&ShardClient::GiveUpTimeout, this),
                timeout);
        return promise.GetReply();
    }

    int ShardClient::remoteValidate(uint32_t &watermark) {
        int shards_to_send_bits = TThread::writeset_shard_bits;
        if (!shards_to_send_bits) return MakoErrorCode::OK;
        calculate_num_response_waiting(shards_to_send_bits);
        uint16_t server_id = shardIndex * config.warehouses + par_id;

        for (int i=0;i<int_received.size();i++) int_received[i]=0;
        client->InvokeValidate(++tid,  // txn_nr
                                shards_to_send_bits,
                                server_id,
                                bind(&ShardClient::SendToAllIntCallBack, this, placeholders::_1),
                                bind(&ShardClient::SendToAllGiveUpTimeout, this),
                                BASIC_TIMEOUT);
        // Single timestamp system: use maximum watermark from all shards
        watermark = 0;
        for (int i=0; i<(int)int_received.size(); i++) {
            if (int_received[i] > watermark) {
                watermark = int_received[i];
            }
        }
        return is_all_response_ok();
    }

    int ShardClient::remoteInstall(uint32_t timestamp) {
        // Single timestamp encoding - no vector needed
        char *cc = encode_single_timestamp(timestamp);
        int shards_to_send_bits = TThread::writeset_shard_bits;
        if (!shards_to_send_bits) return MakoErrorCode::OK;
        calculate_num_response_waiting(shards_to_send_bits);
        uint16_t server_id = shardIndex * config.warehouses + par_id;

        client->InvokeInstall(++tid,  // txn_nr
                            shards_to_send_bits,
                            server_id,
                            cc,
                            bind(&ShardClient::SendToAllStatusCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            BASIC_TIMEOUT);
        free(cc);
        return is_all_response_ok();
    }

    int ShardClient::warmupRequest(uint32_t req_val, uint8_t centerId, uint32_t &ret_value, uint64_t set_bits) {
        calculate_num_response_waiting_no_skip(set_bits);
        uint16_t server_id = req_val; // we don't forward to a helper queue;

        for (int i=0;i<int_received.size();i++) int_received[i]=0;
        client->InvokeWarmup(++tid,  // txn_nr
                            req_val,
                            centerId,
                            set_bits,
                            server_id,
                            bind(&ShardClient::SendToAllIntCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            BASIC_TIMEOUT);
        ret_value = 0;
        for (int i=0; i<(int)int_received.size(); i++) {
            ret_value += int_received[i];
        }
        return is_all_response_ok();
    }

    int ShardClient::checkRemoteShardReady(int dstShardIndex) {
        // Use warmup mechanism to ping a specific remote shard
        // If the shard responds, it's ready; otherwise timeout/error

        return mako::ErrorCode::SUCCESS;

        // TO FIX: a server is ready on other shards, but this warmup rpc is frequently TIMEOUT!
        /*
        uint32_t ret_value = 0;
        uint64_t set_bits = (1ULL << dstShardIndex);  // Target only this shard
        uint8_t centerId = clusterRole;  // Use our cluster role

        // Use a shorter timeout for readiness check (1 second)
        calculate_num_response_waiting_no_skip(set_bits);
        uint16_t server_id = 0;  // Readiness check doesn't need specific server

        for (int i=0; i<(int)int_received.size(); i++) int_received[i]=0;
        try {
            client->InvokeWarmup(++tid,
                                0,  // req_val = 0 for readiness check
                                centerId,
                                set_bits,
                                server_id,
                                bind(&ShardClient::SendToAllIntCallBack, this, placeholders::_1),
                                bind(&ShardClient::SendToAllGiveUpTimeout, this),
                                1000);  // 1 second timeout for readiness check
        } catch (int n) {
            Warning("Timeout on InvokeWarmup with error-no:%d!", n);
            return mako::ErrorCode::TIMEOUT;
        }
        return is_all_response_ok(); */
    }

    void ShardClient::remotePingForOWD() {
        // Ping all remote shards to measure RTT and update OWD table
        auto& luigiOwd = luigi::LuigiOWD::getInstance();
        if (!luigiOwd.isInitialized()) {
            return;  // OWD service not initialized
        }

        auto remote_shards = luigiOwd.getRemoteShards();
        uint8_t centerId = clusterRole;
        uint16_t server_id = 0;

        for (int dst_shard : remote_shards) {
            uint64_t set_bits = (1ULL << dst_shard);
            
            // Record start time
            auto start_time = std::chrono::steady_clock::now();
            
            // Send ping (reuse warmup mechanism)
            calculate_num_response_waiting_no_skip(set_bits);
            for (int i = 0; i < (int)int_received.size(); i++) int_received[i] = 0;
            
            client->InvokeWarmup(++tid,
                                0,  // req_val = 0 for ping
                                centerId,
                                set_bits,
                                server_id,
                                bind(&ShardClient::SendToAllIntCallBack, this, placeholders::_1),
                                bind(&ShardClient::SendToAllGiveUpTimeout, this),
                                BASIC_TIMEOUT);
            
            // Calculate RTT
            auto end_time = std::chrono::steady_clock::now();
            uint64_t rtt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time
            ).count();
            
            // Only update OWD if ping succeeded
            if (is_all_response_ok()) {
                luigiOwd.updateOWD(dst_shard, rtt_ms);
            }
        }
    }

    int ShardClient::remoteControl(int control, uint32_t value, uint32_t &ret_value, uint64_t set_bits) {
        calculate_num_response_waiting_no_skip(set_bits);
        uint16_t server_id = 0; // to locate which helper_queue

        for (int i=0;i<int_received.size();i++) int_received[i]=0;
        client->InvokeControl(++tid,  // txn_nr
                            control,
                            value,
                            set_bits,
                            server_id,
                            bind(&ShardClient::SendToAllIntCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            BASIC_TIMEOUT);
        ret_value = 0;
        for (int i=0; i<(int)int_received.size(); i++) {
            ret_value += int_received[i];
        }
        return is_all_response_ok(); 
    }

    int ShardClient::remoteExchangeWatermark(uint32_t &watermark, uint64_t set_bits) {
        calculate_num_response_waiting(set_bits);
        uint16_t server_id = 0; // to locate which helper_queue, does not matter

        for (int i=0;i<int_received.size();i++) int_received[i]=0;
        client->InvokeExchangeWatermark(++tid,  // txn_nr
                            set_bits,
                            server_id,
                            bind(&ShardClient::SendToAllIntCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            BASIC_TIMEOUT);
        // Single timestamp system: use maximum watermark from all shards
        watermark = 0;
        for (int i=0; i<(int)int_received.size(); i++) {
            if (int_received[i] > watermark) {
                watermark = int_received[i];
            }
        }
        return is_all_response_ok();
    }

    int ShardClient::remoteUnLock() {
        int shards_to_send_bits = TThread::writeset_shard_bits;
        if (!shards_to_send_bits) return MakoErrorCode::OK;
        calculate_num_response_waiting(shards_to_send_bits);
        uint16_t server_id = shardIndex * config.warehouses + par_id;

        client->InvokeUnLock(++tid,  // txn_nr
                            shards_to_send_bits,
                            server_id,
                            bind(&ShardClient::SendToAllStatusCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            BASIC_TIMEOUT);
        return is_all_response_ok();
    }

    int ShardClient::remoteGetTimestamp(uint32_t &timestamp) {
        int shards_to_send_bits = TThread::writeset_shard_bits;
        if (!shards_to_send_bits) return MakoErrorCode::OK;
        calculate_num_response_waiting(shards_to_send_bits);
        uint16_t server_id = shardIndex * config.warehouses + par_id;

        for (int i=0;i<int_received.size();i++) int_received[i]=0;
        client->InvokeGetTimestamp(++tid,  // txn_nr
                            shards_to_send_bits,
                            server_id,
                            bind(&ShardClient::SendToAllIntCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            BASIC_TIMEOUT);
        // Single timestamp system: use maximum timestamp from all shards
        timestamp = 0;
        for (int i=0; i<(int)int_received.size(); i++) {
            if (int_received[i] > timestamp) {
                timestamp = int_received[i];
            }
        }
        return is_all_response_ok();
    }

    int ShardClient::remoteInvokeSerializeUtil(uint32_t timestamp) {
        // Single timestamp encoding - no vector needed
        char *cc = encode_single_timestamp(timestamp);
        int shards_to_send_bits = TThread::writeset_shard_bits;
        if (!shards_to_send_bits) return MakoErrorCode::OK;
        calculate_num_response_waiting(shards_to_send_bits);
        uint16_t server_id = shardIndex * config.warehouses + par_id;

        client->InvokeSerializeUtil(++tid,  // txn_nr
                            shards_to_send_bits,
                            server_id,
                            cc,
                            bind(&ShardClient::SendToAllStatusCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            BASIC_TIMEOUT);
        free(cc);
        return is_all_response_ok();
    }

    int ShardClient::remoteAbort() {
        int shards_to_send_bits = TThread::writeset_shard_bits | TThread::readset_shard_bits;
        if (TThread::trans_nosend_abort > 0){
            shards_to_send_bits = shards_to_send_bits ^ TThread::trans_nosend_abort;
        }
        if (!shards_to_send_bits) return MakoErrorCode::OK;
        calculate_num_response_waiting(shards_to_send_bits);
        uint16_t server_id = shardIndex * config.warehouses + par_id;

        client->InvokeAbort(++tid,  // txn_nr
                            shards_to_send_bits,
                            server_id,
                            bind(&ShardClient::SendToAllStatusCallBack, this, placeholders::_1),
                            bind(&ShardClient::SendToAllGiveUpTimeout, this),
                            ABORT_TIMEOUT);
        return is_all_response_ok();
    }

    //=========================================================================
    // Luigi: Timestamp-ordered execution dispatch (ASYNC with polling)
    //
    // Flow:
    //   1. Send dispatch to all shards
    //   2. Get QUEUED ACKs immediately (servers don't block)
    //   3. Poll for completion (status check RPCs)
    //   4. Return when all shards report COMPLETE
    //=========================================================================
    
    void ShardClient::LuigiDispatchCallback(char *respBuf) {
        auto *resp = reinterpret_cast<luigi_dispatch_response_t *>(respBuf);
        status_received.push_back(resp->status);
        
        // For QUEUED responses, we don't have results yet
        // For sync responses (if any shard returns COMPLETE directly), parse results
        if (resp->status != LUIGI_STATUS_QUEUED) {
            std::vector<std::string> read_values;
            char* data_ptr = resp->results_data;
            for (uint16_t i = 0; i < resp->num_results; i++) {
                uint16_t vlen = *reinterpret_cast<uint16_t*>(data_ptr);
                data_ptr += sizeof(uint16_t);
                read_values.emplace_back(data_ptr, vlen);
                data_ptr += vlen;
            }
            
            int response_idx = status_received.size() - 1;
            luigi_execute_timestamps_[response_idx] = resp->commit_timestamp;
            luigi_read_results_[response_idx] = std::move(read_values);
        }
    }
    
    void ShardClient::LuigiStatusResponseHandler(char *respBuf) {
        auto *resp = reinterpret_cast<luigi_status_response_t *>(respBuf);
        status_received.push_back(resp->status);
        
        // If complete, parse results
        if (resp->status == LUIGI_STATUS_COMPLETE) {
            std::vector<std::string> read_values;
            char* data_ptr = resp->results_data;
            for (uint16_t i = 0; i < resp->num_results; i++) {
                uint16_t vlen = *reinterpret_cast<uint16_t*>(data_ptr);
                data_ptr += sizeof(uint16_t);
                read_values.emplace_back(data_ptr, vlen);
                data_ptr += vlen;
            }
            
            int response_idx = status_received.size() - 1;
            luigi_execute_timestamps_[response_idx] = resp->commit_timestamp;
            luigi_read_results_[response_idx] = std::move(read_values);
        }
    }

    int ShardClient::remoteLuigiDispatch(
        uint64_t txn_id,
        uint64_t expected_time,
        std::vector<int>& table_ids,
        std::vector<uint8_t>& op_types,
        std::vector<std::string>& keys,
        std::vector<std::string>& values,
        std::map<int, uint64_t>& out_execute_timestamps,
        std::map<int, std::vector<std::string>>& out_read_results)
    {
        if (table_ids.empty()) {
            return MakoErrorCode::OK;
        }

        // Clear previous luigi response storage
        luigi_execute_timestamps_.clear();
        luigi_read_results_.clear();
        status_received.clear();

        // Group operations by destination shard
        std::map<int, LuigiDispatchRequestBuilder*> requests_per_shard;
        std::vector<int> shard_order;  // Track order for response mapping
        
        uint16_t server_id = shardIndex * config.warehouses + par_id;
        int shards_to_send_bits = 0;

        for (size_t i = 0; i < table_ids.size(); i++) {
            int remote_table_id = table_ids[i];
            int dst_shard_idx = (remote_table_id - 1) / mako::NUM_TABLES_PER_SHARD;
            
            shards_to_send_bits |= (1 << dst_shard_idx);

            // Create builder for this shard if not exists
            if (requests_per_shard.find(dst_shard_idx) == requests_per_shard.end()) {
                auto* builder = new LuigiDispatchRequestBuilder();
                builder->set_header(server_id, txn_id, expected_time);
                requests_per_shard[dst_shard_idx] = builder;
                shard_order.push_back(dst_shard_idx);
            }

            // Add operation to the appropriate shard's request
            requests_per_shard[dst_shard_idx]->add_op(
                remote_table_id,
                op_types[i],
                keys[i],
                values[i]
            );
        }

        // IMPORTANT: Set involved_shards on ALL builders so each shard knows
        // which other shards are participating in this multi-shard transaction.
        // This is critical for leader agreement protocol.
        for (auto& kv : requests_per_shard) {
            kv.second->set_involved_shards(shard_order);
        }

        //=====================================================================
        // PHASE 1: Send dispatch to all shards, get QUEUED ACKs
        //=====================================================================
        {
            Promise promise(BASIC_TIMEOUT);
            waiting = &promise;
            
            calculate_num_response_waiting_no_skip(shards_to_send_bits);

            client->InvokeLuigiDispatch(
                ++tid,
                requests_per_shard,
                bind(&ShardClient::LuigiDispatchCallback, this, placeholders::_1),
                bind(&ShardClient::SendToAllGiveUpTimeout, this),
                BASIC_TIMEOUT
            );

            // Clean up builders
            for (auto& kv : requests_per_shard) {
                delete kv.second;
            }

            // Wait for QUEUED ACKs
            promise.GetReply();
            waiting = nullptr;
        }
        
        // Check if all responses received and all are QUEUED (or already COMPLETE)
        if (status_received.size() != shard_order.size()) {
            Warning("Luigi dispatch: only got %zu/%zu responses", 
                    status_received.size(), shard_order.size());
            return MakoErrorCode::ABORT;
        }
        
        // Check if any shard reported error
        for (int status : status_received) {
            if (status != LUIGI_STATUS_QUEUED && 
                status != LUIGI_STATUS_COMPLETE && 
                status != MakoErrorCode::OK) {
                Warning("Luigi dispatch: shard returned error %d", status);
                return MakoErrorCode::ABORT;
            }
        }
        
        //=====================================================================
        // PHASE 2: Poll for completion
        // Wait until expected_time passes, then poll for results
        //=====================================================================
        
        // Calculate how long to wait before first poll
        // expected_time is in microseconds from epoch
        auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        if ((int64_t)expected_time > now_us) {
            // Wait until expected_time + small buffer
            int64_t wait_us = (int64_t)expected_time - now_us + 1000; // +1ms buffer
            if (wait_us > 0 && wait_us < 10000000) { // Max 10 seconds
                std::this_thread::sleep_for(std::chrono::microseconds(wait_us));
            }
        }
        
        // Poll for completion with retries
        const int MAX_POLL_RETRIES = 100;  // Max poll attempts
        const int POLL_INTERVAL_US = 500;   // 500us between polls
        
        for (int poll_attempt = 0; poll_attempt < MAX_POLL_RETRIES; poll_attempt++) {
            // Clear previous status for this poll round
            status_received.clear();
            luigi_execute_timestamps_.clear();
            luigi_read_results_.clear();
            
            // Send status check to all shards
            {
                Promise promise(BASIC_TIMEOUT);
                waiting = &promise;
                
                calculate_num_response_waiting_no_skip(shards_to_send_bits);

                client->InvokeLuigiStatusCheck(
                    ++tid,
                    txn_id,
                    shard_order,
                    bind(&ShardClient::LuigiStatusResponseHandler, this, placeholders::_1),
                    bind(&ShardClient::SendToAllGiveUpTimeout, this),
                    BASIC_TIMEOUT
                );

                promise.GetReply();
                waiting = nullptr;
            }
            
            // Check responses
            bool all_complete = true;
            bool any_abort = false;
            bool any_queued = false;
            
            for (size_t i = 0; i < status_received.size(); i++) {
                int status = status_received[i];
                if (status == LUIGI_STATUS_QUEUED) {
                    all_complete = false;
                    any_queued = true;
                } else if (status == LUIGI_STATUS_ABORTED || 
                           status == LUIGI_STATUS_NOT_FOUND ||
                           status == MakoErrorCode::ABORT) {
                    any_abort = true;
                } else if (status != LUIGI_STATUS_COMPLETE && 
                           status != MakoErrorCode::OK) {
                    // Unknown status
                    Warning("Luigi status check: unknown status %d", status);
                    any_abort = true;
                }
            }
            
            if (any_abort) {
                Warning("Luigi status check: txn %lu aborted", txn_id);
                return MakoErrorCode::ABORT;
            }
            
            if (all_complete && status_received.size() == shard_order.size()) {
                // All shards complete! Map results and return
                for (size_t i = 0; i < shard_order.size(); i++) {
                    int shard_idx = shard_order[i];
                    out_execute_timestamps[shard_idx] = luigi_execute_timestamps_[i];
                    out_read_results[shard_idx] = luigi_read_results_[i];
                }
                return MakoErrorCode::OK;
            }
            
            // Not all complete yet, wait and retry
            if (any_queued) {
                std::this_thread::sleep_for(std::chrono::microseconds(POLL_INTERVAL_US));
            }
        }
        
        // Exceeded max poll attempts
        Warning("Luigi poll timeout for txn %lu after %d attempts", txn_id, MAX_POLL_RETRIES);
        return MakoErrorCode::TIMEOUT;
    }

    //=========================================================================
    // OWD: Ping a single shard for latency measurement
    //=========================================================================
    int ShardClient::pingOneShard(int shard_idx) {
        // Send ping to exactly one shard
        uint64_t shard_bit = (1ULL << shard_idx);
        calculate_num_response_waiting_no_skip(shard_bit);
        
        status_received.clear();
        
        Promise promise(BASIC_TIMEOUT);
        waiting = &promise;
        
        client->InvokeOwdPing(
            ++tid,
            shard_idx,
            bind(&ShardClient::SendToAllStatusCallBack, this, placeholders::_1),
            bind(&ShardClient::SendToAllGiveUpTimeout, this),
            BASIC_TIMEOUT
        );
        
        promise.GetReply();
        waiting = nullptr;
        
        return is_all_response_ok();
    }
}
