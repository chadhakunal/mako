#include <iostream>
#include <random>
#include <chrono>
#include <thread>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include "lib/fasttransport.h"
#include "lib/timestamp.h"
#include "lib/server.h"
#include "lib/common.h"
#include "lib/transport_request_handle.h"
#include "benchmarks/sto/Interface.hh"
#include "benchmarks/common.h"
#include "benchmarks/bench.h"
#include "benchmarks/tpcc.h"
#include "benchmarks/benchmark_config.h"
#include <x86intrin.h>
#include "deptran/s_main.h"
#include "benchmarks/sto/sync_util.hh"

// Luigi (Tiga-style) scheduler
#include "deptran/luigi/luigi_entry.h"
#include "deptran/luigi/luigi_scheduler.h"
#include "deptran/luigi/luigi_rpc_setup.h"
#include "deptran/config.h"  // For Config::CreateMinimalConfig

// rrr RPC framework (for Luigi leader agreement)
#include "rrr/rrr.hpp"

std::function<int()> ss_callback_ = nullptr;
void register_sync_util_ss(std::function<int()> cb) {
    ss_callback_ = cb;
}

namespace mako
{
    using namespace std;

    ShardReceiver::ShardReceiver(std::string file) : config(file)
    {
        current_term = 0;
        luigi_scheduler_ = nullptr;
    }

    void ShardReceiver::Register(abstract_db *dbX,
                                 const map<int, abstract_ordered_index *> &open_tables_table_idX /*,
                                 const map<string, vector<abstract_ordered_index *>> &partitionsX,
                                 const map<string, vector<abstract_ordered_index *>> &remote_partitionsX*/)
    {
        db = dbX;
        open_tables_table_id = open_tables_table_idX;

        txn_obj_buf.reserve(str_arena::MinStrReserveLength);
        txn_obj_buf.resize(db->sizeof_txn_object(0));
        db->shard_reset(); // initialize
        obj_key0.reserve(128);
        obj_key1.reserve(128);
        obj_v.reserve(256);
    }

    //=========================================================================
    // Luigi Scheduler Management
    //=========================================================================
    void ShardReceiver::InitLuigiScheduler(uint32_t partition_id) {
        if (luigi_scheduler_ != nullptr) {
            return;  // Already initialized
        }
        partition_id_ = partition_id;
        
        // Ensure minimal deptran Config exists (needed by SchedulerLuigi's TxLogServer base)
        janus::Config::CreateMinimalConfig();
        
        luigi_scheduler_ = new janus::SchedulerLuigi();
        luigi_scheduler_->SetPartitionId(partition_id);
        
        // Set up callbacks that delegate to Mako's DB operations
        // Capture 'this' to access open_tables_table_id
        luigi_scheduler_->SetReadCallback(
            [this](int table_id, const std::string& key, std::string& value_out) -> bool {
                auto it = open_tables_table_id.find(table_id);
                if (it == open_tables_table_id.end() || it->second == nullptr) {
                    return false;
                }
                return it->second->shard_get(lcdf::Str(key), value_out);
            }
        );
        
        luigi_scheduler_->SetWriteCallback(
            [this](int table_id, const std::string& key, const std::string& value) -> bool {
                auto it = open_tables_table_id.find(table_id);
                if (it == open_tables_table_id.end() || it->second == nullptr) {
                    return false;
                }
                try {
                    it->second->shard_put(lcdf::Str(key), value);
                    return true;
                } catch (...) {
                    return false;
                }
            }
        );
        
        // Replication callback - serialize Luigi entry and submit to Paxos
        luigi_scheduler_->SetReplicationCallback(
            [this](const std::shared_ptr<janus::LuigiLogEntry>& entry) -> bool {
                return ReplicateLuigiEntry(entry);
            }
        );
        
        luigi_scheduler_->Start();
        Log_info("Luigi scheduler initialized for partition %d", partition_id);
    }

    void ShardReceiver::SetupLuigiRpc(
        rrr::Server* rpc_server,
        rusty::Arc<rrr::PollThread> poll_thread,
        const std::map<uint32_t, std::string>& shard_addresses) {
        
        if (luigi_scheduler_ == nullptr) {
            Log_error("SetupLuigiRpc: Luigi scheduler not initialized!");
            return;
        }
        
        if (luigi_rpc_setup_ != nullptr) {
            Log_warn("SetupLuigiRpc: Already set up");
            return;
        }
        
        luigi_rpc_setup_ = new janus::LuigiRpcSetup();
        
        // Register the RPC service so we can receive proposals from other leaders
        if (rpc_server != nullptr) {
            bool ok = luigi_rpc_setup_->SetupService(rpc_server, luigi_scheduler_);
            if (!ok) {
                Log_error("SetupLuigiRpc: Failed to register service");
            }
        } else {
            Log_warn("SetupLuigiRpc: No RPC server provided, skipping service registration");
        }
        
        // Connect to other shard leaders
        if (!shard_addresses.empty() && poll_thread) {
            int connected = luigi_rpc_setup_->ConnectToLeaders(
                shard_addresses, poll_thread, luigi_scheduler_);
            Log_info("Luigi RPC: connected to %d remote leaders", connected);
        } else {
            Log_info("Luigi RPC: No remote shard addresses provided (single-shard mode)");
        }
    }

    void ShardReceiver::StopLuigiScheduler() {
        // Clean up RPC first
        if (luigi_rpc_setup_ != nullptr) {
            luigi_rpc_setup_->Shutdown();
            delete luigi_rpc_setup_;
            luigi_rpc_setup_ = nullptr;
        }
        
        if (luigi_scheduler_ != nullptr) {
            luigi_scheduler_->Stop();
            delete luigi_scheduler_;
            luigi_scheduler_ = nullptr;
            Log_info("Luigi scheduler stopped for partition %d", partition_id_);
        }
    }

    void ShardReceiver::UpdateTableEntry(int table_id, abstract_ordered_index *table)
    {
        if (table_id <= 0 || !table)
            return;
        open_tables_table_id[table_id] = table;
    }

    // Message handlers.
    size_t ShardReceiver::ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf)
    {
        Debug("server deal with reqType: %d", reqType);
        size_t respLen;
        switch (reqType)
        {
        case getReqType:
            HandleGetRequest(reqBuf, respBuf, respLen);
            break;
        case scanReqType:
            HandleScanRequest(reqBuf, respBuf, respLen);
            break;
        case lockReqType:
            HandleLockRequest(reqBuf, respBuf, respLen);
            break;
        case validateReqType:
            HandleValidateRequest(reqBuf, respBuf, respLen);
            break;
        case getTimestampReqType:
            HandleGetTimestampRequest(reqBuf, respBuf, respLen);
            break;
        case serializeUtilReqType:
            HandleSerializeUtilRequest(reqBuf, respBuf, respLen);
            break;
        case installReqType:
            HandleInstallRequest(reqBuf, respBuf, respLen);
            break;
        case unLockReqType:
            HandleUnLockRequest(reqBuf, respBuf, respLen);
            break;
        case abortReqType:
            HandleAbortRequest(reqBuf, respBuf, respLen);
            break;
        case batchLockReqType:
            HandleBatchLockRequest(reqBuf, respBuf, respLen);
            break;
        case luigiDispatchReqType:
            HandleLuigiDispatch(reqBuf, respBuf, respLen);
            break;
        case luigiStatusReqType:
            HandleLuigiStatusCheck(reqBuf, respBuf, respLen);
            break;
        case owdPingReqType:
            HandleOwdPing(reqBuf, respBuf, respLen);
            break;
        default:
            Warning("Unrecognized rquest type: %d", reqType);
        }

        return respLen;
    }

    void ShardReceiver::HandleAbortRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        int status = MakoErrorCode::OK;
        auto *req = reinterpret_cast<basic_request_t *>(reqBuf);
        db->shard_abort_txn(nullptr);

        auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        respLen = sizeof(basic_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
        db->shard_reset();

    }

    void ShardReceiver::HandleUnLockRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        Panic("Deprecated!");
        int status = MakoErrorCode::OK;
        auto *req = reinterpret_cast<basic_request_t *>(reqBuf);
        try {
            db->shard_unlock(true);
        } catch (abstract_db::abstract_abort_exception &ex) {
            //db->shard_abort_txn(nullptr);
            status = MakoErrorCode::ABORT;
            Warning("HandleUnLockRequest error");
        }

        auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        respLen = sizeof(basic_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
        db->shard_reset();
    }

    void ShardReceiver::HandleInstallRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        int status = MakoErrorCode::OK;
        auto *req = reinterpret_cast<vector_int_request_t *>(reqBuf);
        try {
            // Single timestamp system: decode single timestamp directly
            uint32_t timestamp = decode_single_timestamp(req->value);
            db->shard_install(timestamp);
            db->shard_serialize_util(timestamp);
            db->shard_unlock(true);
        } catch (abstract_db::abstract_abort_exception &ex) {
            //db->shard_abort_txn(nullptr);
            status = MakoErrorCode::ABORT;
            Warning("HandleInstallRequest error");
        }

        auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        respLen = sizeof(basic_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
        db->shard_reset();
    }

    void ShardReceiver::HandleValidateRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        int status = MakoErrorCode::OK;
        auto *req = reinterpret_cast<basic_request_t *>(reqBuf);
        try {
            status = db->shard_validate();
            if (status>0){
                //db->shard_abort_txn(nullptr); // early reject, unlock the key earlier
            }
        } catch (abstract_db::abstract_abort_exception &ex) {
            //db->shard_abort_txn(nullptr);
            status = MakoErrorCode::ABORT;
            Warning("HandleValidateRequest error");
        }

        auto *resp = reinterpret_cast<get_int_response_t *>(respBuf);
        respLen = sizeof(get_int_response_t);
        resp->result = sync_util::sync_logger::retrieveShardW();
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->shard_index = TThread::get_shard_index();
        resp->req_nr = req->req_nr;
    }

    void ShardReceiver::HandleGetTimestampRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        int status = MakoErrorCode::OK;
        uint32_t result = 0;
        auto *req = reinterpret_cast<basic_request_t*>(reqBuf);
        auto *resp = reinterpret_cast<get_int_response_t *>(respBuf);
        resp->shard_index = TThread::get_shard_index();
        resp->req_nr = req->req_nr;
        respLen = sizeof(get_int_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->result = __sync_fetch_and_add(&sync_util::sync_logger::local_replica_id, 1);;
    }

    void ShardReceiver::HandleSerializeUtilRequest(char *reqBuf, char *respBuf, size_t &respLen) {
        Panic("Deprecated");
        // int status = MakoErrorCode::OK;
        // auto *req = reinterpret_cast<vector_int_request_t *>(reqBuf);
        // std::vector<uint32_t> ret;
        // decode_vec_uint32(req->value, TThread::get_nshards()).swap(ret);
        // db->shard_serialize_util(ret);

        // auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        // respLen = sizeof(basic_response_t);
        // resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        // resp->req_nr = req->req_nr;
    }

    void ShardReceiver::HandleBatchLockMicroMegaRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        auto *req = reinterpret_cast<batch_lock_request_t *>(reqBuf);
        int status = MakoErrorCode::OK;

        uint16_t table_id, klen, vlen;
        char *k_ptr, *v_ptr;
        auto wrapper = BatchLockRequestWrapper(reqBuf);
        
        while (!wrapper.all_request_handled()) {
            wrapper.read_one_request(&k_ptr, &klen, &v_ptr, &vlen, &table_id);
            //string key(k_ptr, klen);
            obj_key0.assign(k_ptr, klen);
            //string value(v_ptr, vlen);
            obj_v.assign(v_ptr, vlen);
            item_micro::key v_s_temp;
            const item_micro::key *k_s = Decode(obj_key0, v_s_temp);
            if (table_id > 0) {
                try {
                    int base_ol_i_id = k_s->i_id;
                    item_micro::key k_s_new(*k_s);
                    for (int i=0; i<mako::mega_batch_size; i++) {
                        k_s_new.i_id = base_ol_i_id + i;
                        open_tables_table_id[table_id]->shard_put(EncodeK(obj_key0, k_s_new), obj_v);
                    }
                } catch (abstract_db::abstract_abort_exception &ex) {
                   status = MakoErrorCode::ABORT;
                   Debug("HandleBatchLockMicroMegaRequest: fail to lock a key");
                }
            }
        }

        auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        respLen = sizeof(basic_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
    }

    void ShardReceiver::HandleBatchLockMegaRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        auto *req = reinterpret_cast<batch_lock_request_t *>(reqBuf);
        int status = MakoErrorCode::OK;

        uint16_t table_id, klen, vlen;
        char *k_ptr, *v_ptr;
        auto wrapper = BatchLockRequestWrapper(reqBuf);
        
        while (!wrapper.all_request_handled()) {
            wrapper.read_one_request(&k_ptr, &klen, &v_ptr, &vlen, &table_id);
            //string key(k_ptr, klen);
            obj_key0.assign(k_ptr, klen);
            //string value(v_ptr, vlen);
            obj_v.assign(v_ptr, vlen);
            stock::key v_s_temp;
            const stock::key *k_s = Decode(obj_key0, v_s_temp);
            if (table_id > 0) {
                try {
                    int base_ol_i_id = k_s->s_i_id;
                    stock::key k_s_new(*k_s);
                    for (int i=0; i<mako::mega_batch_size; i++) {
                        k_s_new.s_i_id = base_ol_i_id + i;
                        open_tables_table_id[table_id]->shard_put(EncodeK(obj_key0, k_s_new), obj_v);
                    }
                } catch (abstract_db::abstract_abort_exception &ex) {
                   //db->shard_abort_txn(nullptr);
                   status = MakoErrorCode::ABORT;
                   Debug("HandleLockRequest: fail to lock a key");
                }
            }
        }

        auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        respLen = sizeof(basic_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
    }

    void ShardReceiver::HandleBatchLockRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
#if defined(MEGA_BENCHMARK)
        HandleBatchLockMegaRequest(reqBuf, respBuf, respLen);
#elif defined(MEGA_BENCHMARK_MICRO)
        HandleBatchLockMicroMegaRequest(reqBuf, respBuf, respLen);
#else
        auto *req = reinterpret_cast<batch_lock_request_t *>(reqBuf);
        int status = MakoErrorCode::OK;

        uint16_t table_id, klen, vlen;
        char *k_ptr, *v_ptr;
        auto wrapper = BatchLockRequestWrapper(reqBuf);
        
        while (!wrapper.all_request_handled()) {
            wrapper.read_one_request(&k_ptr, &klen, &v_ptr, &vlen, &table_id);
            //string key(k_ptr, klen);
            obj_key0.assign(k_ptr, klen);
            //string value(v_ptr, vlen);
            obj_v.assign(v_ptr, vlen);

            if (table_id > 0) {
                try {
                    open_tables_table_id[table_id]->shard_put(obj_key0, obj_v);
                } catch (abstract_db::abstract_abort_exception &ex) {
                   //db->shard_abort_txn(nullptr);
                   status = MakoErrorCode::ABORT;
                   Debug("HandleLockRequest: fail to lock a key");
                }
            }
        }

        auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        respLen = sizeof(basic_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
#endif
    }

    void ShardReceiver::HandleLockRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        Panic("Deprecated");
        string val;
        auto *req = reinterpret_cast<lock_request_t *>(reqBuf);
        //std::string key = string(req->key_and_value, req->klen);
        obj_key0.assign(req->key_and_value, req->klen);
        //std::string value = string(req->key_and_value + req->klen, req->vlen);
        obj_v.assign(req->key_and_value + req->klen, req->vlen);

        int table_id = req->table_id;
        int status = MakoErrorCode::OK;

        if (table_id > 0) {
            try {
                open_tables_table_id[table_id]->shard_put(obj_key0, obj_v);
            } catch (abstract_db::abstract_abort_exception &ex) {
                //db->shard_abort_txn(nullptr);
                status = MakoErrorCode::ABORT;
                Debug("HandleLockRequest: fail to lock a key");
            }
        }

        auto *resp = reinterpret_cast<basic_response_t *>(respBuf);
        respLen = sizeof(basic_response_t);
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
    }

    void ShardReceiver::HandleScanRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        string val;
        scoped_str_arena s_arena(arena);

        auto *req = reinterpret_cast<scan_request_t *>(reqBuf);
        obj_key0.assign(req->start_end_key, req->slen);
        obj_key1.assign(req->start_end_key+req->slen, req->elen);
        // const std::string start_key = string(req->start_end_key, req->slen);
        // const std::string end_key = string(req->start_end_key+req->slen, req->elen);

        int status = MakoErrorCode::OK;

        static_limit_callback<512> c(s_arena.get(), true); // probably a safe bet for now, NMaxCustomerIdxScanElems
        if (req->table_id > 0) {
            try {
                open_tables_table_id[req->table_id]->shard_scan(obj_key0, &obj_key1, c, s_arena.get());
                if (c.size() == 0) {
                    //Warning("# of scan is 0, table_id: %d", (int)req->table_id);
                    throw abstract_db::abstract_abort_exception();
                } else {
                    ALWAYS_ASSERT(c.size() > 0);
                    int index = c.size() / 2;
                    if (c.size() % 2 == 0)
                        index--;
                    val = *c.values[index].second;
                }
            } catch (abstract_db::abstract_abort_exception &ex) {
                db->shard_abort_txn(nullptr);
                status = MakoErrorCode::ABORT;
            }
        } else {
            val = "this is a mocked value for erpc_client and erpc_server";
        }
        
        auto *resp = reinterpret_cast<scan_response_t *>(respBuf);
        respLen = sizeof(scan_response_t) - max_value_length + val.length();
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
        resp->len = val.length();
        memcpy(resp->value, val.c_str(), val.length());
    }

    void ShardReceiver::HandleGetMicroMegaRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        auto *req = reinterpret_cast<get_request_t *>(reqBuf);
        obj_key0.assign(req->key, req->len);

        // for MicroMega, all get request is for item table
        item_micro::key v_s_temp;
        const item_micro::key *k_s = Decode(obj_key0, v_s_temp);
        std::string c_v;
        int offset = 0;
        int value_size = 8;
        c_v.resize(value_size);

        int status = MakoErrorCode::OK;
        if (req->table_id > 0) {
            try {
                bool ret = true;
                int base_ol_i_id = k_s->i_id;
                item_micro::key k_s_new(*k_s); 
                for (int i=0; i<mako::mega_batch_size; i++) {
                   k_s_new.i_id = base_ol_i_id + i;
                   ret = open_tables_table_id[req->table_id]->shard_get(EncodeK(obj_key0, k_s_new), obj_v);
                   memcpy((char*)c_v.c_str()+offset,obj_v.c_str(),value_size);
                   offset = 0;
                }
                // abort here,
                if (!ret){ // key not found or found but invalid
                    db->shard_abort_txn(nullptr);
                    status = MakoErrorCode::ABORT;
                }
            } catch (abstract_db::abstract_abort_exception &ex) {
                // No need to abort, the client side will issue an abort
                db->shard_abort_txn(nullptr);
                status = MakoErrorCode::ABORT;
            }
        } else {
            obj_v = "this is a mocked value for erpc_client and erpc_server";
        }
        
        auto *resp = reinterpret_cast<get_response_t *>(respBuf);
        respLen = sizeof(get_response_t) - max_value_length + c_v.length();
        ALWAYS_ASSERT(max_value_length>=obj_v.length());
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
        resp->len = c_v.length();
        //Warning("the remoteGET,len:%d,table_id:%d,keys:%s,key_len:%d,val_len:%d",obj_v.length(),req->table_id,mako::printStringAsBit(obj_key0).c_str(),req->len,obj_v.length());
        memcpy(resp->value, c_v.c_str(), c_v.length());
    }

    void ShardReceiver::HandleGetMegaRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
        auto *req = reinterpret_cast<get_request_t *>(reqBuf);
        obj_key0.assign(req->key, req->len);

        // for NewOrderMega, all get request is for stock table
        stock::key v_s_temp;
        const stock::key *k_s = Decode(obj_key0, v_s_temp);
        //std::cout<<"HandleGetRequest, base:"<<k_s->s_i_id<<", table-id:"<<req->table_id<<std::endl;
        //int tol_len = mako::mega_batch_size* mako::size_per_stock_value;  
        int tol_len = 1* mako::size_per_stock_value;  
        std::string c_v;
        int offset = 0;
        c_v.resize(tol_len);

        int status = MakoErrorCode::OK;
        if (req->table_id > 0) {
            try {
                bool ret = true;
                int base_ol_i_id = k_s->s_i_id;
                stock::key k_s_new(*k_s); 
                for (int i=0; i<mako::mega_batch_size; i++) {
                   k_s_new.s_i_id = base_ol_i_id + i;
                   ret = open_tables_table_id[req->table_id]->shard_get(EncodeK(obj_key0, k_s_new), obj_v);
                   memcpy((char*)c_v.c_str()+offset,obj_v.c_str(),mako::size_per_stock_value);
                   //offset += mako::size_per_stock_value;
                   offset = 0;
                }
                // abort here,
                //  "not found a key" maybe a expected behavior
                if (!ret){ // key not found or found but invalid
                    db->shard_abort_txn(nullptr);
                    status = MakoErrorCode::ABORT;
                }
            } catch (abstract_db::abstract_abort_exception &ex) {
                // No need to abort, the client side will issue an abort
                db->shard_abort_txn(nullptr);
                status = MakoErrorCode::ABORT;
            }
        } else {
            obj_v = "this is a mocked value for erpc_client and erpc_server";
        }
        
        auto *resp = reinterpret_cast<get_response_t *>(respBuf);
        respLen = sizeof(get_response_t) - max_value_length + c_v.length();
        ALWAYS_ASSERT(max_value_length>=obj_v.length());
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
        resp->len = c_v.length();
        //Warning("the remoteGET,len:%d,table_id:%d,keys:%s,key_len:%d,val_len:%d",obj_v.length(),req->table_id,mako::printStringAsBit(obj_key0).c_str(),req->len,obj_v.length());
        memcpy(resp->value, c_v.c_str(), c_v.length());
    }

    void ShardReceiver::HandleGetRequest(char *reqBuf, char *respBuf, size_t &respLen)
    {
#if defined(MEGA_BENCHMARK)
        HandleGetMegaRequest(reqBuf, respBuf, respLen);
#elif defined(MEGA_BENCHMARK_MICRO)
        HandleGetMicroMegaRequest(reqBuf, respBuf, respLen);
#else
        auto *req = reinterpret_cast<get_request_t *>(reqBuf);
#if defined(FAIL_NEW_VERSION)
        current_term = ss_callback_();
#endif
        obj_key0.assign(req->key, req->len);

        int status = MakoErrorCode::OK;
        if (req->table_id > 0) {
            // Check if table exists (may not exist in micro benchmark mode)
            auto it = open_tables_table_id.find(req->table_id);
            if (it == open_tables_table_id.end() || it->second == nullptr) {
                db->shard_abort_txn(nullptr);
                status = MakoErrorCode::ABORT;
            } else {
                try {
                    bool ret = it->second->shard_get(obj_key0, obj_v);
                    // abort here,
                    //  "not found a key" maybe a expected behavior
                    if (!ret){ // key not found or found but invalid
                        db->shard_abort_txn(nullptr);
                        status = MakoErrorCode::ABORT;
                    }
                } catch (abstract_db::abstract_abort_exception &ex) {
                    // No need to abort, the client side will issue an abort
                    db->shard_abort_txn(nullptr);
                    status = MakoErrorCode::ABORT;
                }
            }
        } else {
            obj_v = "this is a mocked value for erpc_client and erpc_server";
        }
        
        auto *resp = reinterpret_cast<get_response_t *>(respBuf);
        respLen = sizeof(get_response_t) - max_value_length + obj_v.length();
        ALWAYS_ASSERT(max_value_length>=obj_v.length());
        resp->status = (current_term > req->req_nr % 10)? MakoErrorCode::ABORT: status; // If a reqest comes from old epoch, reject it.;
        resp->req_nr = req->req_nr;
        resp->len = obj_v.length();
        //Warning("the remoteGET,len:%d,table_id:%d,keys:%s,key_len:%d,val_len:%d",obj_v.length(),req->table_id,mako::printStringAsBit(obj_key0).c_str(),req->len,obj_v.length());
        memcpy(resp->value, obj_v.c_str(), obj_v.length());
#endif
    }

    /**
     * file: configuration fileName
     * par_id: to distinguish the running thread
     */
    ShardServer::ShardServer(std::string file, int clientShardIndex, int serverShardIndex, int par_id) : config(file),
                                                                                                         serverShardIndex(serverShardIndex),
                                                                                                         clientShardIndex(clientShardIndex),
                                                                                                         par_id(par_id)
    {
        shardReceiver = new mako::ShardReceiver(file);
    }

    void ShardServer::Register(abstract_db *dbX,
                               mako::HelperQueue *queueX,
                               mako::HelperQueue *queueY,
                               const map<int, abstract_ordered_index *> &open_tablesX)
    {
        db = dbX;
        queue = queueX;
        queue_response = queueY;
        open_tables_table_id = open_tablesX;
        shardReceiver->Register(db, open_tables_table_id);
        
        // Initialize Luigi scheduler only when --use-luigi flag is enabled
        if (BenchmarkConfig::getInstance().getUseLuigi()) {
            // par_id serves as the partition_id for this server
            shardReceiver->InitLuigiScheduler(par_id);
        }
    }

    void ShardServer::UpdateTable(int table_id, abstract_ordered_index *table)
    {
        if (table_id > 0 && table) {
            open_tables_table_id[table_id] = table;
        }
        shardReceiver->UpdateTableEntry(table_id, table);
    }

    void ShardServer::SetupLuigiRpc(rrr::Server* rpc_server,
                                    rusty::Arc<rrr::PollThread> poll_thread,
                                    const std::map<uint32_t, std::string>& shard_addresses)
    {
        shardReceiver->SetupLuigiRpc(rpc_server, poll_thread, shard_addresses);
    }

    //=========================================================================
    // HandleLuigiDispatch: Luigi (Tiga-style) ASYNC timestamp-ordered execution
    //
    // This handler receives a transaction with a future timestamp deadline,
    // parses the operations, dispatches to the Luigi scheduler, and returns
    // IMMEDIATELY with QUEUED status. The caller must poll for completion.
    //
    // Flow:
    //   1. Parse request, extract ops and involved shards
    //   2. Queue in scheduler with async callback
    //   3. Return QUEUED immediately (thread freed!)
    //   4. When scheduler completes, callback stores result for polling
    //=========================================================================
    void ShardReceiver::HandleLuigiDispatch(char *reqBuf, char *respBuf, size_t &respLen)
    {
        auto *req = reinterpret_cast<luigi_dispatch_request_t *>(reqBuf);
        
        // Parse operations from request
        // Format: [table_id(2) | op_type(1) | klen(2) | vlen(2) | key | value]
        std::vector<janus::LuigiOp> ops;
        char *data_ptr = req->ops_data;
        
        for (uint16_t i = 0; i < req->num_ops; i++) {
            janus::LuigiOp op;
            
            // Read table_id (2 bytes)
            op.table_id = *reinterpret_cast<uint16_t*>(data_ptr);
            data_ptr += sizeof(uint16_t);
            
            // Read op_type (1 byte): 0=read, 1=write
            op.op_type = *reinterpret_cast<uint8_t*>(data_ptr);
            data_ptr += sizeof(uint8_t);
            
            // Read key length (2 bytes)
            uint16_t klen = *reinterpret_cast<uint16_t*>(data_ptr);
            data_ptr += sizeof(uint16_t);
            
            // Read value length (2 bytes)
            uint16_t vlen = *reinterpret_cast<uint16_t*>(data_ptr);
            data_ptr += sizeof(uint16_t);
            
            // Read key
            op.key.assign(data_ptr, klen);
            data_ptr += klen;
            
            // Read value (for writes)
            if (vlen > 0) {
                op.value.assign(data_ptr, vlen);
                data_ptr += vlen;
            }
            
            ops.push_back(op);
        }
        
        // Extract involved shards for multi-shard agreement
        std::vector<uint32_t> involved_shards;
        for (uint16_t i = 0; i < req->num_involved_shards && i < luigi_max_shards; i++) {
            involved_shards.push_back(req->involved_shards[i]);
        }
        
        // Prepare response buffer
        auto *resp = reinterpret_cast<luigi_dispatch_response_t *>(respBuf);
        resp->req_nr = req->req_nr;
        resp->txn_id = req->txn_id;
        respLen = sizeof(luigi_dispatch_response_t);
        
        // Check if Luigi scheduler is initialized
        if (luigi_scheduler_ == nullptr) {
            Warning("Luigi scheduler not initialized, rejecting request");
            resp->status = MakoErrorCode::ABORT;
            resp->commit_timestamp = 0;
            resp->num_results = 0;
            return;
        }
        
        // Capture txn_id for the async callback
        uint64_t txn_id = req->txn_id;
        
        // Dispatch to Luigi scheduler with async completion callback
        // The callback will store the result for later polling
        luigi_scheduler_->LuigiDispatchFromRequest(
            txn_id,
            req->expected_time,
            ops,
            involved_shards,
            [this, txn_id](int status, uint64_t commit_ts, const std::vector<std::string>& read_results) {
                // Store result for polling - this runs async when txn completes
                StoreLuigiResult(txn_id, status, commit_ts, read_results);
            }
        );
        
        // Return QUEUED immediately - don't wait for execution!
        resp->status = LUIGI_STATUS_QUEUED;
        resp->commit_timestamp = 0;
        resp->num_results = 0;
        
        Debug("Luigi dispatch queued txn %lu, expected_time %lu", txn_id, req->expected_time);
    }

    //=========================================================================
    // HandleLuigiStatusCheck: Poll for completion of async Luigi dispatch
    //
    // Called by coordinator to check if a transaction has completed.
    // Returns QUEUED if still pending, COMPLETE/ABORTED with results if done.
    //=========================================================================
    void ShardReceiver::HandleLuigiStatusCheck(char *reqBuf, char *respBuf, size_t &respLen)
    {
        auto *req = reinterpret_cast<luigi_status_request_t *>(reqBuf);
        auto *resp = reinterpret_cast<luigi_status_response_t *>(respBuf);
        
        resp->req_nr = req->req_nr;
        resp->txn_id = req->txn_id;
        respLen = sizeof(luigi_status_response_t);
        
        // Look up result in completed txns map
        {
            std::shared_lock<std::shared_mutex> lock(luigi_results_mutex_);
            auto it = luigi_completed_txns_.find(req->txn_id);
            
            if (it == luigi_completed_txns_.end()) {
                // Not found - either still queued or expired
                // Check if scheduler knows about this txn
                if (luigi_scheduler_ != nullptr && 
                    luigi_scheduler_->HasPendingTxn(req->txn_id)) {
                    resp->status = LUIGI_STATUS_QUEUED;
                } else {
                    resp->status = LUIGI_STATUS_NOT_FOUND;
                }
                resp->commit_timestamp = 0;
                resp->num_results = 0;
                return;
            }
            
            // Found completed result
            const auto& result = it->second;
            resp->status = result.status;
            resp->commit_timestamp = result.commit_timestamp;
            resp->num_results = result.read_results.size();
            
            // Copy read results to response buffer
            char* results_ptr = resp->results_data;
            for (const auto& val : result.read_results) {
                uint16_t vlen = val.size();
                memcpy(results_ptr, &vlen, sizeof(uint16_t));
                results_ptr += sizeof(uint16_t);
                memcpy(results_ptr, val.data(), vlen);
                results_ptr += vlen;
            }
        }
        
        // Optionally remove the result after successful retrieval
        // (coordinator got it, no need to keep it)
        if (resp->status == LUIGI_STATUS_COMPLETE || resp->status == LUIGI_STATUS_ABORTED) {
            std::unique_lock<std::shared_mutex> lock(luigi_results_mutex_);
            luigi_completed_txns_.erase(req->txn_id);
        }
        
        Debug("Luigi status check txn %lu: status=%d", req->txn_id, resp->status);
    }

    //=========================================================================
    // StoreLuigiResult: Store completed txn result for polling
    //
    // Called from scheduler's async callback when a txn completes.
    //=========================================================================
    void ShardReceiver::StoreLuigiResult(uint64_t txn_id, int status, uint64_t commit_ts,
                                          const std::vector<std::string>& read_results)
    {
        std::unique_lock<std::shared_mutex> lock(luigi_results_mutex_);
        
        LuigiTxnResult result;
        result.status = (status == MakoErrorCode::OK) ? LUIGI_STATUS_COMPLETE : LUIGI_STATUS_ABORTED;
        result.commit_timestamp = commit_ts;
        result.read_results = read_results;
        result.completion_time = std::chrono::steady_clock::now();
        
        luigi_completed_txns_[txn_id] = std::move(result);
        
        Debug("Luigi result stored for txn %lu: status=%d, commit_ts=%lu",
              txn_id, result.status, commit_ts);
        
        // Periodic cleanup of old results
        static int cleanup_counter = 0;
        if (++cleanup_counter >= 100) {  // Every 100 completions
            cleanup_counter = 0;
            // Don't hold lock during cleanup - call outside
            lock.unlock();
            CleanupStaleLuigiResults(60);  // 60 second TTL
        }
    }

    //=========================================================================
    // CleanupStaleLuigiResults: Remove old results to prevent memory leak
    //=========================================================================
    void ShardReceiver::CleanupStaleLuigiResults(int ttl_seconds)
    {
        auto now = std::chrono::steady_clock::now();
        auto ttl = std::chrono::seconds(ttl_seconds);
        
        std::unique_lock<std::shared_mutex> lock(luigi_results_mutex_);
        
        for (auto it = luigi_completed_txns_.begin(); it != luigi_completed_txns_.end(); ) {
            if (now - it->second.completion_time > ttl) {
                Debug("Luigi cleanup: removing stale result for txn %lu", it->first);
                it = luigi_completed_txns_.erase(it);
            } else {
                ++it;
            }
        }
    }

    //=========================================================================
    // ReplicateLuigiEntry: Serialize Luigi entry and submit to Paxos
    //
    // Log format (compatible with Mako's existing format):
    //   [luigi_magic(4)] [txn_id(8)] [commit_ts(8)] [num_writes(2)]
    //   For each write: [key_len(2)] [key] [val_len(2)] [val] [table_id(2)]
    //   [checksum(4)]
    //
    // We only replicate writes (reads don't need replication).
    //=========================================================================
    bool ShardReceiver::ReplicateLuigiEntry(const std::shared_ptr<janus::LuigiLogEntry>& entry)
    {
        // Count writes first
        uint16_t num_writes = 0;
        for (const auto& op : entry->ops_) {
            if (op.op_type == janus::LUIGI_OP_WRITE && op.executed) {
                num_writes++;
            }
        }
        
        if (num_writes == 0) {
            // Read-only transaction, no need to replicate
            Debug("Luigi replication: txn %lu is read-only, skipping", entry->tid_);
            return true;
        }
        
        // Estimate buffer size needed
        // Header: magic(4) + txn_id(8) + commit_ts(8) + num_writes(2) = 22
        // Per write: key_len(2) + key + val_len(2) + val + table_id(2)
        // Footer: checksum(4)
        size_t estimated_size = 22 + 4;  // header + footer
        for (const auto& op : entry->ops_) {
            if (op.op_type == janus::LUIGI_OP_WRITE && op.executed) {
                estimated_size += 2 + op.key.size() + 2 + op.value.size() + 2;
            }
        }
        
        // Allocate buffer
        std::vector<char> buffer(estimated_size);
        char* ptr = buffer.data();
        
        // Magic number to identify Luigi logs (for recovery differentiation)
        constexpr uint32_t LUIGI_LOG_MAGIC = 0x4C554947;  // "LUIG"
        memcpy(ptr, &LUIGI_LOG_MAGIC, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        
        // Transaction ID
        uint64_t txn_id = entry->tid_;
        memcpy(ptr, &txn_id, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        
        // Commit timestamp (agreed_ts for multi-shard, proposed_ts for single-shard)
        uint64_t commit_ts = entry->agreed_ts_ > 0 ? entry->agreed_ts_ : entry->proposed_ts_;
        memcpy(ptr, &commit_ts, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        
        // Number of writes
        memcpy(ptr, &num_writes, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        
        // Serialize each write operation
        for (const auto& op : entry->ops_) {
            if (op.op_type != janus::LUIGI_OP_WRITE || !op.executed) {
                continue;
            }
            
            // Key length + key
            uint16_t key_len = static_cast<uint16_t>(op.key.size());
            memcpy(ptr, &key_len, sizeof(uint16_t));
            ptr += sizeof(uint16_t);
            memcpy(ptr, op.key.data(), key_len);
            ptr += key_len;
            
            // Value length + value
            uint16_t val_len = static_cast<uint16_t>(op.value.size());
            memcpy(ptr, &val_len, sizeof(uint16_t));
            ptr += sizeof(uint16_t);
            memcpy(ptr, op.value.data(), val_len);
            ptr += val_len;
            
            // Table ID
            uint16_t table_id = op.table_id;
            memcpy(ptr, &table_id, sizeof(uint16_t));
            ptr += sizeof(uint16_t);
        }
        
        // Simple checksum (XOR of all bytes)
        uint32_t checksum = 0;
        for (size_t i = 0; i < static_cast<size_t>(ptr - buffer.data()); i++) {
            checksum ^= static_cast<uint32_t>(static_cast<uint8_t>(buffer[i])) << ((i % 4) * 8);
        }
        memcpy(ptr, &checksum, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        
        // Calculate actual size
        size_t actual_size = static_cast<size_t>(ptr - buffer.data());
        
        Debug("Luigi replication: txn %lu, commit_ts %lu, %u writes, %zu bytes",
              txn_id, commit_ts, num_writes, actual_size);
        
        // Submit to Paxos via add_log_to_nc
        // This is fire-and-forget - Paxos handles replication asynchronously
        add_log_to_nc(buffer.data(), static_cast<int>(actual_size), partition_id_);
        
        return true;
    }

    //=========================================================================
    // HandleOwdPing: Simple ping for One-Way Delay measurement
    //
    // This handler immediately responds to an OWD ping request.
    // The client uses the round-trip time to estimate network latency.
    //=========================================================================
    void ShardReceiver::HandleOwdPing(char *reqBuf, char *respBuf, size_t &respLen)
    {
        auto *req = reinterpret_cast<owd_ping_request_t *>(reqBuf);
        auto *resp = reinterpret_cast<owd_ping_response_t *>(respBuf);
        
        resp->req_nr = req->req_nr;
        resp->status = MakoErrorCode::OK;
        respLen = sizeof(owd_ping_response_t);
        
        Debug("OWD ping handled, req_nr: %u", req->req_nr);
    }

    void ShardServer::Run()
    {
        while (true) {
            queue->suspend();

            while (true) {
                erpc::ReqHandle *handle;
                size_t msg_size;
                if (!queue->fetch_one_req(&handle, msg_size)) {
                    break;
                }
                if (!handle) {
                    Panic("the pointer is invalid, p:%s, rIdx:%d, wIdx:%d, count:%d",
                            (void*)handle,
                                queue->req_buffer_reader_idx,queue->req_buffer_writer_idx,
                                queue->req_cnt);

                }

                // Cast to transport-agnostic interface
                // The backend has enqueued a TransportRequestHandle* (cast to erpc::ReqHandle*)
                mako::TransportRequestHandle* req_handle = reinterpret_cast<mako::TransportRequestHandle*>(handle);

                // Use abstract interface methods instead of eRPC-specific API
                size_t msgLen = shardReceiver->ReceiveRequest(
                    req_handle->GetRequestType(),
                    req_handle->GetRequestBuffer(),
                    req_handle->GetResponseBuffer());

                // Enqueue response via transport-agnostic interface
                // This will call ErpcRequestHandle::EnqueueResponse() or RrrRequestHandle::EnqueueResponse()
                req_handle->EnqueueResponse(msgLen);
            }

            if (queue->should_stop()) {
                // Stop Luigi scheduler on shutdown
                shardReceiver->StopLuigiScheduler();
                break;
            }
        }
    }
}
