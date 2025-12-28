#pragma once

#include "rrr.hpp"

#include <errno.h>


namespace janus {

class LuigiService: public rrr::Service {
public:
    enum {
        DISPATCH = 0x15d168a0,
        OWDPING = 0x1afa4db1,
        DEADLINEPROPOSE = 0x141e05c1,
        DEADLINECONFIRM = 0x3e09c3a8,
        DEADLINEBATCHPROPOSE = 0x1e626797,
        DEADLINEBATCHCONFIRM = 0x17771e58,
        WATERMARKEXCHANGE = 0x27b2776d,
        REPLICATE = 0x21aac499,
        BATCHREPLICATE = 0x5a60a59a,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(DISPATCH, this, &LuigiService::__Dispatch__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(OWDPING, this, &LuigiService::__OwdPing__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DEADLINEPROPOSE, this, &LuigiService::__DeadlinePropose__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DEADLINECONFIRM, this, &LuigiService::__DeadlineConfirm__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DEADLINEBATCHPROPOSE, this, &LuigiService::__DeadlineBatchPropose__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DEADLINEBATCHCONFIRM, this, &LuigiService::__DeadlineBatchConfirm__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(WATERMARKEXCHANGE, this, &LuigiService::__WatermarkExchange__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(REPLICATE, this, &LuigiService::__Replicate__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(BATCHREPLICATE, this, &LuigiService::__BatchReplicate__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(DISPATCH);
        svr->unreg(OWDPING);
        svr->unreg(DEADLINEPROPOSE);
        svr->unreg(DEADLINECONFIRM);
        svr->unreg(DEADLINEBATCHPROPOSE);
        svr->unreg(DEADLINEBATCHCONFIRM);
        svr->unreg(WATERMARKEXCHANGE);
        svr->unreg(REPLICATE);
        svr->unreg(BATCHREPLICATE);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, req is rusty::Box (auto-cleaned); weak_sconn requires lock() before use
    virtual void Dispatch(const rrr::i64& txn_id, const rrr::i64& expected_time, const rrr::i32& worker_id, const std::vector<rrr::i32>& involved_shards, const std::string& ops_data, rrr::i32* status, rrr::i64* commit_timestamp, std::string* results_data, rrr::DeferredReply* defer) = 0;
    virtual void OwdPing(const rrr::i64& send_time, rrr::i32* status, rrr::DeferredReply* defer) = 0;
    virtual void DeadlinePropose(const rrr::i64& tid, const rrr::i32& src_shard, const rrr::i64& proposed_ts, rrr::i32* status, rrr::DeferredReply* defer) = 0;
    virtual void DeadlineConfirm(const rrr::i64& tid, const rrr::i32& src_shard, const rrr::i64& agreed_ts, rrr::i32* status, rrr::DeferredReply* defer) = 0;
    virtual void DeadlineBatchPropose(const std::vector<rrr::i64>& tids, const rrr::i32& src_shard, const std::vector<rrr::i64>& proposed_timestamps, const std::vector<rrr::i64>& watermarks, rrr::i32* status, rrr::DeferredReply* defer) = 0;
    virtual void DeadlineBatchConfirm(const std::vector<rrr::i64>& tids, const rrr::i32& src_shard, const std::vector<rrr::i64>& agreed_timestamps, rrr::i32* status, rrr::DeferredReply* defer) = 0;
    virtual void WatermarkExchange(const rrr::i32& src_shard, const std::vector<rrr::i64>& watermarks, rrr::i32* status, rrr::DeferredReply* defer) = 0;
    virtual void Replicate(const rrr::i32& worker_id, const rrr::i64& slot_id, const rrr::i64& txn_id, const rrr::i64& timestamp, const std::string& log_data, rrr::i32* status, rrr::DeferredReply* defer) = 0;
    virtual void BatchReplicate(const rrr::i32& worker_id, const rrr::i64& prev_committed_slot, const std::vector<rrr::i64>& slot_ids, const std::vector<rrr::i64>& txn_ids, const std::vector<rrr::i64>& timestamps, const std::vector<std::string>& log_entries, rrr::i32* status, rrr::i64* last_appended_slot, rrr::DeferredReply* defer) = 0;
private:
    void __Dispatch__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i64* in_0 = new rrr::i64;
        req->m >> *in_0;
        rrr::i64* in_1 = new rrr::i64;
        req->m >> *in_1;
        rrr::i32* in_2 = new rrr::i32;
        req->m >> *in_2;
        std::vector<rrr::i32>* in_3 = new std::vector<rrr::i32>;
        req->m >> *in_3;
        std::string* in_4 = new std::string;
        req->m >> *in_4;
        rrr::i32* out_0 = new rrr::i32;
        rrr::i64* out_1 = new rrr::i64;
        std::string* out_2 = new std::string;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
                const_cast<rrr::ServerConnection&>(*sconn) << *out_1;
                const_cast<rrr::ServerConnection&>(*sconn) << *out_2;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete in_3;
            delete in_4;
            delete out_0;
            delete out_1;
            delete out_2;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->Dispatch(*in_0, *in_1, *in_2, *in_3, *in_4, out_0, out_1, out_2, __defer__);
    }
    void __OwdPing__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i64* in_0 = new rrr::i64;
        req->m >> *in_0;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->OwdPing(*in_0, out_0, __defer__);
    }
    void __DeadlinePropose__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i64* in_0 = new rrr::i64;
        req->m >> *in_0;
        rrr::i32* in_1 = new rrr::i32;
        req->m >> *in_1;
        rrr::i64* in_2 = new rrr::i64;
        req->m >> *in_2;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->DeadlinePropose(*in_0, *in_1, *in_2, out_0, __defer__);
    }
    void __DeadlineConfirm__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i64* in_0 = new rrr::i64;
        req->m >> *in_0;
        rrr::i32* in_1 = new rrr::i32;
        req->m >> *in_1;
        rrr::i64* in_2 = new rrr::i64;
        req->m >> *in_2;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->DeadlineConfirm(*in_0, *in_1, *in_2, out_0, __defer__);
    }
    void __DeadlineBatchPropose__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        std::vector<rrr::i64>* in_0 = new std::vector<rrr::i64>;
        req->m >> *in_0;
        rrr::i32* in_1 = new rrr::i32;
        req->m >> *in_1;
        std::vector<rrr::i64>* in_2 = new std::vector<rrr::i64>;
        req->m >> *in_2;
        std::vector<rrr::i64>* in_3 = new std::vector<rrr::i64>;
        req->m >> *in_3;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete in_3;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->DeadlineBatchPropose(*in_0, *in_1, *in_2, *in_3, out_0, __defer__);
    }
    void __DeadlineBatchConfirm__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        std::vector<rrr::i64>* in_0 = new std::vector<rrr::i64>;
        req->m >> *in_0;
        rrr::i32* in_1 = new rrr::i32;
        req->m >> *in_1;
        std::vector<rrr::i64>* in_2 = new std::vector<rrr::i64>;
        req->m >> *in_2;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->DeadlineBatchConfirm(*in_0, *in_1, *in_2, out_0, __defer__);
    }
    void __WatermarkExchange__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i32* in_0 = new rrr::i32;
        req->m >> *in_0;
        std::vector<rrr::i64>* in_1 = new std::vector<rrr::i64>;
        req->m >> *in_1;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->WatermarkExchange(*in_0, *in_1, out_0, __defer__);
    }
    void __Replicate__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i32* in_0 = new rrr::i32;
        req->m >> *in_0;
        rrr::i64* in_1 = new rrr::i64;
        req->m >> *in_1;
        rrr::i64* in_2 = new rrr::i64;
        req->m >> *in_2;
        rrr::i64* in_3 = new rrr::i64;
        req->m >> *in_3;
        std::string* in_4 = new std::string;
        req->m >> *in_4;
        rrr::i32* out_0 = new rrr::i32;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete in_3;
            delete in_4;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->Replicate(*in_0, *in_1, *in_2, *in_3, *in_4, out_0, __defer__);
    }
    void __BatchReplicate__wrapper__(rusty::Box<rrr::Request> req, rrr::WeakServerConnection weak_sconn) {
        rrr::i32* in_0 = new rrr::i32;
        req->m >> *in_0;
        rrr::i64* in_1 = new rrr::i64;
        req->m >> *in_1;
        std::vector<rrr::i64>* in_2 = new std::vector<rrr::i64>;
        req->m >> *in_2;
        std::vector<rrr::i64>* in_3 = new std::vector<rrr::i64>;
        req->m >> *in_3;
        std::vector<rrr::i64>* in_4 = new std::vector<rrr::i64>;
        req->m >> *in_4;
        std::vector<std::string>* in_5 = new std::vector<std::string>;
        req->m >> *in_5;
        rrr::i32* out_0 = new rrr::i32;
        rrr::i64* out_1 = new rrr::i64;
        auto __marshal_reply__ = [=] {
            auto sconn_opt = weak_sconn.upgrade();
            if (sconn_opt.is_some()) {
                auto sconn = sconn_opt.unwrap();
                const_cast<rrr::ServerConnection&>(*sconn) << *out_0;
                const_cast<rrr::ServerConnection&>(*sconn) << *out_1;
            }
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete in_3;
            delete in_4;
            delete in_5;
            delete out_0;
            delete out_1;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(std::move(req), weak_sconn, __marshal_reply__, __cleanup__);
        this->BatchReplicate(*in_0, *in_1, *in_2, *in_3, *in_4, *in_5, out_0, out_1, __defer__);
    }
};

class LuigiProxy {
protected:
    rrr::Client* __cl__;
public:
    LuigiProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::FutureResult async_Dispatch(const rrr::i64& txn_id, const rrr::i64& expected_time, const rrr::i32& worker_id, const std::vector<rrr::i32>& involved_shards, const std::string& ops_data, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::DISPATCH, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << txn_id;
        *__cl__ << expected_time;
        *__cl__ << worker_id;
        *__cl__ << involved_shards;
        *__cl__ << ops_data;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 Dispatch(const rrr::i64& txn_id, const rrr::i64& expected_time, const rrr::i32& worker_id, const std::vector<rrr::i32>& involved_shards, const std::string& ops_data, rrr::i32* status, rrr::i64* commit_timestamp, std::string* results_data) {
        auto __fu_result__ = this->async_Dispatch(txn_id, expected_time, worker_id, involved_shards, ops_data);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
            __fu__->get_reply() >> *commit_timestamp;
            __fu__->get_reply() >> *results_data;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_OwdPing(const rrr::i64& send_time, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::OWDPING, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << send_time;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 OwdPing(const rrr::i64& send_time, rrr::i32* status) {
        auto __fu_result__ = this->async_OwdPing(send_time);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_DeadlinePropose(const rrr::i64& tid, const rrr::i32& src_shard, const rrr::i64& proposed_ts, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::DEADLINEPROPOSE, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << tid;
        *__cl__ << src_shard;
        *__cl__ << proposed_ts;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 DeadlinePropose(const rrr::i64& tid, const rrr::i32& src_shard, const rrr::i64& proposed_ts, rrr::i32* status) {
        auto __fu_result__ = this->async_DeadlinePropose(tid, src_shard, proposed_ts);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_DeadlineConfirm(const rrr::i64& tid, const rrr::i32& src_shard, const rrr::i64& agreed_ts, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::DEADLINECONFIRM, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << tid;
        *__cl__ << src_shard;
        *__cl__ << agreed_ts;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 DeadlineConfirm(const rrr::i64& tid, const rrr::i32& src_shard, const rrr::i64& agreed_ts, rrr::i32* status) {
        auto __fu_result__ = this->async_DeadlineConfirm(tid, src_shard, agreed_ts);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_DeadlineBatchPropose(const std::vector<rrr::i64>& tids, const rrr::i32& src_shard, const std::vector<rrr::i64>& proposed_timestamps, const std::vector<rrr::i64>& watermarks, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::DEADLINEBATCHPROPOSE, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << tids;
        *__cl__ << src_shard;
        *__cl__ << proposed_timestamps;
        *__cl__ << watermarks;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 DeadlineBatchPropose(const std::vector<rrr::i64>& tids, const rrr::i32& src_shard, const std::vector<rrr::i64>& proposed_timestamps, const std::vector<rrr::i64>& watermarks, rrr::i32* status) {
        auto __fu_result__ = this->async_DeadlineBatchPropose(tids, src_shard, proposed_timestamps, watermarks);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_DeadlineBatchConfirm(const std::vector<rrr::i64>& tids, const rrr::i32& src_shard, const std::vector<rrr::i64>& agreed_timestamps, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::DEADLINEBATCHCONFIRM, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << tids;
        *__cl__ << src_shard;
        *__cl__ << agreed_timestamps;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 DeadlineBatchConfirm(const std::vector<rrr::i64>& tids, const rrr::i32& src_shard, const std::vector<rrr::i64>& agreed_timestamps, rrr::i32* status) {
        auto __fu_result__ = this->async_DeadlineBatchConfirm(tids, src_shard, agreed_timestamps);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_WatermarkExchange(const rrr::i32& src_shard, const std::vector<rrr::i64>& watermarks, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::WATERMARKEXCHANGE, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << src_shard;
        *__cl__ << watermarks;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 WatermarkExchange(const rrr::i32& src_shard, const std::vector<rrr::i64>& watermarks, rrr::i32* status) {
        auto __fu_result__ = this->async_WatermarkExchange(src_shard, watermarks);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_Replicate(const rrr::i32& worker_id, const rrr::i64& slot_id, const rrr::i64& txn_id, const rrr::i64& timestamp, const std::string& log_data, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::REPLICATE, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << worker_id;
        *__cl__ << slot_id;
        *__cl__ << txn_id;
        *__cl__ << timestamp;
        *__cl__ << log_data;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 Replicate(const rrr::i32& worker_id, const rrr::i64& slot_id, const rrr::i64& txn_id, const rrr::i64& timestamp, const std::string& log_data, rrr::i32* status) {
        auto __fu_result__ = this->async_Replicate(worker_id, slot_id, txn_id, timestamp, log_data);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
        }
        // Arc auto-released
        return __ret__;
    }
    rrr::FutureResult async_BatchReplicate(const rrr::i32& worker_id, const rrr::i64& prev_committed_slot, const std::vector<rrr::i64>& slot_ids, const std::vector<rrr::i64>& txn_ids, const std::vector<rrr::i64>& timestamps, const std::vector<std::string>& log_entries, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        auto __fu_result__ = __cl__->begin_request(LuigiService::BATCHREPLICATE, __fu_attr__);
        if (__fu_result__.is_err()) {
            return __fu_result__;  // Propagate error
        }
        auto __fu__ = __fu_result__.unwrap();
        *__cl__ << worker_id;
        *__cl__ << prev_committed_slot;
        *__cl__ << slot_ids;
        *__cl__ << txn_ids;
        *__cl__ << timestamps;
        *__cl__ << log_entries;
        __cl__->end_request();
        return rrr::FutureResult::Ok(__fu__);
    }
    rrr::i32 BatchReplicate(const rrr::i32& worker_id, const rrr::i64& prev_committed_slot, const std::vector<rrr::i64>& slot_ids, const std::vector<rrr::i64>& txn_ids, const std::vector<rrr::i64>& timestamps, const std::vector<std::string>& log_entries, rrr::i32* status, rrr::i64* last_appended_slot) {
        auto __fu_result__ = this->async_BatchReplicate(worker_id, prev_committed_slot, slot_ids, txn_ids, timestamps, log_entries);
        if (__fu_result__.is_err()) {
            return __fu_result__.unwrap_err();  // Return error code
        }
        auto __fu__ = __fu_result__.unwrap();
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *status;
            __fu__->get_reply() >> *last_appended_slot;
        }
        // Arc auto-released
        return __ret__;
    }
};

} // namespace janus



