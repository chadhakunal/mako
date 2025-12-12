// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * Client.h:
 *   Client information - issue eRPC request
 *
 **********************************************************************/

#ifndef _LIB_CLIENT_H_
#define _LIB_CLIENT_H_

#include "lib/fasttransport.h"
#include "lib/common.h"

void register_sync_util_sc(std::function<int()>);

namespace mako
{
    // Helper class for building Luigi dispatch requests
    class LuigiDispatchRequestBuilder {
    public:
        LuigiDispatchRequestBuilder() {
            request_ = new luigi_dispatch_request_t;
            request_->num_ops = 0;
            request_->num_involved_shards = 0;
            data_ptr_ = request_->ops_data;
            msg_len_ = 0;
        }

        ~LuigiDispatchRequestBuilder() {
            delete request_;
        }

        void set_header(uint16_t server_id, uint64_t txn_id, uint64_t expected_time) {
            request_->target_server_id = server_id;
            request_->txn_id = txn_id;
            request_->expected_time = expected_time;
        }

        void set_req_nr(uint32_t req_nr) {
            request_->req_nr = req_nr;
        }
        
        // Set the list of all shards involved in this transaction
        // This is needed for multi-shard leader agreement
        void set_involved_shards(const std::vector<int>& shards) {
            request_->num_involved_shards = std::min(shards.size(), luigi_max_shards);
            for (size_t i = 0; i < request_->num_involved_shards; i++) {
                request_->involved_shards[i] = static_cast<uint16_t>(shards[i]);
            }
        }

        void add_op(uint16_t table_id, uint8_t op_type, 
                   const std::string& key, const std::string& value) {
            request_->num_ops++;
            
            // table_id (2 bytes)
            memcpy(data_ptr_, &table_id, sizeof(uint16_t));
            data_ptr_ += sizeof(uint16_t);
            msg_len_ += sizeof(uint16_t);
            
            // op_type (1 byte)
            memcpy(data_ptr_, &op_type, sizeof(uint8_t));
            data_ptr_ += sizeof(uint8_t);
            msg_len_ += sizeof(uint8_t);
            
            // klen (2 bytes)
            uint16_t klen = key.size();
            memcpy(data_ptr_, &klen, sizeof(uint16_t));
            data_ptr_ += sizeof(uint16_t);
            msg_len_ += sizeof(uint16_t);
            
            // vlen (2 bytes)
            uint16_t vlen = value.size();
            memcpy(data_ptr_, &vlen, sizeof(uint16_t));
            data_ptr_ += sizeof(uint16_t);
            msg_len_ += sizeof(uint16_t);
            
            // key data
            memcpy(data_ptr_, key.c_str(), klen);
            data_ptr_ += klen;
            msg_len_ += klen;
            
            // value data
            if (vlen > 0) {
                memcpy(data_ptr_, value.c_str(), vlen);
                data_ptr_ += vlen;
                msg_len_ += vlen;
            }
        }

        luigi_dispatch_request_t* get_request() { return request_; }
        size_t get_total_size() { 
            return sizeof(luigi_dispatch_request_t) - sizeof(request_->ops_data) + msg_len_; 
        }

    private:
        luigi_dispatch_request_t* request_;
        char* data_ptr_;
        size_t msg_len_;
    };
    using namespace std;

    class Client : public TransportReceiver
    {
    public:
        Client(std::string file,
               Transport *transport,
               uint64_t clientid);

        void ReceiveResponse(uint8_t reqType, char *respBuf);

        void InvokeGet(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            const string &key,
                            uint16_t table_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);
        
        void InvokeScan(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            const string &start_key,
                            const string &end_key,
                            uint16_t table_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeBatchLock(uint64_t txn_nr,
                            uint16_t id,
                            map<int, BatchLockRequestWrapper> &request_batch_per_shard,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeLock(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            const string &key,
                            const string &value,
                            uint16_t table_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeValidate(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeInstall(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            char*cc,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeUnLock(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeAbort(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            basic_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeGetTimestamp(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);
        
        void InvokeExchangeWatermark(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeControl(uint64_t txn_nr,
                           int control,
                           uint64_t value,
                            int dstShardIdx,
                            uint16_t server_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);

        void InvokeWarmup(uint64_t txn_nr,
                           uint32_t req_val,
                           uint8_t centerId,
                            int dstShardIdx,
                            uint16_t server_id,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout);


        void InvokeSerializeUtil(uint64_t txn_nr,
                            int dstShardIdx,
                            uint16_t server_id,
                            char*cc,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout); 

        // Luigi: timestamp-ordered execution dispatch
        void InvokeLuigiDispatch(
            uint64_t txn_nr,
            std::map<int, LuigiDispatchRequestBuilder*>& requests_per_shard,
                            resp_continuation_t continuation,
                            error_continuation_t error_continuation,
                            uint32_t timeout); 

        // OWD: Ping a single shard for latency measurement
        void InvokeOwdPing(
            uint64_t txn_nr,
            int dst_shard_idx,
            resp_continuation_t continuation,
            error_continuation_t error_continuation,
            uint32_t timeout);

        // Luigi Status Check: Poll for async dispatch completion
        void InvokeLuigiStatusCheck(
            uint64_t txn_nr,
            uint64_t txn_id,                    // The txn to check status for
            const std::vector<int>& shard_indices,  // Which shards to poll
            resp_continuation_t continuation,
            error_continuation_t error_continuation,
            uint32_t timeout);

        void HandleGetReply(char *respBuf);
        void HandleScanReply(char *respBuf);
        void HandleLockReply(char *respBuf);
        void HandleBatchLockReply(char *respBuf);
        void HandleValidateReply(char *respBuf);
        void HandleControlReply(char *respBuf);
        void HandleSerializeUtil(char *respBuf);
        void HandleInstallReply(char *respBuf);
        void HandleUnLockReply(char *respBuf);
        void HandleAbortReply(char *respBuf);
        void HandleGetTimestamp(char *respBuf);
        void HandleWatermarkReply(char *respBuf);
        void HandleWarmupReply(char *respBuf);
        void HandleLuigiDispatchReply(char *respBuf);
        void HandleOwdPingReply(char *respBuf);
        void HandleLuigiStatusReply(char *respBuf);
        size_t ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) override { PPanic("Not implemented."); };
        bool Blocked() override { return blocked; };
        void SetNumResponseWaiting(int num_response_waiting) { this->num_response_waiting = num_response_waiting; };

    protected:
        struct PendingRequest
        {
            string request;
            uint64_t req_nr;
            uint64_t txn_nr;
            uint16_t id;
            continuation_t continuation;
            bool continuationInvoked = false;

            inline PendingRequest(){};
            inline PendingRequest(string request, uint64_t req_nr,
                                  uint64_t txn_nr, uint16_t server_id,
                                  continuation_t continuation
                                  )
                : request(request),
                  req_nr(req_nr),
                  txn_nr(txn_nr),
                  id(id),
                  continuation(continuation)
                  {};
            virtual ~PendingRequest(){};
        };

        struct PendingRequestK : public PendingRequest
        {
            error_continuation_t error_continuation;
            resp_continuation_t resp_continuation;

            inline PendingRequestK(){};
            inline PendingRequestK(
                string request, uint64_t clientReqId, uint64_t clienttxn_nr,
                uint16_t server_id,
                resp_continuation_t resp_continuation,
                error_continuation_t error_continuation)
                : PendingRequest(request, clientReqId, clienttxn_nr, id, nullptr),
                  error_continuation(error_continuation),
                  resp_continuation(resp_continuation){};
        };

        transport::Configuration config;
        std::atomic<uint32_t> lastReqId;
        PendingRequestK crtReqK;
        Transport *transport;
        uint64_t clientid;
        bool blocked;
        int num_response_waiting;
        
        int current_term;
    };

}
#endif  /* _LIB_CONFIGURATION_H_ */
