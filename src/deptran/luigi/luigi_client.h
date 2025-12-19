#pragma once

/**
 * LuigiClient: Standalone client for Luigi timestamp-ordered execution
 * protocol.
 *
 * This is a complete separation from Mako's Client, handling only
 * Luigi-specific request types while using Mako's eRPC transport.
 */

#include <functional>
#include <map>
#include <string>
#include <vector>

#include "lib/fasttransport.h"
#include "lib/transport.h"

#include "luigi_common.h"

namespace janus {

/**
 * LuigiDispatchBuilder: Helper class for building Luigi dispatch requests.
 *
 * Provides a fluent interface for constructing dispatch requests
 * with multiple operations.
 */
class LuigiDispatchBuilder {
public:
  LuigiDispatchBuilder();
  ~LuigiDispatchBuilder();

  // Prevent copying (owns request buffer)
  LuigiDispatchBuilder(const LuigiDispatchBuilder &) = delete;
  LuigiDispatchBuilder &operator=(const LuigiDispatchBuilder &) = delete;

  // Allow moving
  LuigiDispatchBuilder(LuigiDispatchBuilder &&other) noexcept;
  LuigiDispatchBuilder &operator=(LuigiDispatchBuilder &&other) noexcept;

  /**
   * Set the transaction ID.
   */
  LuigiDispatchBuilder &SetTxnId(uint64_t txn_id);

  /**
   * Set the expected execution timestamp.
   */
  LuigiDispatchBuilder &SetExpectedTime(uint64_t expected_time);

  /**
   * Set the target server ID.
   */
  LuigiDispatchBuilder &SetTargetServer(uint16_t server_id);

  /**
   * Set the request number.
   */
  LuigiDispatchBuilder &SetReqNr(uint32_t req_nr);

  /**
   * Set the list of involved shards (for multi-shard agreement).
   */
  LuigiDispatchBuilder &SetInvolvedShards(const std::vector<uint32_t> &shards);

  /**
   * Set the transaction type (TPC-C: NEW_ORDER, PAYMENT, etc.).
   */
  LuigiDispatchBuilder &SetTxnType(uint32_t txn_type);

  /**
   * Add a read operation.
   */
  LuigiDispatchBuilder &AddRead(uint16_t table_id, const std::string &key);

  /**
   * Add a write operation.
   */
  LuigiDispatchBuilder &AddWrite(uint16_t table_id, const std::string &key,
                                 const std::string &value);

  /**
   * Add a working_set entry (TPC-C parameters).
   */
  LuigiDispatchBuilder &AddWorkingSetEntry(int32_t var_id, const std::string &value);

  /**
   * Set entire working_set from a map.
   */
  LuigiDispatchBuilder &SetWorkingSet(const std::map<int32_t, std::string> &working_set);

  /**
   * Get the underlying request structure.
   */
  luigi::DispatchRequest *GetRequest() { return request_; }

  /**
   * Get the total serialized size.
   */
  size_t GetTotalSize() const;

private:
  luigi::DispatchRequest *request_;
  size_t msg_len_ = 0; // Current offset in ops_data
  size_t ws_len_ = 0;  // Current offset in working_set_data
};

/**
 * Response/error continuation types.
 */
using ResponseCallback = std::function<void(char *respBuf)>;
using ErrorCallback = std::function<void()>;

/**
 * LuigiClient: Client for Luigi protocol operations.
 *
 * Uses Mako's eRPC transport for network communication.
 */
class LuigiClient : public TransportReceiver {
public:
  LuigiClient(const std::string &config_file, Transport *transport,
              uint64_t client_id = 0);
  ~LuigiClient() = default;

  /**
   * Receive and dispatch response to appropriate handler.
   */
  void ReceiveResponse(uint8_t reqType, char *respBuf) override;

  /**
   * Check if client is blocked waiting for response.
   */
  bool Blocked() override { return blocked_; }

  //=========================================================================
  // Luigi Protocol Operations
  //=========================================================================

  /**
   * Send Luigi dispatch requests to multiple shards.
   *
   * @param txn_nr Transaction number for tracking
   * @param requests_per_shard Map of shard_idx -> dispatch builder
   * @param continuation Callback on response
   * @param error_continuation Callback on error
   * @param timeout Request timeout in ms
   */
  void InvokeDispatch(uint64_t txn_nr,
                      std::map<int, LuigiDispatchBuilder *> &requests_per_shard,
                      ResponseCallback continuation,
                      ErrorCallback error_continuation, uint32_t timeout = 250);

  /**
   * Poll for completion of async dispatch.
   *
   * @param txn_nr Transaction number
   * @param txn_id Luigi transaction ID
   * @param shard_indices Shards to query
   * @param continuation Callback on response
   * @param error_continuation Callback on error
   * @param timeout Request timeout in ms
   */
  void InvokeStatusCheck(uint64_t txn_nr, uint64_t txn_id,
                         const std::vector<int> &shard_indices,
                         ResponseCallback continuation,
                         ErrorCallback error_continuation,
                         uint32_t timeout = 250);

  /**
   * Send OWD (one-way delay) ping to a shard.
   *
   * @param txn_nr Transaction number
   * @param shard_idx Target shard
   * @param continuation Callback on response
   * @param error_continuation Callback on error
   * @param timeout Request timeout in ms
   */
  void InvokeOwdPing(uint64_t txn_nr, int shard_idx,
                     ResponseCallback continuation,
                     ErrorCallback error_continuation, uint32_t timeout = 250);

  /**
   * Send deadline proposal to a remote leader (for timestamp agreement).
   *
   * @param target_shard Target shard ID
   * @param tid Transaction ID
   * @param proposed_ts Our proposed timestamp
   * @param phase 1 = initial proposal, 2 = confirmation
   * @param continuation Callback on response
   * @param error_continuation Callback on error
   */
  void InvokeDeadlinePropose(uint32_t target_shard, uint64_t tid,
                             uint64_t proposed_ts, uint32_t phase,
                             ResponseCallback continuation,
                             ErrorCallback error_continuation);

  /**
   * Send deadline confirmation to a remote leader (after repositioning).
   *
   * @param target_shard Target shard ID
   * @param tid Transaction ID
   * @param new_ts New timestamp after repositioning
   * @param continuation Callback on response
   * @param error_continuation Callback on error
   */
  void InvokeDeadlineConfirm(uint32_t target_shard, uint64_t tid,
                             uint64_t new_ts, ResponseCallback continuation,
                             ErrorCallback error_continuation);

  /**
   * Broadcast watermarks to a remote leader.
   *
   * @param target_shard Target shard ID
   * @param watermarks Vector of watermark values
   * @param continuation Callback on response
   * @param error_continuation Callback on error
   */
  void InvokeWatermarkExchange(uint32_t target_shard,
                               const std::vector<uint64_t> &watermarks,
                               ResponseCallback continuation,
                               ErrorCallback error_continuation);

protected:
  void HandleDispatchReply(char *respBuf);
  void HandleStatusReply(char *respBuf);
  void HandleOwdPingReply(char *respBuf);
  void HandleDeadlineProposeReply(char *respBuf);
  void HandleDeadlineConfirmReply(char *respBuf);
  void HandleWatermarkExchangeReply(char *respBuf);

public:
  // Set local receiver for handling local shard requests directly
  void SetLocalReceiver(TransportReceiver* receiver) { local_receiver_ = receiver; }

private:
  transport::Configuration config_;
  Transport *rpc_transport_;
  uint64_t client_id_;
  TransportReceiver* local_receiver_ = nullptr;  // For local shard handling

  // Request tracking
  uint32_t last_req_id_ = 0;
  bool blocked_ = false;
  int num_response_waiting_ = 0;

  // Current pending request state
  struct PendingRequest {
    std::string name;
    uint32_t req_nr = 0;
    uint64_t txn_nr = 0;
    uint16_t server_id = 0;
    ResponseCallback response_cb;
    ErrorCallback error_cb;
  };
  PendingRequest current_request_;
};

} // namespace janus
