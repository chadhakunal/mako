#pragma once

#include "deptran/__dep__.h"       // Must come first for type definitions
#include "deptran/procedure.h"     // For SimpleCommand
#include "deptran/command_marshaler.h"  // For command marshaling
#include "deptran/rcc_rpc.h"       // Generated RPC (needs SimpleCommand)
#include "luigi_entry.h"

namespace janus {

class SchedulerLuigi;

/**
 * LuigiLeaderServiceImpl: Handles leader-to-leader RPC for timestamp agreement.
 * 
 * This service implements the LuigiLeaderService interface defined in rcc_rpc.rpc.
 * Each leader runs this service to receive deadline proposals from other leaders.
 * 
 * Protocol:
 * 1. DeadlinePropose: Other leader sends its proposed timestamp
 *    - We return our proposed timestamp for the same txn
 *    - Both sides compute agreed_ts = max(all proposals)
 *    
 * 2. DeadlineConfirm: Other leader confirms it has repositioned to agreed_ts
 *    - Used in Case 2: we're waiting for stragglers to reposition
 */
class LuigiLeaderServiceImpl : public LuigiLeaderService {
 public:
  LuigiLeaderServiceImpl(SchedulerLuigi* scheduler);
  ~LuigiLeaderServiceImpl() = default;

  //===========================================================================
  // RPC Handlers (called by rrr framework when RPC arrives)
  //===========================================================================
  
  /**
   * Handle incoming deadline proposal from another leader.
   * 
   * When another leader has a multi-shard txn at head of its priority queue,
   * it sends us a DeadlinePropose to exchange timestamps.
   * 
   * We respond with our proposed timestamp for this txn (if we have one).
   * If we don't have this txn yet, we store the remote proposal and respond
   * with ts=0 (meaning "I don't have a proposal yet").
   */
  void DeadlinePropose(const LuigiDeadlineRequest& req,
                       LuigiDeadlineResponse* res,
                       rrr::DeferredReply* defer) override;

  /**
   * Handle incoming confirmation that a leader has repositioned.
   * 
   * After Case 3 resolution (leader repositions to agreed_ts), that leader
   * sends DeadlineConfirm to notify us. If we're in Case 2 (waiting for
   * confirmations), we decrement our pending count and may proceed.
   */
  void DeadlineConfirm(const LuigiDeadlineRequest& req,
                       rrr::i32* status,
                       rrr::DeferredReply* defer) override;

 private:
  SchedulerLuigi* scheduler_;
};

} // namespace janus
