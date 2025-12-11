#include "deptran/__dep__.h"  // Must come first
#include "luigi_service.h"
#include "luigi_scheduler.h"

namespace janus {

LuigiLeaderServiceImpl::LuigiLeaderServiceImpl(SchedulerLuigi* scheduler)
    : scheduler_(scheduler) {
  verify(scheduler_ != nullptr);
}

void LuigiLeaderServiceImpl::DeadlinePropose(
    const LuigiDeadlineRequest& req,
    LuigiDeadlineResponse* res,
    rrr::DeferredReply* defer) {
  //===========================================================================
  // Phase 1 RPC: Another leader is proposing its timestamp for txn req.tid
  // 
  // We need to:
  // 1. Look up if we have this txn in our pending map
  // 2. If yes, return our proposed timestamp
  // 3. If no, store the remote proposal and return ts=0 (no local proposal yet)
  //
  // The scheduler maintains:
  // - pending_agreement_: map<tid, AgreementState> for txns we're coordinating
  // - remote_proposals_: map<tid, map<shard_id, ts>> for proposals we've received
  //===========================================================================
  
  Log_info("LuigiService: DeadlinePropose received tid=%ld, proposed_ts=%ld, src_shard=%d, phase=%d",
           req.tid, req.proposed_ts, req.src_shard, req.phase);
  
  // Fill basic response fields
  res->tid = req.tid;
  res->shard_id = scheduler_->partition_id();
  res->status = 0;  // Success
  
  // Handle based on phase
  if (req.phase == 1) {
    // Phase 1: Initial proposal exchange
    res->proposed_ts = scheduler_->HandleRemoteDeadlineProposal(
        req.tid, req.src_shard, req.proposed_ts, req.phase);
  } else {
    // Phase 2: Confirmation (should use DeadlineConfirm, but handle here too)
    scheduler_->HandleRemoteDeadlineConfirm(req.tid, req.src_shard, req.proposed_ts);
    res->proposed_ts = req.proposed_ts;  // Echo back
  }
  
  defer->reply();
}

void LuigiLeaderServiceImpl::DeadlineConfirm(
    const LuigiDeadlineRequest& req,
    rrr::i32* status,
    rrr::DeferredReply* defer) {
  //===========================================================================
  // Phase 2 RPC: Another leader confirms it has repositioned to agreed_ts
  //
  // This is sent by Case 3 leaders after they reposition their txn.
  // If we're in Case 2 (waiting for confirmations), we can now proceed.
  //===========================================================================
  
  Log_info("LuigiService: DeadlineConfirm received tid=%ld, new_ts=%ld, src_shard=%d",
           req.tid, req.proposed_ts, req.src_shard);
  
  bool ok = scheduler_->HandleRemoteDeadlineConfirm(
      req.tid, req.src_shard, req.proposed_ts);
  
  *status = ok ? 0 : -1;
  defer->reply();
}

} // namespace janus
