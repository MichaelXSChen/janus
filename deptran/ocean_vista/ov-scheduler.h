//
// Created by micha on 2020/3/23.
//


#pragma once
#include "deptran/brq/sched.h"
#include "deptran/rcc_rpc.h"
namespace rococo {

class OVCommo;
class SchedulerOV : public BrqSched {
 public:
  using BrqSched::BrqSched;


  int OnDispatch(const vector<SimpleCommand> &cmd,
                 const ChronosDispatchReq &chr_req,
                 rrr::i32 *res,
                 ChronosDispatchRes *chr_res,
                 TxnOutput* output);


  void OnStore(txnid_t txnid,
                   const vector<SimpleCommand> &cmds,
                   const OVStoreReq &ov_req,
                   int32_t *res,
                   OVStoreRes *ov_res);


  void OnAccept(txnid_t txn_id,
                const ballot_t& ballot,
                const ChronosAcceptReq &chr_req,
                int32_t* res,
                ChronosAcceptRes *chr_res);

  void OnCommit(txnid_t txn_id,
                const ChronosCommitReq &chr_req,
                int32_t *res,
                TxnOutput *output,
                ChronosCommitRes *chr_res,
                const function<void()> &callback);

  int OnInquire(epoch_t epoch,
                cmdid_t cmd_id,
                RccGraph* graph,
                const function<void()> &callback) override;

  OVCommo* commo();

};
} // namespace janus
