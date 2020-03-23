//
// Created by micha on 2020/3/23.
//


#pragma once
#include "deptran/janus/scheduler.h"
#include "deptran/rcc_rpc.h"
namespace janus {

class RccGraph;
class ChronosCommo;
class SchedulerChronos : public SchedulerJanus {
 public:
  using SchedulerJanus::SchedulerJanus;

  map<txnid_t, shared_ptr<TxRococo>> Aggregate(RccGraph& graph);


  int OnDispatch(const vector<SimpleCommand> &cmd,
                 const ChronosDispatchReq &chr_req,
                 rrr::i32 *res,
                 ChronosDispatchRes *chr_res,
                 TxnOutput* output);


  void OnPreAccept(txnid_t txnid,
                   const vector<SimpleCommand> &cmds,
                   const ChronosPreAcceptReq &chr_req,
                   int32_t *res,
                   ChronosPreAcceptRes *chr_res);


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
                shared_ptr<RccGraph> graph,
                const function<void()> &callback) override;

  ChronosCommo* commo();

  std::atomic<uint64_t> logical_clock {0};

};
} // namespace janus
