#pragma once
#include "../janus/commo.h"

namespace janus {

class ChronosCommo : public JanusCommo {
 public:
  using JanusCommo::JanusCommo;

  void SendHandoutRo(SimpleCommand& cmd,
                     const function<void(int res,
                                         SimpleCommand& cmd,
                                         map<int, mdb::version_t>& vers)>&)
  override;

  void SendFinish(parid_t pid,
                  txnid_t tid,
                  shared_ptr<RccGraph> graph,
                  const function<void(TxnOutput&)>&) override;

  void SendInquire(parid_t pid,
                   epoch_t epoch,
                   txnid_t tid,
                   const function<void(RccGraph& graph)>&) override;




  bool IsGraphOrphan(RccGraph& graph, txnid_t cmd_id);


  //xs's code
  void SendDispatch(vector<SimpleCommand>& cmd,
                    const ChronosDispatchReq& req,
                    const function<void(int res,
                                        TxnOutput& output,
                                        ChronosDispatchRes &chr_res,
                                        RccGraph& graph)>&);

  void BroadcastPreAccept(parid_t par_id,
                          txnid_t txn_id,
                          ballot_t ballot,
                          vector<SimpleCommand>& cmds,
                          ChronosPreAcceptReq &chr_req,
                          const function<void(int32_t, ChronosPreAcceptRes &chr_res)>& callback);

  void BroadcastAccept(parid_t par_id,
                       txnid_t cmd_id,
                       ballot_t ballot,
                       ChronosAcceptReq &res,
                       const function<void(int, ChronosAcceptRes&)>& callback);

  void BroadcastCommit(
      parid_t,
      txnid_t cmd_id_,
      ChronosCommitReq &chr_req,
      const function<void(int32_t, ChronosCommitRes&, TxnOutput&)>& callback);


};

} // namespace
