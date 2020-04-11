//

#pragma once
#include "brq/commo.h"

namespace rococo {

class OVCommo : public BrqCommo {
 public:
  using BrqCommo::BrqCommo;

  void SendHandoutRo(SimpleCommand& cmd,
                     const function<void(int res,
                                         SimpleCommand& cmd,
                                         map<int, mdb::version_t>& vers)>&)
  override;



  void SendCreateTs(txnid_t txn_id,
                      vector<parid_t> &par_ids,
                     const function<void(int64_t ts_raw, siteid_t server_id)>&);




  //xs's code
  void SendDispatch(vector<SimpleCommand>& cmd,
                    const ChronosDispatchReq& req,
                    const function<void(int res,
                                        TxnOutput& output,
                                        ChronosDispatchRes &chr_res)>&)  ;

  void BroadcastPreAccept(parid_t par_id,
                          txnid_t txn_id,
                          ballot_t ballot,
                          vector<SimpleCommand>& cmds,
                          ChronosPreAcceptReq &chr_req,
                          const function<void(int32_t, std::shared_ptr<ChronosPreAcceptRes>)>& callback)  ;

  void BroadcastAccept(parid_t par_id,
                       txnid_t cmd_id,
                       ballot_t ballot,
                       ChronosAcceptReq &res,
                       const function<void(int, ChronosAcceptRes&)>& callback) ;

  void BroadcastCommit(
      parid_t,
      txnid_t cmd_id_,
      ChronosCommitReq &chr_req,
      const function<void(int32_t, ChronosCommitRes&, TxnOutput&)>& callback) ;


};

} // namespace


//
// Created by micha on 2020/4/7.
//
