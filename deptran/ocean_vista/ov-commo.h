//

#pragma once
#include "brq/commo.h"

namespace rococo {
class ov_ts_t;
class OVCommo : public BrqCommo {
 public:
  using BrqCommo::BrqCommo;

  void SendHandoutRo(SimpleCommand& cmd,
                     const function<void(int res,
                                         SimpleCommand& cmd,
                                         map<int, mdb::version_t>& vers)>&)
  override;



  void SendCreateTs(txnid_t txn_id,
                     const function<void(int64_t ts_raw, siteid_t server_id)>&);

  void SendStoredRemoveTs(txnid_t txn_id, int64_t timestamp, int16_t site_id,
                      const function<void(int res)>&);


  void BroadcastStore(parid_t par_id,
                          txnid_t txn_id,
                          vector<SimpleCommand>& cmds,
                          OVStoreReq &req,
                          const function<void(int, OVStoreRes&)>& callback);

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
      txnid_t cmd_id,
      ChronosCommitReq &chr_req,
      const function<void(int32_t, ChronosCommitRes&, TxnOutput&)>& callback) ;


  void BroadcastExecute(
      parid_t par_id,
      txnid_t cmd_id,
      OVExecuteReq &chr_req,
      const function<void(int32_t, OVExecuteRes&, TxnOutput&)>& callback) ;

  void SendPublish(siteid_t siteid,
                   const ov_ts_t& dc_vwm,
                   const function<void(const ov_ts_t&)>&);

  void SendExchange(siteid_t target_siteid,
                    const std::string& my_dcname,
                    const ov_ts_t& my_dvw,
                    const function<void(const ov_ts_t&)>&);


};

} // namespace


//
// Created by micha on 2020/4/7.
//
