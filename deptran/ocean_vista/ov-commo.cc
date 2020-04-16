//
// Created by micha on 2020/3/23.
//

#include "deptran/rcc/dtxn.h"
#include "../rcc/graph_marshaler.h"
#include "ov-commo.h"
#include "marshallable.h"
#include "txn_chopper.h"
#include "ov-txn_mgr.h"

namespace rococo {

void OVCommo::SendDispatch(vector<TxPieceData> &cmd,
                                const ChronosDispatchReq& chr_req,
                                const function<void(int res,
                                                    TxnOutput &cmd,
                                                    ChronosDispatchRes &chr_res)> &callback) {

  rrr::FutureAttr fuattr;
  auto tid = cmd[0].root_id_;
  auto par_id = cmd[0].partition_id_;
  std::function<void(Future *)> cb =
      [callback, tid, par_id](Future *fu) {
        int res;
        TxnOutput output;
        ChronosDispatchRes chr_res;
        fu->get_reply() >> res >> chr_res >> output;
        callback(res, output, chr_res);
      };
  fuattr.callback = cb;
  auto proxy_info = NearestProxyForPartition(cmd[0].PartitionId());
  //xs: seems to dispatch only the nearst replica fo the shard


  auto proxy = proxy_info.second;
  //XS: proxy is the rpc client side handler.
  Log_debug("dispatch to %ld, proxy (site) = %d", cmd[0].PartitionId(), proxy_info.first);

  Future::safe_release(proxy->async_ChronosDispatch(cmd, chr_req, fuattr));
}

void OVCommo::SendCreateTs(txnid_t txn_id,
                           const function<void(int64_t ts_raw, siteid_t site_id)> &callback) {

  rrr::FutureAttr fuattr;
  std::function<void(Future *)> cb =
      [callback ](Future *fu) {
        int64_t ts_raw;
        int16_t site_id;
        fu->get_reply() >> ts_raw >> site_id;
        callback(ts_raw, site_id);
      };
  fuattr.callback = cb;

  auto rand_id_proxy_pair = NearestRandomProxy();
  auto site_id = rand_id_proxy_pair.first;
  auto proxy = rand_id_proxy_pair.second;

  Log_debug("xsxs Sending CreateTs for txn %lu to site %hd", txn_id, site_id);

  Future::safe_release(proxy->async_OVCreateTs(txn_id, fuattr));
}

void OVCommo::SendStoredRemoveTs(txnid_t txn_id, int64_t timestamp, int16_t site_id, const function<void(int res)> & callback) {
  rrr::FutureAttr fuattr;
  std::function<void(Future *)> cb =
      [callback ](Future *fu) {
        int res;
        fu->get_reply() >> res;
        callback(res);
      };
  fuattr.callback = cb;

  auto proxy = rpc_proxies_[site_id];
  Log_debug("xsxs Sending StoredRemove for txn %lu to site %hd", txn_id, site_id);

  Future::safe_release(proxy->async_OVStoredRemoveTs(txn_id, timestamp, site_id, fuattr));

}

void OVCommo::SendPublish(uint16_t siteid, const ov_ts_t &dc_vwm, const function<void(const ov_ts_t &)>& callback) {

  rrr::FutureAttr fuattr;
  std::function<void(Future *)> cb =
      [callback ](Future *fu) {
        ov_ts_t ret_ovts;
        fu->get_reply() >> ret_ovts.timestamp_ >> ret_ovts.site_id_;
        callback(ret_ovts);
      };
  fuattr.callback = cb;

  auto proxy = rpc_proxies_[siteid];

  Future::safe_release(proxy->async_OVPublish(dc_vwm.timestamp_, dc_vwm.site_id_, fuattr));

}

void OVCommo::SendExchange(siteid_t target_siteid,
                           const std::string &my_dcname,
                           const rococo::ov_ts_t &my_dvw,
                           const function<void(const ov_ts_t&)>& callback) {
  rrr::FutureAttr fuattr;
  std::function<void(Future *)> cb =
      [callback ](Future *fu) {
        ov_ts_t ret_ovts;
        fu->get_reply() >> ret_ovts.timestamp_ >> ret_ovts.site_id_;
        callback(ret_ovts);
      };
  fuattr.callback = cb;

  auto proxy = rpc_proxies_[target_siteid];

  Future::safe_release(proxy->async_OVExchange(my_dcname, my_dvw.timestamp_, my_dvw.site_id_, fuattr));

}



void OVCommo::SendHandoutRo(SimpleCommand &cmd,
                                 const function<void(int res,
                                                     SimpleCommand &cmd,
                                                     map<int,
                                                         mdb::version_t> &vers)> &) {
  verify(0);
}


void OVCommo::BroadcastStore(parid_t par_id,
                    txnid_t txn_id,
                    vector<SimpleCommand>& cmds,
                    OVStoreReq &req,
                    const function<void(int, OVStoreRes&)>& callback){
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback](Future *fu) {
      int32_t res;
      OVStoreRes ov_res;
      fu->get_reply() >> res >> ov_res;
      callback(res, ov_res);
    };
    verify(txn_id > 0);
    Future *f = nullptr;
    Log_debug("Sending txn %lu to par %u to site %u", txn_id, par_id,  p.first);
    f = proxy->async_OVStore(txn_id, cmds, req, fuattr);
    Future::safe_release(f);
  }
}

void OVCommo::BroadcastExecute(uint32_t par_id,
                               uint64_t cmd_id,
                               OVExecuteReq &chr_req,
                               const function<void (int32_t, OVExecuteRes &, TxnOutput &)> &callback) {

  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback](Future *fu) {
      int32_t res;
      TxnOutput output;
      OVExecuteRes ov_res;
      fu->get_reply() >> res >> ov_res >> output;
      callback(res, ov_res, output);
    };
    Future::safe_release(proxy->async_OVExecute(cmd_id, chr_req, fuattr));
  }
}


void OVCommo::BroadcastPreAccept(
    parid_t par_id,
    txnid_t txn_id,
    ballot_t ballot,
    vector<TxPieceData> &cmds,
    ChronosPreAcceptReq &chr_req,
    const function<void(int, std::shared_ptr<ChronosPreAcceptRes>)> &callback) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());


  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattrChro;
    fuattrChro.callback = [callback](Future *fu) {
      int32_t res;
      auto chr_res = std::make_shared<ChronosPreAcceptRes>();
      fu->get_reply() >> res >> *chr_res;
      callback(res, chr_res);
    };


    verify(txn_id > 0);
    Future *f = nullptr;
    f = proxy->async_ChronosPreAccept(txn_id, cmds, chr_req, fuattrChro);
    Future::safe_release(f);
  }
}

void OVCommo::BroadcastAccept(parid_t par_id,
                                   txnid_t cmd_id,
                                   ballot_t ballot,
                                   ChronosAcceptReq &chr_req,
                                   const function<void(int, ChronosAcceptRes&)> &callback) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback](Future *fu) {
      int32_t res;
      ChronosAcceptRes chr_res;
      fu->get_reply() >> res >> chr_res;
      callback(res, chr_res);
    };
    verify(cmd_id > 0);
    Future::safe_release(proxy->async_ChronosAccept(cmd_id,
                                                    ballot,
                                                    chr_req,
                                                    fuattr));
  }
}

void OVCommo::BroadcastCommit(
    parid_t par_id,
    txnid_t cmd_id,
    ChronosCommitReq &chr_req,
    const function<void(int32_t, ChronosCommitRes &,TxnOutput &)> &callback) {

  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattrChronos;
    fuattrChronos.callback = [callback](Future *fu) {
      int32_t res;
      TxnOutput output;
      ChronosCommitRes chr_res;
      fu->get_reply() >> res >> chr_res >> output;
      callback(res, chr_res, output);
    };
    Future::safe_release(proxy->async_ChronosCommit(cmd_id, chr_req, fuattrChronos));
  }
}

} // namespace janus
