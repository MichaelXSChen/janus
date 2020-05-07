//
// Created by micha on 2020/3/23.
//

#include "deptran/rcc/dtxn.h"
#include "../rcc/graph_marshaler.h"
#include "commo.h"
#include "marshallable.h"
#include "txn_chopper.h"

namespace rococo {

void ChronosCommo::SubmitReq(vector<TxPieceData> &cmd,
                                const ChronosDispatchReq& chr_req,
                                bool is_local,
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
  auto proxy_info = EdgeServerForPartition(cmd[0].PartitionId());
  //xs: seems to dispatch only the nearst replica fo the shard


  auto proxy = proxy_info.second;
  //XS: proxy is the rpc client side handler.
  if (is_local){
    Log_info("dispatch local transaction to partition %u, proxy (site) = %hu", cmd[0].PartitionId(), proxy_info.first);
  }else{
    Log_info("dispatch non-local transaction to partition %u, proxy (site) = %hu", cmd[0].PartitionId(), proxy_info.first);
  }

  Future::safe_release(proxy->async_ChronosDispatch(cmd, chr_req, fuattr));
}

void ChronosCommo::SendStoreLocal(const vector<SimpleCommand> &cmd,
                                  const ChronosStoreLocalReq &req,
                                  const function<void(int, int, ChronosStoreLocalRes &)>& callback) {

  rrr::FutureAttr fuattr;
  auto tid = cmd[0].root_id_;
  auto par_id = cmd[0].partition_id_;

  verify(this->site_info_ != nullptr);
  verify(cmd[0].PartitionId() == this->site_info_->partition_id_);
  auto site_proxy_pairs = ProxiesInPartition(cmd[0].PartitionId());
  int partition_n_replicas = site_proxy_pairs.size();

  std::function<void(Future *)> cb =
      [callback, tid, par_id, partition_n_replicas](Future *fu) {
        int res;
        TxnOutput output;
        ChronosStoreLocalRes chr_res;
        fu->get_reply() >> res >> chr_res;
        callback(partition_n_replicas-1, res, chr_res);
      };
  fuattr.callback = cb;

  for (auto& pair: site_proxy_pairs){
     if (pair.first != site_info_->id) {
       Log_info("sending StoreLocl to site %hu, my site id = %hu, my partition id = %u, ts = %lu:%lu:%hu",
           pair.first,
           site_info_->id,
           site_info_->partition_id_,
           req.txn_timestamp,
           req.txn_strech_counter,
           req.txn_site_id);
       Future::safe_release(pair.second->async_ChronosStoreLocal(cmd, req, fuattr));
     }
  }

}

void ChronosCommo::SendHandoutRo(SimpleCommand &cmd,
                                 const function<void(int res,
                                                     SimpleCommand &cmd,
                                                     map<int,
                                                         mdb::version_t> &vers)> &) {
  verify(0);
}


void ChronosCommo::BroadcastPreAccept(
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
    Log_info("ChronosPreAccept");
    f = proxy->async_ChronosPreAccept(txn_id, cmds, chr_req, fuattrChro);
    Future::safe_release(f);
  }
}

void ChronosCommo::BroadcastAccept(parid_t par_id,
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

void ChronosCommo::BroadcastCommit(
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
