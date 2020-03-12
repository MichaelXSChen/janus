#include "../procedure.h"
#include "deptran/janus/tx.h"
#include "../rococo/graph_marshaler.h"
#include "commo.h"
#include "marshallable.h"

namespace janus {

void ChronosCommo::SendDispatch(vector<TxPieceData> &cmd,
                                const ChronosDispatchReq& chr_req,
                                const function<void(int res,
                                                    TxnOutput &cmd,
                                                    ChronosDispatchRes &chr_res,
                                                    RccGraph &graph)> &callback) {

  rrr::FutureAttr fuattr;
  auto tid = cmd[0].root_id_;
  auto par_id = cmd[0].partition_id_;
  std::function<void(Future *)> cb =
      [callback, tid, par_id](Future *fu) {
        int res;
        TxnOutput output;
        MarshallDeputy md;
        ChronosDispatchRes chr_res;
        fu->get_reply() >> res >> output >> chr_res >> md;
        if (md.kind_ == MarshallDeputy::EMPTY_GRAPH) {
          RccGraph rgraph;
          auto v = rgraph.CreateV(tid);
          TxRococo &info = *v;
          info.partition_.insert(par_id);
          verify(rgraph.vertex_index().size() > 0);
          callback(res, output, chr_res, rgraph);
        } else if (md.kind_ == MarshallDeputy::RCC_GRAPH) {
          RccGraph &graph = dynamic_cast<RccGraph &>(*md.sp_data_);
          callback(res, output, chr_res, graph);
        } else {
          verify(0);
        }
      };
  fuattr.callback = cb;
  auto proxy_info = NearestProxyForPartition(cmd[0].PartitionId());
  auto proxy = proxy_info.second;
  //XS: proxy is the rpc client side handler.
  Log_info("dispatch to %ld, proxy (site) = %d", cmd[0].PartitionId(), proxy_info.first);


  Future::safe_release(proxy->async_ChronosDispatch(cmd, chr_req, fuattr));
}

void ChronosCommo::SendHandoutRo(SimpleCommand &cmd,
                                 const function<void(int res,
                                                     SimpleCommand &cmd,
                                                     map<int,
                                                         mdb::version_t> &vers)> &) {
  verify(0);
}

void ChronosCommo::SendFinish(parid_t pid,
                              txnid_t tid,
                              shared_ptr<RccGraph> graph,
                              const function<void(TxnOutput &output)> &callback) {
  verify(0);
  FutureAttr fuattr;
  function<void(Future *)> cb = [callback](Future *fu) {
    int32_t res;
    TxnOutput outputs;
    fu->get_reply() >> res >> outputs;
    callback(outputs);
  };
  fuattr.callback = cb;
  auto proxy = NearestProxyForPartition(pid).second;
  MarshallDeputy md(graph);
  Future::safe_release(proxy->async_JanusCommit(tid, md, fuattr));
}

void ChronosCommo::SendInquire(parid_t pid,
                               epoch_t epoch,
                               txnid_t tid,
                               const function<void(RccGraph &graph)> &callback) {
  FutureAttr fuattr;
  function<void(Future *)> cb = [callback](Future *fu) {
    MarshallDeputy md;
    fu->get_reply() >> md;
    auto graph = dynamic_cast<RccGraph &>(*md.sp_data_);
    callback(graph);
  };
  fuattr.callback = cb;
  // TODO fix.
  auto proxy = NearestProxyForPartition(pid).second;
  Future::safe_release(proxy->async_JanusInquire(epoch, tid, fuattr));
}

bool ChronosCommo::IsGraphOrphan(RccGraph &graph, txnid_t cmd_id) {
  if (graph.size() == 1) {
    auto v = graph.FindV(cmd_id);
    verify(v);
    return true;
  } else {
    return false;
  }
}

void ChronosCommo::BroadcastPreAccept(
    parid_t par_id,
    txnid_t txn_id,
    ballot_t ballot,
    vector<TxPieceData> &cmds,
    ChronosPreAcceptReq &chr_req,
    const function<void(int, ChronosPreAcceptRes &res)> &callback) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());


  for (auto &p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattrChro;
    fuattrChro.callback = [callback](Future *fu) {
      int32_t res;
      ChronosPreAcceptRes chr_res;
      fu->get_reply() >> res >> chr_res;
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
