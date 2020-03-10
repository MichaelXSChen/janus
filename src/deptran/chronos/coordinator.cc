
#include "../__dep__.h"
#include "deptran/procedure.h"
#include "frame.h"
#include "commo.h"
#include "coordinator.h"

namespace janus {
//CoordinatorJanus::CoordinatorJanus(
//  uint32_t                  coo_id,
//  uint32_t                  thread_id,
//  bool                      batch_optimal) : coo_id_(coo_id),
//                                             thread_id_(thread_id),
//                                             batch_optimal_(batch_optimal) {
//  rpc_poll_ = new PollMgr(1);
//  recorder_ = nullptr;
//  if (Config::GetConfig()->do_logging()) {
//    std::string log_path(Config::GetConfig()->log_path());
//    log_path.append(std::to_string(coo_id_));
//    recorder_ = new Recorder(log_path.c_str());
//    rpc_poll_->add(recorder_);
//  }
//}
//
//void CoordinatorJanus::launch(Command &cmd) {
//  cmd_ = cmd;
//  cmd_id_ = next_cmd_id();
//  this->FastAccept();
//}


ChronosCommo *CoordinatorChronos::commo() {
  if (commo_ == nullptr) {
    commo_ = frame_->CreateCommo();
    commo_->loc_id_ = loc_id_;
  }
  verify(commo_ != nullptr);
  return dynamic_cast<ChronosCommo *>(commo_);
}

void CoordinatorChronos::launch_recovery(cmdid_t cmd_id) {
  // TODO
  prepare();
}

void CoordinatorChronos::PreAccept() {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
//  // generate fast accept request
  // set broadcast callback
  // broadcast
  auto dtxn = sp_graph_->FindV(cmd_->id_);

  Log_info("%s:%s called", __FILE__, __FUNCTION__);

  verify(tx_data().partition_ids_.size() == dtxn->partition_.size());
  for (auto par_id : cmd_->GetPartitionIds()) {
    auto cmds = tx_data().GetCmdsByPartition(par_id);

    Log_info("Calling BroadCastPreAccept %d", par_id);

    int counter = 0;
    for (auto &c: cmds) {
      Log_info("%d-th cmd, partition_id = %d, id = %d, type = %d", counter++, par_id, c.id_, c.type());
      c.input.print();
    }
    commo()->BroadcastPreAccept(par_id,
                                cmd_->id_,
                                magic_ballot(),
                                cmds,
                                sp_graph_,
                                std::bind(&CoordinatorJanus::PreAcceptAck,
                                          this,
                                          phase_,
                                          par_id,
                                          std::placeholders::_1,
                                          std::placeholders::_2));
  }
}

void CoordinatorChronos::PreAcceptAck(phase_t phase,
                                      parid_t par_id,
                                      int res,
                                      shared_ptr<RccGraph> graph) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) return;
  verify(graph != nullptr);
//  verify(graph->FindV(txn().root_id_) != nullptr);
//  verify(n_fast_accept_graphs_.size() == 0);
  n_fast_accept_graphs_[par_id].push_back(graph);
  if (res == SUCCESS) {
    n_fast_accept_oks_[par_id]++;
  } else if (res == REJECT) {
    verify(0);
//    n_fast_accept_rejects_[par_id]++;
  } else {
    verify(0);
  }
  if (FastpathPossible()) {
    // there is still chance for fastpath
    if (AllFastQuorumsReached()) {
      // receive enough identical replies to continue fast path.
      // go to the commit.
      fast_path_ = true;
//      Log_info("pre acked success on txn_id: %llx", cmd_->id_);
      ChooseGraph();
      GotoNextPhase();
    } else {
      // skip, wait
    }
  } else {
    // fastpath is no longer a choice
    if (SlowpathPossible()) {
      if (PreAcceptAllSlowQuorumsReached()) {
        verify(!fast_path_);
        GotoNextPhase();
      } else {
        // skip, wait
      }
    } else {
      // slowpath is not a choice either.
      // not handle for now.
      verify(0);
    }
  }
}

/** caller should be thread_safe */
void CoordinatorChronos::prepare() {
  // TODO
  // do not do failure recovery for now.
  verify(0);
}

void CoordinatorChronos::ChooseGraph() {
  for (auto &pair : n_fast_accept_graphs_) {
    auto &vec_graph = pair.second;
    if (fast_path_) {
      auto &g = vec_graph[0];
      sp_graph_->Aggregate(0, *g);
    } else {
      for (auto g : vec_graph) {
        sp_graph_->Aggregate(0, *g);
      }
    }
  }
}

void CoordinatorChronos::Accept() {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  verify(!fast_path_);
//  Log_info("broadcast accept request for txn_id: %llx", cmd_->id_);
  ChooseGraph();
  TxData *txn = (TxData *) cmd_;
  auto dtxn = sp_graph_->FindV(cmd_->id_);
  verify(txn->partition_ids_.size() == dtxn->partition_.size());
  sp_graph_->UpgradeStatus(*dtxn, TXN_CMT);
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastAccept(par_id,
                             cmd_->id_,
                             ballot_,
                             sp_graph_,
                             std::bind(&CoordinatorJanus::AcceptAck,
                                       this,
                                       phase_,
                                       par_id,
                                       std::placeholders::_1));
  }
}

void CoordinatorChronos::AcceptAck(phase_t phase,
                                   parid_t par_id,
                                   int res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  if (phase_ != phase) {
    return;
  }
  verify(res == SUCCESS);
  n_accept_oks_[par_id]++;

  if (AcceptQuorumPossible()) {
    if (AcceptQuorumReached()) {
      GotoNextPhase();
    } else {
      // skip;
    }
  } else {
    // not handle this currently
    verify(0);
  }
  // if have reached a quorum
}

void CoordinatorChronos::Commit() {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  TxData *txn = (TxData *) cmd_;
  auto dtxn = sp_graph_->FindV(cmd_->id_);
  verify(txn->partition_ids_.size() == dtxn->partition_.size());
  sp_graph_->UpgradeStatus(*dtxn, TXN_CMT);
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastCommit(par_id,
                             cmd_->id_,
                             sp_graph_,
                             std::bind(&CoordinatorJanus::CommitAck,
                                       this,
                                       phase_,
                                       par_id,
                                       std::placeholders::_1,
                                       std::placeholders::_2));
  }
  if (fast_commit_) {
    committed_ = true;
    GotoNextPhase();
  }
}

void CoordinatorChronos::CommitAck(phase_t phase,
                                   parid_t par_id,
                                   int32_t res,
                                   TxnOutput &output) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  if (phase != phase_) return;
  if (fast_commit_) return;
  if (res == SUCCESS) {
    committed_ = true;
  } else if (res == REJECT) {
    aborted_ = true;
  } else {
    verify(0);
  }
  n_commit_oks_[par_id]++;
  if (n_commit_oks_[par_id] > 1)
    return;

//  txn().Merge(output);
  // if collect enough results.
  // if there are still more results to collect.
  GotoNextPhase();
//  bool all_acked = txn().OutputReady();
//  if (all_acked)
//  GotoNextPhase();
  return;
}

bool CoordinatorChronos::FastpathPossible() {
  auto pars = tx_data().GetPartitionIds();
  bool all_fast_quorum_possible = true;
  for (auto &par_id : pars) {
    auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
//    if (n_fast_accept_rejects_[par_id] > par_size - GetFastQuorum(par_id)) {
//      all_fast_quorum_possible = false;
//      verify(0);
//      break;
//    }
    // TODO check graph.
    // if more than (par_size - fast quorum) graph is different, then nack.
    int r = FastQuorumGraphCheck(par_id);
    if (r == 1 || r == 3) {

    } else if (r == 2) {
      all_fast_quorum_possible = false;
    }
  }
  return all_fast_quorum_possible;
};

int32_t CoordinatorChronos::GetFastQuorum(parid_t par_id) {
  int32_t n = Config::GetConfig()->GetPartitionSize(par_id);
  return n;
}

int32_t CoordinatorChronos::GetSlowQuorum(parid_t par_id) {
  int32_t n = Config::GetConfig()->GetPartitionSize(par_id);
  return n / 2 + 1;
}

bool CoordinatorChronos::AllFastQuorumsReached() {
  auto pars = tx_data().GetPartitionIds();
  for (auto &par_id : pars) {
    int r = FastQuorumGraphCheck(par_id);
    if (r == 2) {
      return false;
    } else if (r == 1) {
      // do nothing
    } else if (r == 3) {
      // do nothing
      return false;
    } else {
      verify(0);
    }
  }
  return true;
}

bool CoordinatorChronos::AcceptQuorumReached() {
  auto pars = tx_data().GetPartitionIds();
  for (auto &par_id : pars) {
    if (n_fast_accept_oks_[par_id] < GetSlowQuorum(par_id)) {
      return false;
    }
  }
  return true;
}

bool CoordinatorChronos::PreAcceptAllSlowQuorumsReached() {
  auto pars = cmd_->GetPartitionIds();
  bool all_slow_quorum_reached =
      std::all_of(pars.begin(),
                  pars.end(),
                  [this](parid_t par_id) {
                    return n_fast_accept_oks_[par_id] >= GetSlowQuorum(par_id);
                  });
  return all_slow_quorum_reached;
};

// return value
// 1: a fast quorum of the same
// 2: >=(par_size - fast quorum) of different graphs. fast quorum not possible.
// 3: less than a fast quorum graphs received.
int CoordinatorChronos::FastQuorumGraphCheck(parid_t par_id) {
  auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
  auto &vec_graph = n_fast_accept_graphs_[par_id];
  auto fast_quorum = GetFastQuorum(par_id);
  if (vec_graph.size() < fast_quorum)
    return 3;
  int res = fast_accept_graph_check_caches_[par_id];
  if (res > 0) return res;

  res = 1;
//  verify(vec_graph.size() == 1);
//  verify(vec_graph.size() >= 1);
  verify(vec_graph.size() == fast_quorum);
  auto v = vec_graph[0]->FindV(cmd_->id_);
  verify(v != nullptr);
  auto &parent_set = v->GetParentSet();
  for (int i = 1; i < vec_graph.size(); i++) {
    RccGraph &graph = *vec_graph[i];
    auto vv = graph.FindV(cmd_->id_);
    auto &pp_set = vv->GetParentSet();
    if (parent_set != pp_set) {
      res = 2;
      break;
    }
  }
  fast_accept_graph_check_caches_[par_id] = res;
  return res;
}

void CoordinatorChronos::Dispatch() {
  verify(ro_state_ == BEGIN);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxData *) cmd_;
  verify(txn->root_id_ == txn->id_);
  int cnt = 0;
  auto cmds_by_par = txn->GetReadyPiecesData();
  Log_info("transaction (id %d) has been divided into %d pieces", txn->id_, cmds_by_par.size());

  int index = 0;
  for (auto &pair: cmds_by_par) {

    const parid_t &par_id = pair.first;
    Log_info("piece: (id %d, pieces %d) has touch par_id = %d", txn->id_, index++, par_id);
    auto &cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    vector<SimpleCommand> cc;
    for (auto c: cmds) {
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      cc.push_back(*c);
    }

    auto callback = std::bind(&CoordinatorChronos::ChrDispatchAck,
                              this,
                              phase_,
                              std::placeholders::_1,
                              std::placeholders::_2,
                              std::placeholders::_3,
                              std::placeholders::_4);
    Log_info("DispatchACK callback is %x", &callback);
    commo()->SendDispatch(cc, callback);
  }
  Log_info("transaction (id %d)'s n_dispatch = %d", txn->id_, n_dispatch_);
}


//xs: callback for handling Dispatch ACK
//xs: What is the meaning of this function.
void CoordinatorChronos::ChrDispatchAck(phase_t phase,
                           int res,
                           TxnOutput &output,
                           ChronosDispatchRes &chr_res,
                           RccGraph &graph) {
  Log_info("called 1");


  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_); // cannot proceed without all acks.
  verify(tx_data().root_id_ == tx_data().id_);
  verify(graph.vertex_index().size() > 0);

  Log_info("[[%s]] called 2, timestamp = %d", __PRETTY_FUNCTION__, chr_res.max_ts);

  TxRococo &info = *(graph.vertex_index().at(tx_data().root_id_));
//  verify(cmd[0].root_id_ == info.id());
//  verify(info.partition_.find(cmd.partition_id_) != info.partition_.end());

  for (auto &pair : output) {
    n_dispatch_ack_++;
    verify(dispatch_acks_[pair.first] == false);
    dispatch_acks_[pair.first] = true;
    tx_data().Merge(pair.first, pair.second);
    Log_info("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
             n_dispatch_ack_, n_dispatch_, tx_data().id_, pair.first);
  }

  // where should I store this graph?
  Log_info("start response graph size: %d", (int) graph.size());
  verify(graph.size() > 0);

  sp_graph_->Aggregate(0, graph);

  // TODO?
  if (graph.size() > 1) tx_data().disable_early_return();

  if (tx_data().HasMoreUnsentPiece()) {
    Log_info("command has more sub-cmd, cmd_id: %lx,"
             " n_started_: %d, n_pieces: %d",
             tx_data().id_,
             tx_data().n_pieces_dispatched_, tx_data().GetNPieceAll());
    DispatchAsync();
  } else if (AllDispatchAcked()) {
    Log_info("receive all start acks, txn_id: %llx; START PREPARE", cmd_->id_);
    verify(!tx_data().do_early_return());
    GotoNextPhase();
  }
}


void CoordinatorChronos::GotoNextPhase() {

  int n_phase = 5;
  int current_phase = phase_++ % n_phase; // for debug
  Log_info("--------------------------------------------------");
  Log_info("%s: phase = %d", __FUNCTION__, current_phase);

  switch (current_phase) {
    case Phase::CHR_INIT:
      /*
       * Collect the local-DC timestamp.
       * Try to make my clock as up-to-date as possible.
       */
      Dispatch();
      verify(phase_ % n_phase == Phase::CHR_DISPATCH);
      break;
    case Phase::CHR_DISPATCH: //1
      /*
       * Contact all participant replicas
       * Can enter fast if majority replica replies OK.
       */
//      phase_++;
      verify(phase_ % n_phase == Phase::CHR_FAST);
      PreAccept();
      break;

    case Phase::CHR_FAST: //2

      if (fast_path_) {
        phase_++;
        Log_info("here, phase_ = %d", phase_ % n_phase);
        verify(phase_ % n_phase == Phase::CHR_COMMIT); //4
        Commit();
      } else {
        /*
         * Fallback to ocean vista.
         */
        verify(phase_ % n_phase == Phase::CHR_FALLBACK); //3
        Accept();
      }
      // TODO
      break;

    case Phase::CHR_FALLBACK: //3

      verify(phase_ % n_phase == Phase::CHR_COMMIT);
      Commit();
      break;

    case Phase::CHR_COMMIT: //4

      verify(phase_ % n_phase == Phase::CHR_INIT); //overflow
      verify(committed_ != aborted_);
      if (committed_) {
        End();
      } else if (aborted_) {
        Restart();
      } else {
        verify(0);
      }
      break;

    default:verify(0);
  }
  Log_info("GotoNextPhase Returned %d --------------------------------------------------", current_phase);

}

void CoordinatorChronos::Reset() {
  RccCoord::Reset();
  fast_path_ = false;
  fast_commit_ = false;
  n_fast_accept_graphs_.clear();
  n_fast_accept_oks_.clear();
  n_accept_oks_.clear();
//  n_fast_accept_rejects_.clear();
  fast_accept_graph_check_caches_.clear();
  n_commit_oks_.clear();
}
} // namespace janus
