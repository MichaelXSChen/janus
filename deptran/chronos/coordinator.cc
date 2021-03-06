//
// Created by micha on 2020/3/23.
//

#include "../__dep__.h"
#include "txn_chopper.h"
#include "frame.h"
#include "commo.h"
#include "coordinator.h"

namespace rococo {


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

  Log_info("%s:%s called", __FILE__, __FUNCTION__);

  for (auto par_id : cmd_->GetPartitionIds()) {
    auto cmds = txn().GetCmdsByPartition(par_id);

    Log_info("Calling BroadCastPreAccept %d", par_id);

//    int counter = 0;
//    for (auto &c: cmds) {
//      Log_info("%d-th cmd, partition_id = %d, id = %d, type = %d", counter++, par_id, c.id_, c.type());
//      c.input.print();
//    }

    ChronosPreAcceptReq chr_req;
    chr_req.ts_min = ts_left_;
    chr_req.ts_max = ts_right_;
    commo()->BroadcastPreAccept(par_id,
                                cmd_->id_,
                                magic_ballot(),
                                cmds,
                                chr_req,
                                std::bind(&CoordinatorChronos::PreAcceptAck,
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
                                      std::shared_ptr<ChronosPreAcceptRes> chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  //Log_info("[[%s]] callled", __PRETTY_FUNCTION__);

  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) {
    //Log_info("already in next phase, return. Current phase = %d, suppose to be %d", phase_, phase);
    return;
  }
  if (res == SUCCESS) {
    n_fast_accept_oks_[par_id]++;
    pre_accept_acks_[par_id].push_back(chr_res);
    Log_info("Received pre-accpet ok for par_id: %d", par_id);
  } else if (res == REJECT) {
    Log_info("Reject not handled yet");
    verify(0);
  } else {
    verify(0);
  }


  if(PreAcceptQuroumAck()){
    //Received quorum ACK from all participating partitions.
    if(CheckTsIntersection()){
      fast_path_ = true;
      Log_info("[Txn id = %d] Fast path, commit_ts = %d", this->txn().id_, ts_fast_commit_);
      GotoNextPhase();
    }else{
      Log_info("[Txn id = %d] Slow path", this->txn().id_);
      GotoNextPhase();
    }

  }

}




void CoordinatorChronos::Accept() {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  verify(!fast_path_);
//  Log_info("broadcast accept request for txn_id: %llx", cmd_->id_);
  ChooseGraph();
  TxnCommand *txn = (TxnCommand *) cmd_;
  auto dtxn = graph_.FindV(cmd_->id_);
  verify(txn->partition_ids_.size() == dtxn->partition_.size());
  graph_.UpgradeStatus(dtxn, TXN_CMT);
  ChronosAcceptReq chr_req;
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastAccept(par_id,
                             cmd_->id_,
                             ballot_,
                             chr_req,
                             std::bind(&CoordinatorChronos::AcceptAck,
                                       this,
                                       phase_,
                                       par_id,
                                       std::placeholders::_1,
                                       std::placeholders::_2));
  }
}

void CoordinatorChronos::AcceptAck(phase_t phase,
                                   parid_t par_id,
                                   int res,
                                   ChronosAcceptRes &chr_res) {
  Log_info("[[%s]] Called", __PRETTY_FUNCTION__);

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
  ChronosCommitReq chr_req;
  chr_req.commit_ts = ts_fast_commit_;
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastCommit(par_id,
                             cmd_->id_,
                             chr_req,
                             std::bind(&CoordinatorChronos::CommitAck,
                                       this,
                                       phase_,
                                       par_id,
                                       std::placeholders::_1,
                                       std::placeholders::_2,
                                       std::placeholders::_3));
  }
  if (fast_commit_) {
    committed_ = true;
    GotoNextPhase();
  }
}

void CoordinatorChronos::CommitAck(phase_t phase,
                                   parid_t par_id,
                                   int32_t res,
                                   ChronosCommitRes &chr_res,
                                   TxnOutput &output) {
  Log_info("[[%s]] called", __PRETTY_FUNCTION__);

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
  GotoNextPhase();
  return;
}



bool CoordinatorChronos::AcceptQuorumReached() {
  auto pars = txn().GetPartitionIds();
  for (auto &par_id : pars) {
    if (n_fast_accept_oks_[par_id] < 1) {
      //xstodo: temporarily chenged to 1 for code refinement
      return false;
    }
  }
  return true;
}



void CoordinatorChronos::SubmitReq() {


  verify(ro_state_ == BEGIN);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxnCommand *) cmd_;
  verify(txn->root_id_ == txn->id_);
  int cnt = 0;
  map<parid_t, vector<SimpleCommand*>> cmds_by_par = txn->GetReadyCmds();
  Log_debug("transaction (id %d) has %d ready pieces", txn->id_, cmds_by_par.size());

  bool is_local = local_txn_;
  if (is_local){
    verify(cmds_by_par.size() == 1);
  }

  //int index = 0;
  for (auto &pair: cmds_by_par) {

    const parid_t &par_id = pair.first;
    //Log_info("piece: (id %d, pieces %d) has touch par_id = %d", txn->id_, index++, par_id);
    auto &cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    vector<SimpleCommand> cc;
    for (auto c: cmds) {
      c->id_ = next_pie_id(); //next_piece_id
      dispatch_acks_[c->inn_id_] = false;
      cc.push_back(*c);
    }

    auto callback = std::bind(&CoordinatorChronos::SubmitAck,
                              this,
                              phase_,
                              std::placeholders::_1,
                              std::placeholders::_2,
                              std::placeholders::_3);
    ChronosDispatchReq req;
    req.is_local = (int) is_local;
    ts_left_ = logical_clock++;
    ts_right_ = ts_left_ + ts_delta_;
    req.ts_min = ts_left_;
    req.ts_max = ts_right_;

    commo()->SubmitReq(cc, req, is_local, callback);
  }
}

void CoordinatorChronos::SubmitAck(phase_t phase,
                                     int res,
                                     TxnOutput &output,
                                     ChronosDispatchRes &chr_res) {

  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_); // cannot proceed without all acks.
  verify(txn().root_id_ == txn().id_);


  if (chr_res.ts_left > ts_left_){
    ts_left_ = chr_res.ts_left;
    ts_right_ = ts_left_ + ts_delta_;

    Log_info("DispatchAck, ts_left change to %d", ts_left_);
  }

  for (auto &pair : output) {
    n_dispatch_ack_++;
    verify(dispatch_acks_[pair.first] == false);
    dispatch_acks_[pair.first] = true;
    txn().Merge(pair.first, pair.second); //For those txn that need the read value for other command.
    Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
             n_dispatch_ack_, n_dispatch_, txn().id_, pair.first);
  }


  if (txn().HasMoreSubCmdReadyNotOut()) {
    Log_info("command has more sub-cmd, cmd_id: %lx,"
             " n_started_: %d, n_pieces: %d",
             txn().id_,
             txn().n_pieces_dispatched_, txn().GetNPieceAll());
    SubmitReq();
  } else if (AllDispatchAcked()) {
    //xs: this is for OCC + Paxos based method.

    verify(!txn().do_early_return());
    Log_info("receive output for txn id = %lu", txn().id_);
    committed_ = true;
    GotoNextPhase();
  }
}

void CoordinatorChronos::GotoNextPhase() {

  int n_phase = 2;
  int current_phase = phase_++ % n_phase; // for debug

  switch (current_phase) {
    case Phase::CHR_INIT:
      /*
       * Collect the local-DC timestamp.
       * Try to make my clock as up-to-date as possible.
       */
      SubmitReq();
      verify(phase_ % n_phase == Phase::CHR_COMMIT);
      break;

    case Phase::CHR_COMMIT: //4

      verify(phase_ % n_phase == Phase::CHR_INIT); //overflow
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

}

void CoordinatorChronos::Reset() {
  RccCoord::Reset();
  fast_path_ = false;
  fast_commit_ = false;
  n_fast_accept_graphs_.clear();
  n_fast_accept_oks_.clear();
  n_accept_oks_.clear();
  fast_accept_graph_check_caches_.clear();
  n_commit_oks_.clear();
  //xstodo: think about how to forward the clock
  logical_clock = ++ts_right_;
  pre_accept_acks_.clear();
}


bool CoordinatorChronos::PreAcceptQuroumAck() {
  auto pars = txn().GetPartitionIds();
  for (auto &par_id : pars) {
    auto n_replicas = Config::GetConfig()->GetPartitionSize(par_id);
    if (n_fast_accept_oks_[par_id] < n_replicas/2 + 1){
      return false;
    }
  }
  return true;
}


bool CoordinatorChronos::CheckTsIntersection() {
  int64_t t_low = ts_left_;
  int64_t t_high = ts_right_;

  auto pars = txn().GetPartitionIds();

  for (auto &par_id: pars){
    for (auto &res: pre_accept_acks_[par_id]){
        Log_info("res, t_left = %d, t_right = %d", res->ts_left, res->ts_right);
      if (res->ts_left > t_low){
        t_low = res->ts_left;
      }
      if (res->ts_right < t_high){
        t_high= res->ts_right;
      }
    }
  }

  Log_info("t_high = %d, t_low = %d", t_high, t_low);
  if (t_high >= t_low){
    ts_fast_commit_ = t_low;
    return true;
  }else{
    return false;
  }

}

} // namespace janus
