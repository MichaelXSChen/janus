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
  Log_info("[[%s]] callled", __PRETTY_FUNCTION__);

  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) {
    Log_info("already in next phase, return. Current phase = %d, suppose to be %d", phase_, phase);
    return;
  }
  if (res == SUCCESS) {
    n_fast_accept_oks_[par_id]++;
    pre_accept_acks_[par_id] = chr_res;
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
  auto pars = txn().GetPartitionIds();
  bool all_fast_quorum_possible = true;
  for (auto &par_id : pars) {
    int r = FastQuorumCheck(par_id);
    return (r == 1);
  }
};

int32_t CoordinatorChronos::GetQuorumSize(parid_t par_id) {
  int32_t n = Config::GetConfig()->GetPartitionSize(par_id);
  return n/2 + 1;
}

bool CoordinatorChronos::AllFastQuorumsReached() {
  auto pars = txn().GetPartitionIds();
  for (auto &par_id : pars) {
    int r = FastQuorumCheck(par_id);
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
  auto pars = txn().GetPartitionIds();
  for (auto &par_id : pars) {
    if (n_fast_accept_oks_[par_id] < GetQuorumSize(par_id)) {
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
                    return n_fast_accept_oks_[par_id] >= GetQuorumSize(par_id);
                  });
  return all_slow_quorum_reached;
};


// return value
// 1: a fast quorum reached
// -1: fast quorum not possible.
// 0: more wait
int CoordinatorChronos::FastQuorumCheck(uint32_t par_id) {
  auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
  //the number of
  Log_info("fast quorum check, par %d, size %d", par_id, par_size);

  int32_t t_left, t_right;

  int ret = GetTsIntersection(par_id, t_left, t_right);

  return ret;
}

//Return value
//1 has intersection
//-1 has no intersection
//0 more wait



void CoordinatorChronos::Dispatch() {


  verify(ro_state_ == BEGIN);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxnCommand *) cmd_;
  verify(txn->root_id_ == txn->id_);
  int cnt = 0;
  map<parid_t, vector<SimpleCommand*>> cmds_by_par = txn->GetReadyCmds();
  Log_info("transaction (id %d) has %d ready pieces", txn->id_, cmds_by_par.size());

  int index = 0;
  for (auto &pair: cmds_by_par) {

    const parid_t &par_id = pair.first;
    Log_info("piece: (id %d, pieces %d) has touch par_id = %d", txn->id_, index++, par_id);
    auto &cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    vector<SimpleCommand> cc;
    for (auto c: cmds) {
      c->id_ = next_pie_id(); //next_piece_id
      dispatch_acks_[c->inn_id_] = false;
      cc.push_back(*c);
    }

    auto callback = std::bind(&CoordinatorChronos::DispatchAck,
                              this,
                              phase_,
                              std::placeholders::_1,
                              std::placeholders::_2,
                              std::placeholders::_3);
    ChronosDispatchReq req;
    ts_left_ = logical_clock++;
    ts_right_ = ts_left_ + ts_delta_;
    req.ts_min = ts_left_;
    req.ts_max = ts_right_;

    commo()->SendDispatch(cc, req, callback);
  }
  Log_info("transaction (id %d)'s n_dispatch = %d", txn->id_, n_dispatch_);
}

//xs: callback for handling Dispatch ACK
//xs: What is the meaning of this function.
void CoordinatorChronos::DispatchAck(phase_t phase,
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
    Log_info("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
             n_dispatch_ack_, n_dispatch_, txn().id_, pair.first);
  }


  if (txn().HasMoreSubCmdReadyNotOut()) {
    Log_info("command has more sub-cmd, cmd_id: %lx,"
             " n_started_: %d, n_pieces: %d",
             txn().id_,
             txn().n_pieces_dispatched_, txn().GetNPieceAll());
    Dispatch();
  } else if (AllDispatchAcked()) {
    //xs: this is for OCC + Paxos based method.

    Log_info("receive all start acks, txn_id: %llx; START PREPARE", cmd_->id_);
    verify(!txn().do_early_return());
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
  //xstodo: think about how to forward the clock
  logical_clock = ++ts_right_;


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
      if (res->ts_left < t_low){
        t_low = res->ts_left;
      }
      if (res->ts_right > t_high){
        t_high= res->ts_right;
      }
    }
  }

  if (t_high >= t_low){
    ts_fast_commit_ = t_low;
    return true;
  }else{
    return false;
  }

}

} // namespace janus
