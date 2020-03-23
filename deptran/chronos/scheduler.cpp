//
// Created by micha on 2020/3/23.
//


#include "scheduler.h"
#include "commo.h"
#include "deptran/chronos/tx.h"
#include <climits>
#include "deptran/frame.h"

using namespace janus;
class Frame;

int SchedulerChronos::OnDispatch(const vector<SimpleCommand>& cmd,
                                 const ChronosDispatchReq &chr_req,
                                 int32_t* res,
                                 ChronosDispatchRes *chr_res,
                                 TxnOutput* output) {


  std::lock_guard<std::recursive_mutex> guard(mtx_);
  txnid_t txn_id = cmd[0].root_id_; //should have the same root_id
  auto dtxn = dynamic_pointer_cast<TxChronos>(GetOrCreateTx(txn_id)); //type is shared_pointer
  verify(dtxn->id() == txn_id);
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  dtxn->received_prepared_ts_left_ = chr_req.ts_min;
  dtxn->received_prepared_ts_right_ = chr_req.ts_max;



  Log_info("[Scheduler %d] On Dispatch, txn_id = %d, ts_range = [%d, %d]", this->frame_->site_info_->id, txn_id, chr_req.ts_min, chr_req.ts_max);

  for (auto& c : cmd) {
    dtxn->DispatchExecute(const_cast<SimpleCommand&>(c),
                          res, &(*output)[c.inn_id()]);
  }


  dtxn->UpdateStatus(TXN_STD); //started
  verify(cmd[0].root_id_ == txn_id);


  int64_t reply_ts_left, reply_ts_right;
  dtxn->GetTsBound(reply_ts_left, reply_ts_right);

  chr_res->ts_left = reply_ts_left;
  chr_res->ts_right = reply_ts_right;

  Log_info("[Scheduler %d] DispatchReturn, txn_id = %d, ts_left = %d, ts_right = %d", this->frame_->site_info_->id, txn_id, chr_res->ts_left, chr_res->ts_right);
  return 0;
}



void SchedulerChronos::OnPreAccept(const txid_t txn_id,
                                   const vector<SimpleCommand> &cmds,
                                   const ChronosPreAcceptReq &chr_req,
                                   int32_t *res,
                                   ChronosPreAcceptRes *chr_res) {


  std::lock_guard<std::recursive_mutex> lock(mtx_);
  /*
   * xsTODO: Steps:
    */


  //if (RandomGenerator::rand(1, 2000) <= 1)
  //Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  auto dtxn = dynamic_pointer_cast<TxChronos>(GetOrCreateTx(txn_id));

  dtxn->received_prepared_ts_left_ = chr_req.ts_min;
  dtxn->received_prepared_ts_right_ = chr_req.ts_max;

  dtxn->UpdateStatus(TXN_PAC);

  Log_info("[Scheduler %d] pre-accept on txn_id = %d, phase = %d, ts_range [%d, %d]", this->frame_->site_info_->id, txn_id, dtxn->phase_, chr_req.ts_min, chr_req.ts_max);


  dtxn->involve_flag_ = TxRococo::INVOLVED;
  TxChronos &tinfo = *dtxn;
  if (dtxn->max_seen_ballot_ > 0) {
    *res = REJECT;
  } else {
    //Normal case
    if (dtxn->status() < TXN_CMT) {
      if (dtxn->phase_ < PHASE_CHRONOS_DISPATCH && tinfo.status() < TXN_CMT) {
        for (auto &c: cmds) {
          map<int32_t, Value> output;
          dtxn->DispatchExecute(const_cast<SimpleCommand &>(c), res, &output);
        }
      }
    } else {
      if (dtxn->dreqs_.size() == 0) {
        for (auto &c: cmds) {
          dtxn->dreqs_.push_back(c);
        }
      }
    }
    verify(!tinfo.fully_dispatched);


    int64_t reply_ts_left, reply_ts_right;
    dtxn->GetTsBound(reply_ts_left, reply_ts_right);

    tinfo.fully_dispatched = true;
    if (tinfo.status() >= TXN_CMT) {
      waitlist_.insert(dtxn.get());  //xs: for failure recovery
      verify(dtxn->epoch_ > 0);
    }

    chr_res->ts_left = reply_ts_left;
    chr_res->ts_right = reply_ts_right;



    if (reply_ts_left <= reply_ts_right){
      *res = SUCCESS;
    }else{
      *res =  REJECT;
    }

    Log_info("[Scheduler %d] Preaccept returns, txn_id = %d, res = %d, ts_left = %d, ts_right = %d", this->frame_->site_info_->id, txn_id, *res, reply_ts_left, reply_ts_right);
  }
}

void SchedulerChronos::OnAccept(const txnid_t txn_id,
                                const ballot_t &ballot,
                                const ChronosAcceptReq &chr_req,
                                int32_t *res,
                                ChronosAcceptRes *chr_res) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(txn_id));
  if (dtxn->max_seen_ballot_ > ballot) {
    *res = REJECT;
    verify(0); // do not support failure recovery so far.
  } else {
    dtxn->max_accepted_ballot_ = ballot;
    *res = SUCCESS;
  }
}


void SchedulerChronos::OnCommit(const txnid_t cmd_id,
                                const ChronosCommitReq &chr_req,
                                int32_t *res,
                                TxnOutput *output,
                                ChronosCommitRes *chr_res,
                                const function<void()> &callback) {
  // TODO to support cascade abort
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *res = SUCCESS;
  //union the graph into dep graph
  auto dtxn = dynamic_pointer_cast<TxChronos>(GetOrCreateTx(cmd_id));
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;
  dtxn->commit_ts_ = chr_req.commit_ts;


  if (dtxn->IsExecuted()) {
    *res = SUCCESS;
    callback();
  } else if (dtxn->IsAborted()) {
    verify(0);
    *res = REJECT;
    callback();
  } else {
    //Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
    dtxn->commit_request_received_ = true;
    dtxn->finish_reply_callback_ = [callback, res](int r) {
      *res = r;
      callback();
    };
    verify(dtxn->fully_dispatched); //cannot handle non-dispatched now.
    UpgradeStatus(*dtxn, TXN_DCD);
    Execute(*dtxn);
    if (dtxn->to_checks_.size() > 0) {
      for (auto child : dtxn->to_checks_) {
        waitlist_.insert(child);
      }
      CheckWaitlist();
    }
  }
}


int SchedulerChronos::OnInquire(epoch_t epoch,
                                cmdid_t cmd_id,
                                shared_ptr<RccGraph> graph,
                                const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO check epoch, cannot be a too old one.
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(cmd_id));
  TxRococo &info = *dtxn;
  //register an event, triggered when the status >= COMMITTING;
  verify (info.Involve(Scheduler::partition_id_));

  auto cb_wrapper = [callback, graph]() {
    callback();
  };

  if (info.status() >= TXN_CMT) {
    InquiredGraph(info, graph);
    cb_wrapper();
  } else {
    info.graphs_for_inquire_.push_back(graph);
    info.callbacks_for_inquire_.push_back(cb_wrapper);
    verify(info.graphs_for_inquire_.size() ==
        info.callbacks_for_inquire_.size());
    waitlist_.insert(dtxn.get());
    verify(dtxn->epoch_ > 0);
  }
  return 0;

}




ChronosCommo *SchedulerChronos::commo() {

  auto commo = dynamic_cast<ChronosCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}


