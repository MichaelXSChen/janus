//
// Created by micha on 2020/3/23.
//


#include "scheduler.h"
#include "commo.h"
#include "deptran/chronos/tx.h"
#include <climits>
#include "deptran/frame.h"
#include <limits>

using namespace rococo;
class Frame;

int SchedulerChronos::OnSubmit(const vector<SimpleCommand>& cmd,
                                 const ChronosDispatchReq &chr_req,
                                 int32_t* res,
                                 ChronosDispatchRes *chr_res,
                                 TxnOutput* output,
                                 const function<void()> &reply_callback) {
  //Pre-execute the tranasction.
  //Provide the local timestamp as a basic.

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  txnid_t txn_id = cmd[0].root_id_; //should have the same root_id

  verify(local_pending_txns_.count(txn_id) == 0);
  local_pending_txns_.insert(txn_id);
  auto dtxn = (TxChronos* )(GetOrCreateDTxn(txn_id));


  auto execute_callback = [=](){
    *res = SUCCESS;
    for (auto& c : cmd) {
      dtxn->DispatchExecute(const_cast<SimpleCommand&>(c),
                            res, &(*output)[c.inn_id()]);
    }
    dtxn->received_dispatch_ts_left_ = chr_req.ts_min;
    dtxn->received_dispatch_ts_right_ = chr_req.ts_max;


    Log_info("[Scheduler %hu] On Dispatch, txn_id = %lu, is_local = %d, ts_range = [%d, %d]", this->frame_->site_info_->id, txn_id, chr_req.is_local, chr_req.ts_min, chr_req.ts_max);


    dtxn->UpdateStatus(TXN_STD); //started



    int64_t reply_ts_left = 0;
    int64_t reply_ts_right = std::numeric_limits<long long>::max();
    dtxn->GetDispatchTsHint(reply_ts_left, reply_ts_right);

    chr_res->ts_left = reply_ts_left;
    chr_res->ts_right = reply_ts_right;


    Log_debug("[Scheduler %d] DispatchReturn, txn_id = %d, ts_left = %d, ts_right = %d", this->frame_->site_info_->id, txn_id, chr_res->ts_left, chr_res->ts_right);
    reply_callback();
  };




  auto ack_callback = std::bind(&SchedulerChronos::StoreLocalAck,
                            this,
                            txn_id,
                            execute_callback,
                            std::placeholders::_1,
                            std::placeholders::_2,
                            std::placeholders::_3);

  ChronosStoreLocalReq req;

  verify(local_txn_store_oks.count(txn_id) == 0);

  local_txn_store_oks[txn_id] = 0;

  verify(dtxn->id() == txn_id);
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  verify(cmd[0].root_id_ == txn_id);

  commo()->SendStoreLocal(cmd, req, ack_callback);

  return 0;
}

void SchedulerChronos::StoreLocalAck(txnid_t txn_id,
                                     const function<void()> &execute_callback,
                                     int total_count,
                                     int received_res,
                                     ChronosStoreLocalRes &chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called, txnid= %lu, count = %d, total = %d", __FUNCTION__ , txn_id, local_txn_store_oks[txn_id], total_count);
  if (received_res == SUCCESS){
    local_txn_store_oks[txn_id]++;
  }
  else{
    verify(0);
  }

  Log_info("%s called, txnid= %lu, count = %d, total = %d", __FUNCTION__ , txn_id, local_txn_store_oks[txn_id], total_count);
  if (local_txn_store_oks[txn_id] == total_count){
    execute_callback();
  }
}


void SchedulerChronos::OnPreAccept(const txnid_t txn_id,
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
  auto dtxn = (TxChronos*)(GetOrCreateDTxn(txn_id));

  dtxn->received_prepared_ts_left_ = chr_req.ts_min;
  dtxn->received_prepared_ts_right_ = chr_req.ts_max;

  dtxn->UpdateStatus(TXN_PAC);

  Log_info("[Scheduler %d] pre-accept on txn_id = %d, phase = %d, ts_range [%d, %d]", this->frame_->site_info_->id, txn_id, dtxn->phase_, chr_req.ts_min, chr_req.ts_max);


  dtxn->involve_flag_ = RccDTxn::INVOLVED;
  TxChronos &tinfo = *dtxn;
  if (dtxn->max_seen_ballot_ > 0) {
    //xs: for recovery
    *res = REJECT;
  } else {
    //Normal case
    if (dtxn->status() < TXN_CMT) {
      if (dtxn->phase_ < PHASE_CHRONOS_PRE_ACCEPT && tinfo.status() < TXN_CMT) {
        for (auto &c: cmds) {
          map<int32_t, Value> output;
          //this will lock the rows
          dtxn->PreAcceptExecute(const_cast<SimpleCommand &>(c), res, &output);
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


    dtxn->GetTsBound();




    tinfo.fully_dispatched = true;
    if (tinfo.status() >= TXN_CMT) {
      waitlist_.insert(dtxn);  //xs: for failure recovery
      verify(dtxn->epoch_ > 0);
    }


    chr_res->ts_left = dtxn->local_prepared_ts_left_;
    chr_res->ts_right = dtxn->local_prepared_ts_right_;



    if (chr_res->ts_left <= chr_res->ts_right){
     //save the prepared version to database
     //unlock
      dtxn->StorePreparedVers();
      *res = SUCCESS;
      Log_info("[Scheduler %d] Preaccept success, txn_id = %d, res = %d, ts_left = %d, ts_right = %d", this->frame_->site_info_->id, txn_id, *res, chr_res->ts_left, chr_res->ts_right);
    }else{
      *res =  REJECT;
      Log_info("[Scheduler %d] Preaccept reject, txn_id = %d, res = %d, ts_left = %d, ts_right = %d", this->frame_->site_info_->id, txn_id, *res, chr_res->ts_left, chr_res->ts_right);
    }
  }
}

void SchedulerChronos::OnAccept(const txnid_t txn_id,
                                const ballot_t &ballot,
                                const ChronosAcceptReq &chr_req,
                                int32_t *res,
                                ChronosAcceptRes *chr_res) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = (TxChronos*)(GetOrCreateDTxn(txn_id));
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
  auto dtxn = (TxChronos *)(GetOrCreateDTxn(cmd_id));
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;
  dtxn->commit_ts_ = chr_req.commit_ts;

  if (dtxn->IsExecuted()) {
    *res = SUCCESS;
    Log_info("%s, Is executed", __FUNCTION__);
    callback();
  } else if (dtxn->IsAborted()) {
    verify(0); //this should not hanppen
    *res = REJECT;
    callback();
  } else {
    Log_info("%s, will execute", __FUNCTION__);
    //Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
    dtxn->commit_request_received_ = true;
    dtxn->finish_reply_callback_ = [callback, res](int r) {
      *res = r;
      callback();
    };
    verify(dtxn->fully_dispatched); //cannot handle non-dispatched now.
    UpgradeStatus(dtxn, TXN_DCD);
//    Execute(*dtxn);
    dtxn->CommitExecute();
    dtxn->RemovePreparedVers();
    if (dtxn->to_checks_.size() > 0) {
      for (auto child : dtxn->to_checks_) {
        waitlist_.insert(child);
      }
      CheckWaitlist();
    }
    dtxn->ReplyFinishOk();
  }
  Log_info("%s returned", __FUNCTION__);
}


int SchedulerChronos::OnInquire(epoch_t epoch,
                                cmdid_t cmd_id,
                                RccGraph* graph,
                                const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO check epoch, cannot be a too old one.
  auto dtxn = (TxChronos *)(GetOrCreateDTxn(cmd_id));
  RccDTxn &info = *dtxn;
//  TxRococo &info = *dtxn;
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
    waitlist_.insert(dtxn);
    verify(dtxn->epoch_ > 0);
  }
  return 0;

}




ChronosCommo *SchedulerChronos::commo() {

  auto commo = dynamic_cast<ChronosCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}


