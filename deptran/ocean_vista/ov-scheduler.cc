//
// Created by micha on 2020/3/23.
//


#include "ov-scheduler.h"
#include "ov-commo.h"
#include "ov-tx.h"
#include <climits>
#include "deptran/frame.h"
#include <limits>

using namespace rococo;
class Frame;

int SchedulerOV::OnDispatch(const vector<SimpleCommand>& cmd,
                                 const ChronosDispatchReq &chr_req,
                                 int32_t* res,
                                 ChronosDispatchRes *chr_res,
                                 TxnOutput* output) {
  //Pre-execute the tranasction.
  //Provide the local timestamp as a basic.

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  txnid_t txn_id = cmd[0].root_id_; //should have the same root_id
  auto dtxn = (TxOV* )(GetOrCreateDTxn(txn_id)); //type is shared_pointer
  verify(dtxn->id() == txn_id);
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  dtxn->received_dispatch_ts_left_ = chr_req.ts_min;
  dtxn->received_dispatch_ts_right_ = chr_req.ts_max;


  Log_info("[Scheduler %d] On Dispatch, txn_id = %d, ts_range = [%d, %d]", this->frame_->site_info_->id, txn_id, chr_req.ts_min, chr_req.ts_max);

  for (auto& c : cmd) {
    dtxn->DispatchExecute(const_cast<SimpleCommand&>(c),
                          res, &(*output)[c.inn_id()]);
  }


  dtxn->UpdateStatus(TXN_STD); //started
  verify(cmd[0].root_id_ == txn_id);


  int64_t reply_ts_left = 0;
  int64_t reply_ts_right = std::numeric_limits<long long>::max();
  dtxn->GetDispatchTsHint(reply_ts_left, reply_ts_right);

  chr_res->ts_left = reply_ts_left;
  chr_res->ts_right = reply_ts_right;


  Log_info("[Scheduler %d] DispatchReturn, txn_id = %d, ts_left = %d, ts_right = %d", this->frame_->site_info_->id, txn_id, chr_res->ts_left, chr_res->ts_right);
  return 0;
}



void SchedulerOV::OnStore(const txnid_t txn_id,
                                   const vector<SimpleCommand> &cmds,
                                   const OVStoreReq &ov_req,
                                   int32_t *res,
                                   OVStoreRes *ov_res) {


  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_info("%s called", __FUNCTION__);
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  auto dtxn = (TxOV*)(GetOrCreateDTxn(txn_id));

  dtxn->UpdateStatus(TXN_PAC);

  dtxn->involve_flag_ = RccDTxn::INVOLVED;
  TxOV &tinfo = *dtxn;

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

  tinfo.fully_dispatched = true;

  dtxn->ovts_.timestamp_ = ov_req.ts;
  dtxn->ovts_.site_id_ = ov_req.site_id;

  TxOV* txovptr = dynamic_cast<TxOV*>(dtxn);

  txovptr->ov_status_ = TxOV::OV_txn_status::STORED;

  stored_txns_by_id_[txn_id] = txovptr;

  *res = SUCCESS;
}

void SchedulerOV::OnAccept(const txnid_t txn_id,
                                const ballot_t &ballot,
                                const ChronosAcceptReq &chr_req,
                                int32_t *res,
                                ChronosAcceptRes *chr_res) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = (TxOV*)(GetOrCreateDTxn(txn_id));
  if (dtxn->max_seen_ballot_ > ballot) {
    *res = REJECT;
    verify(0); // do not support failure recovery so far.
  } else {
    dtxn->max_accepted_ballot_ = ballot;
    *res = SUCCESS;
  }
}


void SchedulerOV::OnCommit(const txnid_t cmd_id,
                                const ChronosCommitReq &chr_req,
                                int32_t *res,
                                TxnOutput *output,
                                ChronosCommitRes *chr_res,
                                const function<void()> &callback) {
  // TODO to support cascade abort
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *res = SUCCESS;
  //union the graph into dep graph
  auto dtxn = (TxOV *)(GetOrCreateDTxn(cmd_id));
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


int SchedulerOV::OnInquire(epoch_t epoch,
                                cmdid_t cmd_id,
                                RccGraph* graph,
                                const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO check epoch, cannot be a too old one.
  auto dtxn = (TxOV *)(GetOrCreateDTxn(cmd_id));
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



void SchedulerOV::OnCreateTs (txnid_t txnid,
                 int64_t *timestamp,
                 int16_t *server_id){


  std::lock_guard<std::recursive_mutex> lock(mtx_);
  ov_ts_t ovts = tid_mgr_->CreateTs(txnid);

//  Log_info("Returned ts = %ld, id = %d", ovts.timestamp, ovts.site_id);

  *timestamp = ovts.timestamp_;
  *server_id = siteid_t(ovts.site_id_);

  return;
}

void SchedulerOV::OnStoredRemoveTs(uint64_t txnid, int64_t timestamp, int16_t server_id, int32_t *res) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);

  tid_mgr_->StoredTs(txnid, timestamp, server_id);
  *res = SUCCESS;

  return;
}

void SchedulerOV::OnPublish(int64_t dc_ts, int16_t dc_id, int64_t *ret_ts, int16_t *ret_id) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);

  ov_ts_t dc_ovts (dc_ts, dc_id);

  if (dc_ovts > this->vwatermark_){
    this->vwatermark_ = dc_ovts;
    //TODO: trigger execute
  }

  ov_ts_t svw = tid_mgr_->GetServerVWatermark();
  *ret_ts = svw.timestamp_;
  *ret_id = svw.site_id_;

  return;
}

void SchedulerOV::OnExecute(uint64_t txn_id,
                            const OVExecuteReq &req,
                            int32_t *res,
                            OVExecuteRes *ov_res,
                            TxnOutput *output,
                            const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);

  *res = SUCCESS;

  auto dtxn = (TxOV *) (GetOrCreateDTxn(txn_id));
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;

  verify(!dtxn->IsExecuted());

  if (dtxn->ovts_> vwatermark_){
    Log_info("[txn %d] vwatermark not ready, postpone execution, vwatermark = %ld, ts = %ld", txn_id, vwatermark_.timestamp_, dtxn->ovts_.timestamp_);
    dtxn->executed_callback = callback;
  }
  else{
    dtxn->CommitExecute();
    stored_txns_by_id_.erase(txn_id);
    Log_info("[txn %d] executed, vwatermark = %ld, ts = %ld", txn_id,  vwatermark_.timestamp_, dtxn->ovts_.timestamp_);
    callback();
  }

  return;
}

OVCommo *SchedulerOV::commo() {

  auto commo = dynamic_cast<OVCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}


