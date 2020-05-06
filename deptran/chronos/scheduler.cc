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

SchedulerChronos::SchedulerChronos(Frame *frame): BrqSched(){
  this->frame_ = frame;
  auto config = Config::GetConfig();
  for (auto &site: config->SitesByPartitionId(frame->site_info_->partition_id_)){
    if (site.id != frame->site_info_->id){
      local_replicas_ts_[site.id] = chr_ts_t();
      Log_info("created timestamp for local replica for partition %u,  site_id = %hu", frame->site_info_->partition_id_, site.id);
    }
  }
}


int SchedulerChronos::OnSubmit(const vector<SimpleCommand>& cmd,
                                 const ChronosDispatchReq &chr_req,
                                 int32_t* res,
                                 ChronosDispatchRes *chr_res,
                                 TxnOutput* output,
                                 const function<void()> &reply_callback) {

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  txnid_t txn_id = cmd[0].root_id_; //should have the same root_id

  verify(local_txns_by_me.count(txn_id) == 0);
  chr_ts_t ts = GenerateChrTs(true);

  Log_info("%s called for txn_id = %lu", __FUNCTION__, txn_id);

  if (pending_local_txns_.count(ts) != 0){
    Log_info("ts %lu:%lu:%hu with original id = %lu, new id = %lu", ts.timestamp_, ts.stretch_counter_, ts.site_id_, pending_local_txns_[ts], txn_id);
    verify(0);
  }else{
    Log_info("ts %lu:%lu:%hu set for id = %lu", ts.timestamp_, ts.stretch_counter_, ts.site_id_, txn_id);
    pending_local_txns_[ts] = txn_id;
  }

  auto dtxn = (TxChronos* )(GetOrCreateDTxn(txn_id));
  dtxn->ts_ = ts;
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

  dtxn->execute_callback_ = execute_callback;

  auto ack_callback = std::bind(&SchedulerChronos::StoreLocalAck,
                            this,
                            txn_id,
                            std::placeholders::_1,
                            std::placeholders::_2,
                            std::placeholders::_3);

  ChronosStoreLocalReq req;
  req.txn_site_id = ts.site_id_;
  req.txn_timestamp = ts.timestamp_;
  req.txn_strech_counter = ts.stretch_counter_;


  verify(dtxn->id() == txn_id);
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  verify(cmd[0].root_id_ == txn_id);

  commo()->SendStoreLocal(cmd, req, ack_callback);

  return 0;
}

void SchedulerChronos::StoreLocalAck(txnid_t txn_id,
                                     int total_count,
                                     int received_res,
                                     ChronosStoreLocalRes &chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  auto dtxn = (TxChronos* )(GetOrCreateDTxn(txn_id));
  if (received_res == SUCCESS){
    dtxn->n_local_store_acks++;
  }
  else{
    verify(0);
  }


  siteid_t src_site = chr_res.my_site_id;
  chr_ts_t src_ts;
  src_ts.site_id_ = chr_res.my_site_id;
  src_ts.stretch_counter_ = chr_res.my_strech_counter;
  src_ts.timestamp_ = chr_res.my_timestamp;


  verify(local_replicas_ts_.count(src_site) != 0);
  if (local_replicas_ts_[src_site] < src_ts){
    local_replicas_ts_[src_site] = src_ts;
    Log_info("watermark for site %hu changed to %lu:%lu:%hu", src_site, src_ts.timestamp_, src_ts.stretch_counter_, src_ts.site_id_);
  }

  Log_info("%s called, txnid= %lu, count = %d, total = %d", __FUNCTION__ , txn_id, dtxn->n_local_store_acks, total_count);
  if (dtxn->n_local_store_acks == total_count){
    auto dtxn = (TxChronos* )(GetOrCreateDTxn(txn_id));
    dtxn->local_stored_ = true;
  }

  CheckExecutableTxns();

}

void SchedulerChronos::CheckExecutableTxns(){
  //xs todo: this seems not necessary
  chr_ts_t min_ts = GenerateChrTs(true);
  for (auto &pair: local_replicas_ts_){
    if (pair.second  < min_ts){
      min_ts = pair.second;
    }
  }

  for (auto itr = pending_local_txns_.begin(); itr != pending_local_txns_.end(); ){
    if (itr->first > min_ts){
      Log_info("Not going to execute, queue head ts = %lu:%lu:%hu < min ts = %lu:%lu:%hu",
          itr->first.timestamp_,
          itr->first.stretch_counter_,
          itr->first.site_id_,
          min_ts.timestamp_,
          min_ts.stretch_counter_,
          min_ts.site_id_);

      break;
    }
    txnid_t txn_id = itr->second;
    auto dtxn = (TxChronos* )(GetOrCreateDTxn(txn_id));
    if (!dtxn->local_stored_){
      Log_info("Not going to execute, queue head txn id = %lu not stored", itr->second);
      break;
    }
    Log_info("Going to execute txn with id = %lu", txn_id);
    dtxn->execute_callback_();
    itr = pending_local_txns_.erase(itr);
  }

}

//Log_info stretchable timestamp implemented here
chr_ts_t SchedulerChronos::GenerateChrTs(bool for_local) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  chr_ts_t ret;


  auto now = std::chrono::system_clock::now();

  int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() + this->time_drift_ms_;
  ret.timestamp_ = ts;
  ret.site_id_ = site_id_;
  ret.stretch_counter_ = 0;

  Log_info("ret = %lu:%lu:%hu, last = %lu:%lu:%hu", ret.timestamp_, ret.stretch_counter_, ret.site_id_, last_clock_.timestamp_, last_clock_.stretch_counter_, last_clock_.site_id_);
  if (ret <= last_clock_){
    ret = last_clock_; 
    ret.stretch_counter_ ++; 
    
  }
    last_clock_ = ret;
  return ret;
}


void SchedulerChronos::OnStoreLocal(const vector<SimpleCommand> &cmd,
                 const ChronosStoreLocalReq &chr_req,
                 rrr::i32 *res,
                 ChronosStoreLocalRes *chr_res,
                 const function<void()> &reply_callback){

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called, txnid = %lu, ts = %lu:%lu:%hu", __FUNCTION__ , cmd[0].root_id_,
      chr_req.txn_timestamp,
      chr_req.txn_strech_counter,
      chr_req.txn_site_id);
  *res = SUCCESS;

  chr_ts_t ts;
  ts.timestamp_ = chr_req.txn_timestamp;
  ts.site_id_ = chr_req.txn_site_id;
  ts.stretch_counter_ = chr_req.txn_strech_counter;

  if (local_replicas_ts_[ts.site_id_] < ts){
    local_replicas_ts_[ts.site_id_] = ts;
    CheckExecutableTxns();
  }

  auto txn_id = cmd[0].root_id_;

  if (this->pending_local_txns_.count(ts) != 0){
    Log_info("received tx with same ts %lu:%lu:%hu, id in map = %lu, received id = %lu", ts.timestamp_, ts.stretch_counter_, ts.site_id_,pending_local_txns_[ts], txn_id);
        verify(0);
  }


  this->pending_local_txns_[ts] = txn_id;
  auto dtxn = (TxChronos* )(GetOrCreateDTxn(txn_id));
  dtxn->ts_ = ts;

  auto my_ts = GenerateChrTs(true);

  chr_res->my_site_id = my_ts.site_id_;
  chr_res->my_timestamp = my_ts.timestamp_;
  chr_res->my_strech_counter = my_ts.stretch_counter_;

  reply_callback();
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


