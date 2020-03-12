

#include "scheduler.h"
#include "commo.h"
#include "deptran/rococo/tx.h"

using namespace janus;

void SchedulerChronos::OnPreAccept(const txid_t txn_id,
                                 const vector<SimpleCommand> &cmds,
                                 const ChronosPreAcceptReq &chr_req,
                                 int32_t *res,
                                 ChronosPreAcceptRes *chr_res) {
  /*
   * xsTODO: Steps:
    */

  Log_info("[%s]:%s called", __FILE__, __FUNCTION__ );

  std::lock_guard<std::recursive_mutex> lock(mtx_);
  //Log_info("on preaccept: %llx par: %d", txn_id, (int)partition_id_);
  //if (RandomGenerator::rand(1, 2000) <= 1)
  //Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(txn_id));
  dtxn->UpdateStatus(TXN_PAC);
  dtxn->involve_flag_ = TxRococo::INVOLVED;
  TxRococo &tinfo = *dtxn;
  if (dtxn->max_seen_ballot_ > 0) {
    *res = REJECT;
  } else {
    if (dtxn->status() < TXN_CMT) {
      if (dtxn->phase_ < PHASE_RCC_DISPATCH && tinfo.status() < TXN_CMT) {
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
    tinfo.fully_dispatched = true;
    if (tinfo.status() >= TXN_CMT) {
      waitlist_.insert(dtxn.get());
      verify(dtxn->epoch_ > 0);
    }
    *res = SUCCESS;
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
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(cmd_id));
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;

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


int SchedulerChronos::OnDispatch(const vector<SimpleCommand>& cmd,
                                const ChronosDispatchReq &chr_req,
                                int32_t* res,
                                ChronosDispatchRes *chr_res,
                                TxnOutput* output) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  txnid_t txn_id = cmd[0].root_id_;
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(txn_id));
  verify(dtxn->id() == txn_id);
  verify(RccGraph::partition_id_ == Scheduler::partition_id_);
  //auto job = [&cmd, res, dtxn, callback, graph, output, this, txn_id] () {
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  for (auto& c : cmd) {
    dtxn->DispatchExecute(const_cast<SimpleCommand&>(c),
                          res, &(*output)[c.inn_id()]);
  }
  dtxn->UpdateStatus(TXN_STD);
  verify(cmd[0].root_id_ == txn_id);
  return 0;
}

ChronosCommo *SchedulerChronos::commo() {

  auto commo = dynamic_cast<ChronosCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}
