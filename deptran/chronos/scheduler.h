//
// Created by micha on 2020/3/23.
//


#pragma once
#include "deptran/brq/sched.h"
#include "deptran/rcc_rpc.h"
namespace rococo {

class RccGraph;
class ChronosCommo;


class chr_ts_t{
 public:
  chr_ts_t(int64_t ts, int64_t counter, int16_t site_id): timestamp_(ts), stretch_counter_(counter), site_id_(site_id){}
  chr_ts_t(): timestamp_(0), stretch_counter_(0), site_id_(0){}
  int64_t timestamp_;
  int64_t stretch_counter_;
  int16_t site_id_;
  inline bool operator < (const chr_ts_t rhs) const{
    if (this->timestamp_ < rhs.timestamp_){
      return true;
    }
    else if (this->timestamp_ < rhs.timestamp_){
      return false;
    }
    else {
     if (this->stretch_counter_ < rhs.stretch_counter_){
       return true;
     }
     else if (this->stretch_counter_ > rhs.stretch_counter_){
       return false;
     }
     else{
       return this->site_id_ < rhs.site_id_;
     }
    }
  }

  inline bool operator == (const chr_ts_t rhs) const{
    return (this->timestamp_ == rhs.timestamp_ && this->stretch_counter_ == rhs.stretch_counter_ && this->site_id_ == rhs.site_id_);
  }

  inline bool operator > (const chr_ts_t rhs) const{
    return (!this->operator<(rhs) && !this->operator==(rhs));
  }

};




class SchedulerChronos : public BrqSched {
 public:
  using BrqSched::BrqSched;


  int OnDispatch(const vector<SimpleCommand> &cmd,
                 const ChronosDispatchReq &chr_req,
                 rrr::i32 *res,
                 ChronosDispatchRes *chr_res,
                 TxnOutput* output);


  void OnPreAccept(txnid_t txnid,
                   const vector<SimpleCommand> &cmds,
                   const ChronosPreAcceptReq &chr_req,
                   int32_t *res,
                   ChronosPreAcceptRes *chr_res);


  void OnAccept(txnid_t txn_id,
                const ballot_t& ballot,
                const ChronosAcceptReq &chr_req,
                int32_t* res,
                ChronosAcceptRes *chr_res);

  void OnCommit(txnid_t txn_id,
                const ChronosCommitReq &chr_req,
                int32_t *res,
                TxnOutput *output,
                ChronosCommitRes *chr_res,
                const function<void()> &callback);

  int OnInquire(epoch_t epoch,
                cmdid_t cmd_id,
                RccGraph* graph,
                const function<void()> &callback) override;

  void StoreLocalAck(txnid_t txn_id, int res, ChronosStoreLocalRes &chr_res);

  ChronosCommo* commo();

  std::set<txnid_t> local_pending_txns_ {};

  std::atomic<uint64_t> logical_clock {0};

};
} // namespace janus
