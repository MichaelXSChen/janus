#include "ov-txn_mgr.h"
#include <chrono>


namespace rococo {


ov_ts_t TidMgr::CreateTs(mdb::txn_id_t txn_id) {
  std::unique_lock<std::mutex> lk(mu);

  auto now = std::chrono::system_clock::now();

  int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

  if (ts < last_clock_){
    ts = ++last_clock_;
  }else{
    last_clock_ = ts;
  }

  ov_ts_t ovts;

  ovts.timestamp = ts;
  ovts.timestamp = server_id_;


  s_phase_txns_[ovts] = txn_id;

  return ovts;
}



}//namespace rococo