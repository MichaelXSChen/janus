#include "ov-txn_mgr.h"
#include <chrono>


namespace rococo {


ov_ts_t TidMgr::CreateTs(mdb::txn_id_t txn_id) {
//  std::unique_lock<std::mutex> lk(mu);

  auto now = std::chrono::system_clock::now();

  int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

  if (ts < last_clock_){
    ts = ++last_clock_;
  }else{
    last_clock_ = ts;
  }

  ov_ts_t ovts;


  ovts.timestamp_ = ts;
  ovts.site_id_ = site_id_;



  s_phase_txns_[ovts] = txn_id;


  Log_info("TidMgr %d created Ts for txn %d, ts = %ld, ovts.ts = %ld ", this->site_id_, txn_id, ts, ovts.timestamp_);

  return ovts;
}

void TidMgr::StoredTs(mdb::txn_id_t txn_id, int64_t timestamp, int16_t server_id) {
 ov_ts_t ovts;
 ovts.timestamp_ = timestamp;
 ovts.site_id_ = server_id;

 verify(s_phase_txns_.count(ovts) != 0);
 verify(s_phase_txns_[ovts] == txn_id);

 s_phase_txns_.erase(ovts);


 Log_info("TidMgr %d stored (removed) Ts for txn %d, ts = %ld, ovts.ts = %ld ", this->site_id_, txn_id, timestamp, ovts.timestamp_);

}


ov_ts_t TidMgr::GetServerVWatermark() {
  if (s_phase_txns_.size() == 0){
    ov_ts_t ovts;
    ovts.timestamp_ =  last_clock_;
    ovts.site_id_ = this->site_id_;
    return ovts;
  }else{
    ov_ts_t ovts = s_phase_txns_.begin()->first;
    return ovts;
  }
}






}//namespace rococo