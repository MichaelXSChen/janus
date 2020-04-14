//
// Created by tyycxs on 2020/4/10.
//

#ifndef ROCOCO_DEPTRAN_OCEAN_VISTA_OV_TIG_MGR_H_
#define ROCOCO_DEPTRAN_OCEAN_VISTA_OV_TIG_MGR_H_
#include <cstdint>
#include <map>
#include <deptran/constants.h>
#include "memdb/txn.h"

namespace rococo{
class ov_ts_t{
 public:
  ov_ts_t(int64_t ts, int16_t site_id): timestamp_(ts), site_id_(site_id){}
  ov_ts_t(): timestamp_(0), site_id_(0){}
  int64_t timestamp_;
  int16_t site_id_;
  inline bool operator < (const ov_ts_t rhs) const{
    if (this->timestamp_ < rhs.timestamp_){
      return true;
    }
    else if (this->timestamp_ > rhs.timestamp_){
      return false;
    }
    else {
      return this->site_id_ < rhs.site_id_;
    }
  }

  inline bool operator == (const ov_ts_t rhs) const{
    return (this->timestamp_ == rhs.timestamp_ && this->site_id_ == rhs.site_id_);
  }

  inline bool operator > (const ov_ts_t rhs) const{
    return (!this->operator<(rhs) && !this->operator==(rhs));
  }


};



class TidMgr {


public:
  TidMgr(siteid_t site_id): site_id_(site_id) {};
  ov_ts_t CreateTs(mdb::txn_id_t txn_id);
  void StoredTs(mdb::txn_id_t txn_id, int64_t timestamp, int16_t server_id);


  ov_ts_t GetServerVWatermark();
private:
  //Seems not needed, as the scheduler always holds a lock when calling OnXX
//  std::mutex mu;

  //timestamp is made up of two parts, timestamp (clock) | server_id, to ensure uniqueness;
  siteid_t site_id_;
  int64_t last_clock_; //This is for ensuring the monotonicity of the clock.

  std::map<ov_ts_t, mdb::txn_id_t> s_phase_txns_ = {};  //i.e., ts_set in the paper.
};


} //namespace rococo

#endif //ROCOCO_DEPTRAN_OCEAN_VISTA_OV_TIG_MGR_H_