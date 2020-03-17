#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../command.h"
#include "deptran/janus/coordinator.h"




namespace janus {
class ChronosCommo;
class CoordinatorChronos : public CoordinatorJanus {
 public:
  enum Phase {CHR_INIT=0, CHR_DISPATCH=1, CHR_FAST=2, CHR_FALLBACK=3, CHR_COMMIT=4};
  enum Decision {CHR_UNK=0, CHR_COMMI=1, CHR_ABORT=2 };
  using CoordinatorJanus::CoordinatorJanus;


  map<parid_t, std::shared_ptr<ChronosPreAcceptRes>> pre_accept_acks_;


  virtual ~CoordinatorChronos() {}

  ChronosCommo *commo();
  // Dispatch inherits from RccCoord;
  void DispatchRo() override { DispatchAsync(); }






  // do_one inherits from RccCoord;

  void restart() { verify(0); };
  // functions needed in the fast accept phase.
  bool AllFastQuorumsReached();
  bool SlowpathPossible() {
    // TODO without failures, slow path should always be possible.
    return true;
  };
  int32_t GetSlowQuorum(parid_t par_id);
  bool PreAcceptAllSlowQuorumsReached();

  // functions needed in the accept phase.
  bool AcceptQuorumPossible() {
    return true;
  };
  bool AcceptQuorumReached();


  bool check_commit() {
    verify(0);
    return false;
  };

//  void launch(Command &cmd);
  void launch_recovery(cmdid_t cmd_id);

  ballot_t magic_ballot() {
    ballot_t ret = 0;
    ret = (ret << 32) | coo_id_;
    return ret;
  }

  cmdid_t next_cmd_id() {
    cmdid_t ret = cmdid_prefix_c_++;
    ret = (ret << 32 | coo_id_);
    return ret;
  }
  int FastQuorumGraphCheck(parid_t par_id) = delete;
  void Reset() override;



  //xs's code start here
  bool FastpathPossible();
  std::atomic<uint64_t> logical_clock {0};
  int32_t GetQuorumSize(parid_t par_id);
  int FastQuorumCheck(parid_t par_id);
  int GetTsIntersection(parid_t par_id, int32_t &t_left, int32_t &t_right);

  void Dispatch();
  void DispatchAck(phase_t phase,
                   int res,
                   TxnOutput& cmd,
                   ChronosDispatchRes &chr_res);

  void PreAccept();
  void PreAcceptAck(phase_t phase,
                    parid_t par_id,
                    int res,
                    std::shared_ptr<ChronosPreAcceptRes> chr_res);

  void Accept();
  void AcceptAck(phase_t phase, parid_t par_id, int res, ChronosAcceptRes &chr_res);

  void Commit() override;
  void CommitAck(phase_t phase,
                 parid_t par_id,
                 int32_t res,
                 ChronosCommitRes &chr_res,
                 TxnOutput &output);

  void GotoNextPhase() override;

  int64_t ts_left_;
  int64_t ts_right_;

  int64_t ts_delta_ = 10;

};
} // namespace janus
