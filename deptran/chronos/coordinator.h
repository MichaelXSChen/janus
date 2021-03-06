//
// Created by micha on 2020/3/23.
//
#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../command.h"
#include "../brq/coord.h"




namespace rococo {
class ChronosCommo;
class CoordinatorChronos : public BrqCoord {
 public:
  enum Phase {CHR_INIT=0, CHR_COMMIT=1};
  enum Decision {CHR_UNK=0, CHR_COMMI=1, CHR_ABORT=2 };
  using BrqCoord::BrqCoord;




  virtual ~CoordinatorChronos() {}

  ChronosCommo *commo();
  // Dispatch inherits from RccCoord;





  // functions needed in the fast accept phase.
  bool SlowpathPossible() {
    // TODO without failures, slow path should always be possible.
    return true;
  };

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
  void Reset() override;



  //xs's code start here
  std::atomic<uint64_t> logical_clock {0};
//  int32_t GetQuorumSize(parid_t par_id);


  void SubmitReq();
  void SubmitAck(phase_t phase,
                   int res,
                   TxnOutput& cmd,
                   ChronosDispatchRes &chr_res);

  void PreAccept();
  void PreAcceptAck(phase_t phase,
                    parid_t par_id,
                    int res,
                    std::shared_ptr<ChronosPreAcceptRes> chr_res);

  bool CheckTsIntersection();
  bool PreAcceptQuroumAck();



  void Accept();
  void AcceptAck(phase_t phase, parid_t par_id, int res, ChronosAcceptRes &chr_res);

  void Commit() override;
  void CommitAck(phase_t phase,
                 parid_t par_id,
                 int32_t res,
                 ChronosCommitRes &chr_res,
                 TxnOutput &output);

  void GotoNextPhase() override;



  map<parid_t, std::vector<std::shared_ptr<ChronosPreAcceptRes>>> pre_accept_acks_;

  //
  int64_t ts_left_;
  int64_t ts_right_;
  int64_t ts_delta_ = 10;

  //
  int64_t ts_fast_commit_;
};
} // namespace janus

