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

  virtual ~CoordinatorChronos() {}

  ChronosCommo *commo();
  // Dispatch inherits from RccCoord;
  void DispatchRo() override { DispatchAsync(); }

  void PreAccept();
  void PreAcceptAck(phase_t phase,
                    parid_t par_id,
                    int res,
                    shared_ptr<RccGraph> graph);
  void Dispatch();
  void ChrDispatchAck(phase_t phase,
                           int res,
                           TxnOutput& cmd,
                           ChronosDispatchRes &chr_res,
                           RccGraph& graph);





  // do_one inherits from RccCoord;

  void restart() { verify(0); };
  // functions needed in the fast accept phase.
  bool FastpathPossible();
  bool AllFastQuorumsReached();
  bool SlowpathPossible() {
    // TODO without failures, slow path should always be possible.
    return true;
  };
  int32_t GetFastQuorum(parid_t par_id);
  int32_t GetSlowQuorum(parid_t par_id);
  bool PreAcceptAllSlowQuorumsReached();

  void prepare();
  // functions needed in the accept phase.
  void ChooseGraph();
  void Accept();
  void AcceptAck(phase_t phase, parid_t par_id, int res);
  bool AcceptQuorumPossible() {
    return true;
  };
  bool AcceptQuorumReached();

  void Commit() override;
  void CommitAck(phase_t phase,
                 parid_t par_id,
                 int32_t res,
                 TxnOutput &output);
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
  int FastQuorumGraphCheck(parid_t par_id);
  void GotoNextPhase() override;
  void Reset() override;

  std::atomic<uint64_t> logical_clock {0};
};
} // namespace janus
