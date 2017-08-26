#pragma once

#include "../frame.h"
#include "../constants.h"
#include "commo.h"

namespace janus {

class FrameTapir : public Frame {
 public:
  FrameTapir() : Frame(MODE_TAPIR) { }
  Executor *CreateExecutor(cmdid_t, Scheduler *sched) override;
  Coordinator *CreateCoord(cooid_t coo_id,
                           Config *config,
                           int benchmark,
                           ClientControlServiceImpl *ccsi,
                           uint32_t id,
                           TxnRegistry *txn_reg) override;
  Scheduler *CreateScheduler() override;
  mdb::Row *CreateRow(const mdb::Schema *schema,
                      vector<Value> &row_data) override;
  Communicator* CreateCommo(PollMgr* pollmgr = nullptr) override;

  TxBox* CreateDTxn(epoch_t epoch, txnid_t tid,
                   bool ro, Scheduler * mgr) override;

};
} // namespace rococo