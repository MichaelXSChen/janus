#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coord.h"
#include "scheduler.h"
#include "tx_box.h"
#include "commo.h"
#include "config.h"

namespace rococo {

static Frame *tapir_frame_s = Frame::RegFrame(MODE_TAPIR,
                                              {"tapir"},
                                              []() -> Frame * {
                                                return new FrameTapir();
                                              });

Executor *FrameTapir::CreateExecutor(cmdid_t cmd_id, Scheduler *sched) {
  Executor *exec = new TapirExecutor(cmd_id, sched);
  return exec;
}

Coordinator *FrameTapir::CreateCoord(cooid_t coo_id,
                                     Config *config,
                                     int benchmark,
                                     ClientControlServiceImpl *ccsi,
                                     uint32_t id,
                                     TxnRegistry *txn_reg) {
  verify(config != nullptr);
  TapirCoord *coord = new TapirCoord(coo_id,
                                     benchmark,
                                     ccsi,
                                     id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Communicator *FrameTapir::CreateCommo(PollMgr *pollmgr) {
  // Default: return null;
  commo_ = new TapirCommo;
  return commo_;
}

Scheduler *FrameTapir::CreateScheduler() {
  Scheduler *sched = new SchedulerTapir();
  sched->frame_ = this;
  return sched;
}

mdb::Row *FrameTapir::CreateRow(const mdb::Schema *schema,
                                vector<Value> &row_data) {

  mdb::Row *r = mdb::VersionedRow::create(schema, row_data);
  return r;
}

TxBox *FrameTapir::CreateDTxn(epoch_t epoch, txnid_t tid,
                             bool ro, Scheduler *mgr) {
  auto dtxn = new TxBoxTapir(epoch, tid, mgr);
  return dtxn;
}

} // namespace rococo
