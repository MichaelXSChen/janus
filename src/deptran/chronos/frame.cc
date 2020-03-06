
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "../rococo/rcc_row.h"
#include "commo.h"
#include "frame.h"
#include "coordinator.h"
#include "scheduler.h"
#include "tx.h"

namespace janus {

static Frame *chronos_frame_s = Frame::RegFrame(MODE_CHRONOS,
                                              {"chronos"},
                                              []() -> Frame * {
                                                return new ChronosFrame();
                                              });

Coordinator *ChronosFrame::CreateCoordinator(cooid_t coo_id,
                                           Config *config,
                                           int benchmark,
                                           ClientControlServiceImpl *ccsi,
                                           uint32_t id,
                                           TxnRegistry *txn_reg) {
  
  if (site_info_ != nullptr){
    Log_info("[site %d] created chronos coordinator", site_info_->id); 
  }else{
    Log_info("[site null] created chronos coordinator"); 
  }
  

  verify(config != nullptr);
  CoordinatorChronos *coord = new CoordinatorChronos(coo_id,
                                     benchmark,
                                     ccsi,
                                     id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Executor *ChronosFrame::CreateExecutor(uint64_t, Scheduler *sched) {
  if (site_info_ != nullptr){
    Log_info("[site %d] created executor", site_info_->id); 
  }else{
    Log_info("[site null] created executor"); 
  }
  verify(0);
  return nullptr;
}

Scheduler *ChronosFrame::CreateScheduler() {
  if (site_info_ != nullptr){
    Log_info("[site %d] created scheduler", site_info_->id); 
  }else{
    Log_info("[site null] created scheduler"); 
  }

  Scheduler *sched = new SchedulerChronos();
  sched->frame_ = this;
  return sched;
}

//XS: seems no need to override. Use the base funciton is ok.
//for now, only debug print is slightly different
vector<rrr::Service *>
ChronosFrame::CreateRpcServices(uint32_t site_id,
                              Scheduler *sched,
                              rrr::PollMgr *poll_mgr,
                              ServerControlServiceImpl *scsi) {
  
  
  
  if (site_info_ != nullptr){
    Log_info("[site %d] created rpc services", site_info_->id); 
  }else{
    Log_info("[site null] created rpc services"); 
  }
  
  return Frame::CreateRpcServices(site_id, sched, poll_mgr, scsi);
}

//mdb::Row *ChronosFrame::CreateRow(const mdb::Schema *schema,
//                                vector<Value> &row_data) {
//  if (site_info_ != nullptr){
//    Log_info("[site %d] [Chronos] created row", site_info_->id);
//  }else{
//    Log_info("[site null] [Chrnonos] created row");
//  }
//
//
//
//  mdb::Row *r = RCCRow::create(schema, row_data);
//  return r;
//}
//
//shared_ptr<Tx> ChronosFrame::CreateTx(epoch_t epoch, txnid_t tid,
//                                    bool ro, Scheduler *mgr) {
////  auto dtxn = new JanusDTxn(tid, mgr, ro);
////  return dtxn;
//   if (site_info_ != nullptr){
//     Log_info("[site %d] [chronos] created txn", site_info_->id);
//   }else{
//     Log_info("[site null] [chronos] created txn");
//   }
//
//
//  shared_ptr<Tx> sp_tx(new TxChronos(epoch, tid, mgr, ro));
//  return sp_tx;
//}

Communicator *ChronosFrame::CreateCommo(PollMgr *poll) {
  if (site_info_ != NULL){
    Log_info("[site %d] Creating chronos communicator", site_info_->id);
  }
  else{
    Log_info("[site null] Creating chronos communicator, I think it should be the client");
  }
  return new ChronosCommo(poll);
}

} // namespace janus
