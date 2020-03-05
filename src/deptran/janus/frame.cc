
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "../rococo/rcc_row.h"
#include "commo.h"
#include "frame.h"
#include "coordinator.h"
#include "scheduler.h"
#include "dep_graph.h"
#include "tx.h"

namespace janus {

static Frame *janus_frame_s = Frame::RegFrame(MODE_JANUS,
                                              {"brq", "baroque", "janus"},
                                              []() -> Frame * {
                                                return new JanusFrame();
                                              });

Coordinator *JanusFrame::CreateCoordinator(cooid_t coo_id,
                                           Config *config,
                                           int benchmark,
                                           ClientControlServiceImpl *ccsi,
                                           uint32_t id,
                                           TxnRegistry *txn_reg) {
  
  if (site_info_ != nullptr){
    Log_info("[site %d] created coordinator", site_info_->id); 
  }else{
    Log_info("[site null] created coordinator"); 
  }
  

  verify(config != nullptr);
  CoordinatorJanus *coord = new CoordinatorJanus(coo_id,
                                     benchmark,
                                     ccsi,
                                     id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Executor *JanusFrame::CreateExecutor(uint64_t, Scheduler *sched) {
  if (site_info_ != nullptr){
    Log_info("[site %d] created executor", site_info_->id); 
  }else{
    Log_info("[site null] created executor"); 
  }
  
  
  verify(0);
  return nullptr;
}

Scheduler *JanusFrame::CreateScheduler() {
  if (site_info_ != nullptr){
    Log_info("[site %d] created scheduler", site_info_->id); 
  }else{
    Log_info("[site null] created scheduler"); 
  }

  Scheduler *sched = new SchedulerJanus();
  sched->frame_ = this;
  return sched;
}

vector<rrr::Service *>
JanusFrame::CreateRpcServices(uint32_t site_id,
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

mdb::Row *JanusFrame::CreateRow(const mdb::Schema *schema,
                                vector<Value> &row_data) {
  if (site_info_ != nullptr){
    Log_info("[site %d] created row", site_info_->id); 
  }else{
    Log_info("[site null] created row"); 
  }
  
  
  
  mdb::Row *r = RCCRow::create(schema, row_data);
  return r;
}

shared_ptr<Tx> JanusFrame::CreateTx(epoch_t epoch, txnid_t tid,
                                    bool ro, Scheduler *mgr) {
//  auto dtxn = new JanusDTxn(tid, mgr, ro);
//  return dtxn;
  // if (site_info_ != nullptr){
  //   Log_info("[site %d] created txn", site_info_->id); 
  // }else{
  //   Log_info("[site null] created txn"); 
  // }
  
  
  shared_ptr<Tx> sp_tx(new TxJanus(epoch, tid, mgr, ro));
  return sp_tx;
}

Communicator *JanusFrame::CreateCommo(PollMgr *poll) {
  if (site_info_ != NULL){
    Log_info("[site %d] Creating janus communicator", site_info_->id);
  }
  else{
    Log_info("[site null] Creating janus communicator, I think it should be the client");
  }
  return new JanusCommo(poll);
}

} // namespace janus
