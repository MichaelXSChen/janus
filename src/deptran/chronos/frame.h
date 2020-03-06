#pragma once

#include "../frame.h"
#include "deptran/janus/frame.h"


namespace janus {

class ChronosFrame : public JanusFrame {
 public:
  ChronosFrame(int mode = MODE_CHRONOS) : JanusFrame() {}
  
  //xs: always null ptr, whats that for?
  Executor *CreateExecutor(cmdid_t, Scheduler *sched) override;
  
  //xs: transaction coordinator,
  //broadcastAccept to communicators.
  //call acceptrACK to do after receive ACK. 
  //created by the client process/thread. 
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 TxnRegistry *txn_reg) override;
  
  //xs: What a participant do on receiving various requests.
  //created by each 
  Scheduler *CreateScheduler() override;
  
  //xs: whats that for
  //created by each server, but not client. 
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           Scheduler *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi)
  override;
  
//  //xs: called during initialization
//  mdb::Row *CreateRow(const mdb::Schema *schema,
//                      vector<Value> &row_data) override;
//
//
//  //xs: called in each site.
//  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
//                          bool ro, Scheduler *mgr) override;

  //created by each client **and** each server 
  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
};

} // namespace janus
