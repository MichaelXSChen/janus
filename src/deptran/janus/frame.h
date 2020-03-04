#pragma once

#include "../frame.h"

/*
XSTODO: 

1. who call these creates? Who save/own these created objs.
2. Who call these objects' methods. 
3. What are Executor for. 
4. What are RPCServicies do.
5. What are row, and tx.

*/
namespace janus {

class JanusFrame : public Frame {
 public:
  JanusFrame(int mode = MODE_JANUS) : Frame(mode) {}
  
  //xs: always null ptr, whats that for?
  Executor *CreateExecutor(cmdid_t, Scheduler *sched) override;
  
  //xs: transaction coordinator,
  //broadcastAccept to communicators.
  //call acceptrACK to do after receive ACK. 
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 TxnRegistry *txn_reg) override;
  
  //xs: What a participant do on receiving various requests.
  Scheduler *CreateScheduler() override;
  
  //xs: whats that for
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           Scheduler *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi)
  override;
  mdb::Row *CreateRow(const mdb::Schema *schema,
                      vector<Value> &row_data) override;

  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                          bool ro, Scheduler *mgr) override;

  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
};

} // namespace janus
