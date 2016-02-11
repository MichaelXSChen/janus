
#include "service.h"
#include "sched.h"

namespace rococo {

MultiPaxosServiceImpl::MultiPaxosServiceImpl(Scheduler *sched)
    : sched_((MultiPaxosSched*)sched) {

}

void MultiPaxosServiceImpl::Forward(const Command& cmd,
                                    rrr::DeferredReply* defer) {

}

void MultiPaxosServiceImpl::Prepare(const uint64_t& slot,
                                    const ballot_t& ballot,
                                    uint64_t* max_ballot,
                                    rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnPrepareRequest(slot,
                          ballot,
                          max_ballot,
                          std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Accept(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const Command& cmd,
                                   uint64_t* max_ballot,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnAcceptRequest(slot,
                          ballot,
                          cmd,
                          max_ballot,
                          std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const Command& cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnDecideRequest(slot,
                          ballot,
                          cmd);
  defer->reply();
}


} // namespace rococo;