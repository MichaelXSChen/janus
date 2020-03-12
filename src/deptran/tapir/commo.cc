
#include "command.h"
#include "procedure.h"
#include "command_marshaler.h"
#include "commo.h"
#include "../coordinator.h"
#include "../rcc_rpc.h"
#include "deptran/service.h"

namespace janus {


void TapirCommo::BroadcastFastAccept(parid_t par_id,
                                     cmdid_t cmd_id,
                                     vector<SimpleCommand>& cmds,
                                     const function<void(int32_t)>& cb) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p : proxies) {
    auto proxy = (ClassicProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [cb] (Future* fu) {
      int32_t res;
      fu->get_reply() >> res;
      cb(res);
    };
    Future::safe_release(proxy->async_TapirFastAccept(cmd_id, cmds, fuattr));
  }
}

void TapirCommo::BroadcastDecide(parid_t par_id,
                                 cmdid_t cmd_id,
                                 int32_t decision) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p : proxies) {
    auto proxy = (ClassicProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [] (Future* fu) {} ;
    Future::safe_release(proxy->async_TapirDecide(cmd_id, decision, fuattr));
  }
}

void TapirCommo::BroadcastAccept(parid_t par_id,
                                 cmdid_t cmd_id,
                                 ballot_t ballot,
                                 int decision,
                                 const function<void(Future*)>& callback) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p: proxies) {
    auto proxy = (ClassicProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = callback;
    Future::safe_release(proxy->async_TapirAccept(cmd_id,
                                                  ballot,
                                                  decision,
                                                  fuattr));
  }
}

} // namespace janus
