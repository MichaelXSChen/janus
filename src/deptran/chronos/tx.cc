
#include "tx.h"

namespace janus {

void TxChronos::DispatchExecute(SimpleCommand &cmd,
                              int32_t *res,
                              map<int32_t, Value> *output) {
  Log_info("%s called" , __FUNCTION__);

    phase_ = PHASE_RCC_DISPATCH;
  for (auto& c: dreqs_) {
    if (c.inn_id() == cmd.inn_id()) // already handled?
      return;
  }
  verify(txn_reg_);
  TxnPieceDef& piece = txn_reg_->get(cmd.root_type_, cmd.type_);
  auto& conflicts = piece.conflicts_;
  for (auto& c: conflicts) {
    vector<Value> pkeys;
    for (int i = 0; i < c.primary_keys.size(); i++) {
      pkeys.push_back(cmd.input.at(c.primary_keys[i]));
    }
    auto row = Query(GetTable(c.table), pkeys, c.row_context_id);
    verify(row != nullptr);
    for (auto col_id : c.columns) {
      TraceDep(row, col_id, TXN_DEFERRED);
    }
  }
  dreqs_.push_back(cmd);

  // TODO are these preemptive actions proper?
  int ret_code;
  cmd.input.Aggregate(ws_);
  piece.proc_handler_(nullptr,
                          *this,
                          cmd,
                          &ret_code,
                          *output);
  ws_.insert(*output);
}

} // namespace janus