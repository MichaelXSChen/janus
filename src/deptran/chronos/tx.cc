
#include "tx.h"

namespace janus {



//SimpleCommand is a typedef of TxnPieceData
//add a simpleCommand to the local Tx's dreq
void TxChronos::DispatchExecute(SimpleCommand &cmd,
                              int32_t *res,
                              map<int32_t, Value> *output) {
  Log_info("%s called" , __FUNCTION__);

  phase_ = PHASE_RCC_DISPATCH;

  //xs: Step 1: skip this simpleCommand if it is already in the dreqs.
  for (auto& c: dreqs_) {
    if (c.inn_id() == cmd.inn_id()) // already handled?
      return;
  }
  verify(txn_reg_);

  //xs: step 2: get the definition (stored procedure) of the txn.
  //So next question, what is conflicts.
  TxnPieceDef& piece = txn_reg_->get(cmd.root_type_, cmd.type_);
  auto& conflicts = piece.conflicts_;
  for (auto& c: conflicts) {
    vector<Value> pkeys;
    for (int i = 0; i < c.primary_keys.size(); i++) {
      pkeys.push_back(cmd.input.at(c.primary_keys[i]));
    }
    auto row = Query(GetTable(c.table), pkeys, c.row_context_id);
    verify(row != nullptr);

    //No need for chronos
    //for (auto col_id : c.columns) {
    //    TraceDep(row, col_id, TXN_DEFERRED);
    //}
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



bool TxChronos::ReadColumn(mdb::Row *row,
                          mdb::colid_t col_id,
                          Value *value,
                          int hint_flag) {

  Log_info("[[%s]] called, table = %s, col_id = %d, phase_ = %d, hint_flag = %d", __PRETTY_FUNCTION__ , row->get_table()->Name().c_str(), col_id, phase_, hint_flag);
  Log_info("Rtti = %d", row->rtti());
  auto r = dynamic_cast<mdb::VersionedRow*>(row);




  auto ver = r->get_column_ver(col_id);
  Log_info("Version = %d", ver);


  verify(!read_only_);
  if (phase_ == PHASE_RCC_DISPATCH) {
    int8_t edge_type;
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_INSTANT) {
      mdb_txn()->read_column(row, col_id, value);
    }

    if (hint_flag == TXN_INSTANT || hint_flag == TXN_DEFERRED) {
//      entry_t *entry = ((RCCRow *) row)->get_dep_entry(col_id);
      //TraceDep(row, col_id, hint_flag);
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      mdb_txn()->read_column(row, col_id, value);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  return true;
}


bool TxChronos::WriteColumn(Row *row,
                           colid_t col_id,
                           const Value &value,
                           int hint_flag) {
  verify(!read_only_);
  if (phase_ == PHASE_RCC_DISPATCH) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_INSTANT) {
      mdb_txn()->write_column(row, col_id, value);
    }
    if (hint_flag == TXN_INSTANT || hint_flag == TXN_DEFERRED) {
//      entry_t *entry = ((RCCRow *) row)->get_dep_entry(col_id);
     // TraceDep(row, col_id, hint_flag);
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      mdb_txn()->write_column(row, col_id, value);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  return true;
}

} // namespace janus
