
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

//  Log_info("Rtti = %d", row->rtti());
  auto r = dynamic_cast<mdb::VersionedRow*>(row);
  verify(r->rtti() == symbol_t::ROW_VERSIONED);


  verify(!read_only_);
  if (phase_ == PHASE_RCC_DISPATCH) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_INSTANT) {
      //Currently the same for different Hint-flag
      auto c = r->get_column(col_id);
      row->ref_copy();
      Log_info("Received instant txn");
      *value = c;


      int64_t t_pw = r->max_prepared_wver(col_id);
      int64_t t_cw = r->wver_[col_id];
      int64_t t_low = received_prepared_ts_left_;

      int64_t  t_left = t_pw > t_cw ? t_pw : t_cw;
      t_left = t_left > t_low ? t_left : t_low;

      Log_info("ReadColumn, Prepare phase: table = %s, col_id = %d,  hint_flag = %d, t_pw = %d, t_cw = %d, t_low = %d, t_left = %d",
          row->get_table()->Name().c_str(),
          col_id,
          hint_flag,
          t_pw,
          t_cw,
          t_low,
          t_left);
      prepared_read_ranges_[row][col_id] = pair<int64_t, int64_t>(t_left, received_prepared_ts_right_);


      return true;
    }
    if (hint_flag == TXN_INSTANT || hint_flag == TXN_DEFERRED) {
      auto c = r->get_column(col_id);
      row->ref_copy();
      *value = c;

      int64_t t_pw = r->max_prepared_wver(col_id);
      int64_t t_cw = r->wver_[col_id];
      int64_t t_low = received_prepared_ts_left_;

      int64_t  t_left = t_pw > t_cw ? t_pw : t_cw;
      t_left = t_left > t_low ? t_left : t_low;

      Log_info("ReadColumn, Prepare phase: table = %s, col_id = %d,  hint_flag = %d, t_pw = %d, t_cw = %d, t_low = %d, t_left = %d",
               row->get_table()->Name().c_str(),
               col_id,
               hint_flag,
               t_pw,
               t_cw,
               t_low,
               t_left);
      prepared_read_ranges_[row][col_id] = pair<int64_t, int64_t>(t_left, received_prepared_ts_right_);
      return true;
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      //For commit

      Log_info("ReadColumn, commit phase: table = %s, col_id = %d,  hint_flag = %d, commit_ts = %d,",
               row->get_table()->Name().c_str(),
               col_id,
               hint_flag,
               commit_ts_);

      auto c = r->get_column(col_id);
      *value = c;
      //TODO: remove prepared read version
      r->rver_[col_id] = commit_ts_;
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

  auto r = dynamic_cast<mdb::VersionedRow*>(row);
  verify(r->rtti() == symbol_t::ROW_VERSIONED);

  verify(!read_only_);
  if (phase_ == PHASE_RCC_DISPATCH) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_INSTANT) {

      int64_t t_pr = r->max_prepared_rver(col_id);
      int64_t t_cr = r->rver_[col_id];
      int64_t t_low = received_prepared_ts_left_;

      int64_t t_left = t_pr > t_cr ? t_pr : t_cr;
      t_left = t_left > t_low ? t_left : t_low;

      Log_info("Write Column, Prepare phase: table = %s, col_id = %d,  hint_flag = %d, t_pr = %d, t_cr = %d, t_low = %d, t_left = %d",
               row->get_table()->Name().c_str(),
               col_id,
               hint_flag,
               t_pr,
               t_cr,
               t_low,
               t_left);
      prepared_read_ranges_[row][col_id] = pair<int64_t, int64_t>(t_left, received_prepared_ts_right_);
      mdb_txn()->write_column(row, col_id, value);

    }
    if (hint_flag == TXN_INSTANT || hint_flag == TXN_DEFERRED) {
      int64_t t_pr = r->max_prepared_rver(col_id);
      int64_t t_cr = r->rver_[col_id];
      int64_t t_low = received_prepared_ts_left_;

      int64_t t_left = t_pr > t_cr ? t_pr : t_cr;
      t_left = t_left > t_low ? t_left : t_low;

      Log_info("Write Column, Prepare phase: table = %s, col_id = %d,  hint_flag = %d, t_pr = %d, t_cr = %d, t_low = %d, t_left = %d",
               row->get_table()->Name().c_str(),
               col_id,
               hint_flag,
               t_pr,
               t_cr,
               t_low,
               t_left);
      prepared_read_ranges_[row][col_id] = pair<int64_t, int64_t>(t_left, received_prepared_ts_right_);
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      Log_info("ReadColumn, commit phase: table = %s, col_id = %d,  hint_flag = %d, commit_ts = %d,",
               row->get_table()->Name().c_str(),
               col_id,
               hint_flag,
               commit_ts_);
      r->wver_[col_id] = commit_ts_;
      //TODO: remove prepared ts for GC
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
