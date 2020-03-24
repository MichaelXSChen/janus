//
// Created by micha on 2020/3/23.
//


#include "tx.h"

namespace rococo {

char * defer_str[] = {"defer_real", "defer_no", "defer_fake"};
char * hint_str[] = {"n/a", "bypass", "safe", "instant", "deferred"};
//SimpleCommand is a typedef of TxnPieceData
//add a simpleCommand to the local Tx's dreq
void TxChronos::DispatchExecute(const SimpleCommand &cmd,
                                int32_t *res,
                                map<int32_t, Value> *output) {

  phase_ = PHASE_CHRONOS_DISPATCH;

  //xs: Step 1: skip this simpleCommand if it is already in the dreqs.
  for (auto& c: dreqs_) {
    if (c.inn_id() == cmd.inn_id()) // already handled?
      return;
  }
  verify(txn_reg_);
  // execute the IR actions.
  auto pair = txn_reg_->get(cmd);
  // To tolerate deprecated codes

  Log_info("%s called, defer= %s" , __FUNCTION__, defer_str[pair.defer]);
  int xxx, *yyy;
  if (pair.defer == DF_REAL) {
    yyy = &xxx;
    dreqs_.push_back(cmd);
  } else if (pair.defer == DF_NO) {
    yyy = &xxx;
  } else if (pair.defer == DF_FAKE) {
    dreqs_.push_back(cmd);
    return;
//    verify(0);
  } else {
    verify(0);
  }
  pair.txn_handler(nullptr,
                   this,
                   const_cast<SimpleCommand&>(cmd),
                   yyy,
                   *output);
  *res = pair.defer;
}


bool TxChronos::ReadColumn(mdb::Row *row,
                           mdb::column_id_t col_id,
                           Value *value,
                           int hint_flag) {

  //Log_info("Rtti = %d", row->rtti());
  auto r = dynamic_cast<mdb::VersionedRow*>(row);
  verify(r->rtti() == symbol_t::ROW_VERSIONED);


  verify(!read_only_);
  if (phase_ == PHASE_CHRONOS_DISPATCH|| phase_ == PHASE_CHRONOS_PREPARE) {
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

      Log_info("[txn %d] ReadColumn, Prepare phase1: table = %s, col_id = %d,  hint_flag = %s, t_pw = %d, t_cw = %d, t_low = %d, t_left = %d, t_right = %d",
                this->id(),
               row->get_table()->Name().c_str(),
               col_id,
               hint_str[hint_flag],
               t_pw,
               t_cw,
               t_low,
               t_left,
               received_prepared_ts_right_);
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

      Log_info("[txn %d] ReadColumn, Prepare phase2: table = %s, col_id = %d,  hint_flag = %s, t_pw = %d, t_cw = %d, t_low = %d, t_left = %d, t_right = %d",
                this->id(),
          row->get_table()->Name().c_str(),
               col_id,
               hint_str[hint_flag],
               t_pw,
               t_cw,
               t_low,
               t_left,
               received_prepared_ts_right_);
      prepared_read_ranges_[row][col_id] = pair<int64_t, int64_t>(t_left, received_prepared_ts_right_);
      return true;
    }
  } else if (phase_ == PHASE_CHRONOS_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      //For commit

      Log_info("[txn %d] ReadColumn, commit phase: table = %s, col_id = %d,  hint_flag = %s, commit_ts = %d,",
                id(),
          row->get_table()->Name().c_str(),
               col_id,
               hint_str[hint_flag],
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
                            column_id_t col_id,
                            const Value &value,
                            int hint_flag) {

  auto r = dynamic_cast<mdb::VersionedRow*>(row);
  verify(r->rtti() == symbol_t::ROW_VERSIONED);

  verify(!read_only_);
  if (phase_ == PHASE_CHRONOS_DISPATCH || phase_ == PHASE_CHRONOS_PREPARE) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_INSTANT) {

      int64_t t_pr = r->max_prepared_rver(col_id);
      int64_t t_cr = r->rver_[col_id];
      int64_t t_low = received_prepared_ts_left_;

      int64_t t_left = t_pr > t_cr ? t_pr : t_cr;
      t_left = t_left > t_low ? t_left : t_low;

      Log_info("[txn %d] Write Column, Prepare phase1: table = %s, col_id = %d,  hint_flag = %s, t_pr = %d, t_cr = %d, t_low = %d, t_left = %d, t_right = %d",
                id(),
          row->get_table()->Name().c_str(),
               col_id,
               hint_str[hint_flag],
               t_pr,
               t_cr,
               t_low,
               t_left,
               received_prepared_ts_right_);
      prepared_read_ranges_[row][col_id] = pair<int64_t, int64_t>(t_left, received_prepared_ts_right_);
      mdb_txn()->write_column(row, col_id, value);

    }
    if (hint_flag == TXN_INSTANT || hint_flag == TXN_DEFERRED) {
      int64_t t_pr = r->max_prepared_rver(col_id);
      int64_t t_cr = r->rver_[col_id];
      int64_t t_low = received_prepared_ts_left_;

      int64_t t_left = t_pr > t_cr ? t_pr : t_cr;
      t_left = t_left > t_low ? t_left : t_low;

      Log_info("[txn %d] Write Column, Prepare phase2: table = %s, col_id = %d,  hint_flag = %s, t_pr = %d, t_cr = %d, t_low = %d, t_left = %d, t_right = %d",
              id(),
          row->get_table()->Name().c_str(),
               col_id,
               hint_str[hint_flag],
               t_pr,
               t_cr,
               t_low,
               t_left,
               received_prepared_ts_right_);
      prepared_read_ranges_[row][col_id] = pair<int64_t, int64_t>(t_left, received_prepared_ts_right_);
    }
  } else if (phase_ == PHASE_CHRONOS_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      Log_info("[txn %d] Write Column, commit phase: table = %s, col_id = %d,  hint_flag = %s, commit_ts = %d,",
              id(),
          row->get_table()->Name().c_str(),
               col_id,
               hint_str[hint_flag],
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



void TxChronos::CommitExecute() {
//  verify(phase_ == PHASE_RCC_START);
  phase_ = PHASE_CHRONOS_COMMIT;
  TxnWorkspace ws;
  for (auto &cmd: dreqs_) {
    auto pair = txn_reg_->get(cmd);
    int tmp;
    cmd.input.Aggregate(ws);
    auto& m = output_[cmd.inn_id_];
    pair.txn_handler(nullptr, this, cmd, &tmp, m);
    ws.insert(m);
  }
  committed_ = true;
}

bool TxChronos::GetTsBound(int64_t &left, int64_t &right) {

  left = received_prepared_ts_left_;
  right = received_prepared_ts_right_;

  for (auto &pair: prepared_read_ranges_){
    for (auto &col_range: pair.second){
      if (col_range.second.first > left){
        left = col_range.second.first;
      }
      if (col_range.second.second < right){
        right = col_range.second.second;
      }
    }
  }

  for (auto &pair: prepared_write_ranges_){
    for (auto &col_range: pair.second){
      if (col_range.second.first > left){
        left = col_range.second.first;
      }
      if (col_range.second.second < right){
        right = col_range.second.second;
      }
    }
  }
}


} // namespace janus
