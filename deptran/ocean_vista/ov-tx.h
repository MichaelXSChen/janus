//
// Created by micha on 2020/3/23.
//
#pragma once

#include "deptran/rcc/dtxn.h"
#include "../command.h"
#include "memdb/row_mv.h"
#include "ov-txn_mgr.h"
namespace rococo {

#define PHASE_CHRONOS_DISPATCH (1)
#define PHASE_CHRONOS_PRE_ACCEPT (2)
#define PHASE_CHRONOS_COMMIT (3)


class TxOV : public RccDTxn {
 public:
  using RccDTxn::RccDTxn;


  virtual mdb::Row *CreateRow(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values) override {
    Log_info("[[%s]] called", __PRETTY_FUNCTION__);
    return ChronosRow::create(schema, values);
  }

  void DispatchExecute(const SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

  void PreAcceptExecute(const SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output);

  void CommitExecute() override;

  bool ReadColumn(mdb::Row *row,
                  mdb::column_id_t col_id,
                  Value *value,
                  int hint_flag = TXN_INSTANT) override;

  bool WriteColumn(Row *row,
                   column_id_t col_id,
                   const Value &value,
                   int hint_flag = TXN_INSTANT) override;


  bool GetTsBound();
  bool StorePreparedVers();
  bool RemovePreparedVers();
  bool GetDispatchTsHint(int64_t &ts_left, int64_t &ts_right);

  std::set<ChronosRow *> locked_rows_ = {};

  /**
   * Should deprecated
   */
  int64_t received_prepared_ts_left_ = 0;
  int64_t received_prepared_ts_right_ = 0;

  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_read_ranges_ = {};
  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_write_ranges_ = {};



  int64_t received_dispatch_ts_left_ = 0;
  int64_t received_dispatch_ts_right_ = 0;
  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> dispatch_ranges_ = {};

  int64_t local_prepared_ts_left_;
  int64_t local_prepared_ts_right_;

  int64_t commit_ts_;

  /**
   * Should deprecated end here
   */

  ov_ts_t ovts_;
  std::function<void()> executed_callback = [](){
    Log_fatal("call back not assigned");
    verify(0);
  };

  cmdtype_t root_type = 0;

  enum OV_txn_status {
    INIT = 0,
    STORED = 1,
    CAN_EXECUTE = 2,
    EXECUTED = 3
  };

  OV_txn_status ov_status_ = OV_txn_status::INIT;
};

} // namespace janus



