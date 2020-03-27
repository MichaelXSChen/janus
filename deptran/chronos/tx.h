//
// Created by micha on 2020/3/23.
//
#pragma once

#include "deptran/rcc/dtxn.h"
#include "../command.h"
#include "memdb/row_mv.h"
namespace rococo {

#define PHASE_CHRONOS_DISPATCH (1)
#define PHASE_CHRONOS_PREPARE (2)
#define PHASE_CHRONOS_COMMIT (3)


class TxChronos : public RccDTxn {
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

  std::set<ChronosRow *> locked_rows_ = {};


  int64_t received_prepared_ts_left_ = 0;
  int64_t received_prepared_ts_right_ = 0;

  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_read_ranges_ = {};
  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_write_ranges_ = {};

  int64_t local_prepared_ts_left_;
  int64_t local_prepared_ts_right_;

  int64_t commit_ts_;


};

} // namespace janus



