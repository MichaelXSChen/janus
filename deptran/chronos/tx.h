//
// Created by micha on 2020/3/23.
//
#pragma once

#include "deptran/rcc/dtxn.h"
#include "../command.h"

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
    return VersionedRow::create(schema, values);
  }

  void DispatchExecute(const SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

  virtual void CommitExecute() override;

  bool ReadColumn(mdb::Row *row,
                  mdb::column_id_t col_id,
                  Value *value,
                  int hint_flag = TXN_INSTANT) override;

  bool WriteColumn(Row *row,
                   column_id_t col_id,
                   const Value &value,
                   int hint_flag = TXN_INSTANT) override;


  bool GetTsBound(int64_t &left, int64_t &right);


  int64_t local_ts_left_ = 0;
  int64_t local_ts_right_ = 0;

  int64_t received_prepared_ts_left_ = 0;
  int64_t received_prepared_ts_right_ = 0;

  map<Row *, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_read_ranges_ = {};
  map<Row *, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_write_ranges_ = {};


  int64_t commit_ts_;


};

} // namespace janus


