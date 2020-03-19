#pragma once

#include "deptran/janus/tx.h"
#include "../command.h"

namespace janus {

#define PHASE_CHRONOS_DISPATCH (1)
#define PHASE_CHRONOS_PREPARE (2)
#define PHASE_CHRONOS_COMMIT (3)


class TxChronos : public TxJanus {
public:
  using TxJanus::TxJanus;


  virtual mdb::Row *CreateRow(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values) override {
    Log_info("[[%s]] called", __PRETTY_FUNCTION__);
    return VersionedRow::create(schema, values);
  }

  void DispatchExecute(SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

  virtual void CommitExecute() override;

  bool ReadColumn(mdb::Row *row,
                          mdb::colid_t col_id,
                          Value *value,
                          int hint_flag = TXN_INSTANT) override;

  bool WriteColumn(Row *row,
                           colid_t col_id,
                           const Value &value,
                           int hint_flag = TXN_INSTANT) override;

  int64_t local_ts_left_ = 0;
  int64_t local_ts_right_ = 0;

  int64_t received_prepared_ts_left_ = 0;
  int64_t received_prepared_ts_right_ = 0;

  map<Row *, map<colid_t, pair<mdb::version_t, mdb::version_t>>> prepared_read_ranges_ = {};
  map<Row *, map<colid_t, pair<mdb::version_t, mdb::version_t>>> prepared_write_ranges_ = {};


  int64_t commit_ts_;


};

} // namespace janus
