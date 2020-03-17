#pragma once

#include "deptran/janus/tx.h"
#include "../command.h"

namespace janus {

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

};

} // namespace janus
