#pragma once

#include "deptran/janus/tx.h"
#include "../command.h"

namespace janus {

class TxChronos : public TxJanus {
public:
  using TxJanus::TxJanus;

  void DispatchExecute(SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

};

} // namespace janus
