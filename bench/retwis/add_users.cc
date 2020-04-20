#include "chopper.h"
#include "piece.h"
#include "generator.h"

namespace rococo {

static uint32_t TXN_TYPE = RETWIS_ADD_USERS;

void RetwisTxn::AddUsersInit(TxnRequest &req) {
  AddUsersRetry();
}

void RetwisTxn::AddUsersRetry() {
  status_[RETWIS_ADDUSERS_0] = DISPATCHABLE;
  status_[RETWIS_ADDUSERS_1] = DISPATCHABLE;
  status_[RETWIS_ADDUSERS_2] = DISPATCHABLE;
  n_pieces_all_ = 3;
  n_pieces_dispatchable_ =  3;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;
}

void RetwisPiece::RegAddUsers() {
    INPUT_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_0,
              RETWIS_VAR_ADD_USERS_1)
    SHARD_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_0,
              RETWIS_TB, TPCC_VAR_H_KEY)
    BEGIN_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_0, DF_REAL) {
      mdb::MultiBlob buf(1);
      Value result(0);
      buf[0] = cmd.input[RETWIS_VAR_ADD_USERS_1].get_blob();
      auto tbl = dtxn->GetTable(RETWIS_TB);
      auto row = dtxn->Query(tbl, buf);
      dtxn->ReadColumn(row, 1, &result, TXN_BYPASS);
      output[0] = result;
      result.set_i32(result.get_i32()+1);
      dtxn->WriteColumn(row, 1, result, TXN_DEFERRED);
      *res = SUCCESS;
      return;
    } END_PIE


    INPUT_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_1,
              RETWIS_VAR_ADD_USERS_2)
    SHARD_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_1,
              RETWIS_TB, TPCC_VAR_H_KEY)
    BEGIN_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_1, DF_NO) {
      mdb::MultiBlob buf(1);
      Value result(0);
      buf[0] = cmd.input[RETWIS_VAR_ADD_USERS_2].get_blob();
      auto tbl = dtxn->GetTable(RETWIS_TB);
      auto row = dtxn->Query(tbl, buf);
      dtxn->ReadColumn(row, 1, &result, TXN_BYPASS);
      output[1] = result;
      *res = SUCCESS;
      return;
    } END_PIE

    INPUT_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_2,
              RETWIS_VAR_ADD_USERS_3)
    SHARD_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_2,
              RETWIS_TB, TPCC_VAR_H_KEY)
    BEGIN_PIE(RETWIS_ADD_USERS, RETWIS_ADD_USERS_2, DF_NO) {
      mdb::MultiBlob buf(1);
      Value result(0);
      buf[0] = cmd.input[RETWIS_VAR_ADD_USERS_3].get_blob();
      auto tbl = dtxn->GetTable(RETWIS_TB);
      auto row = dtxn->Query(tbl, buf);
      dtxn->ReadColumn(row, 1, &result, TXN_BYPASS);
      output[2] = result;
      *res = SUCCESS;
      return;
    } END_PIE

  }

}
} // namespace rococo
