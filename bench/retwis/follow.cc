#include "chopper.h"
#include "piece.h"
#include "generator.h"
#include "../tpcc/piece.h"
namespace rococo {

static uint32_t TXN_TYPE = RETWIS_FOLLOW;

void RetwisTxn::FollowInit(TxnRequest &req) {
  FollowRetry();
}

void RetwisTxn::FollowRetry() {
  status_[RETWIS_FOLLOW_0] = DISPATCHABLE;
  status_[RETWIS_FOLLOW_1] = DISPATCHABLE;
  n_pieces_all_ = 2;
  n_pieces_dispatchable_ =  2;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;

}

void RetwisPiece::RegFollow() {
    INPUT_PIE(RETWIS_FOLLOW, RETWIS_FOLLOW_0,
              RETWIS_VAR_FOLLOW_1)
    SHARD_PIE(RETWIS_FOLLOW, RETWIS_FOLLOW_0,
              RETWIS_TB, TPCC_VAR_H_KEY)
    BEGIN_PIE(RETWIS_FOLLOW, RETWIS_FOLLOW_0, DF_REAL) {
      mdb::MultiBlob buf(1);
      Value result(0);
      buf[0] = cmd.input[RETWIS_VAR_FOLLOW_1].get_blob();
      auto tbl = dtxn->GetTable(RETWIS_TB);
      auto row = dtxn->Query(tbl, buf);
      dtxn->ReadColumn(row, 1, &result, TXN_BYPASS);
      output[0] = result;
      result.set_i32(result.get_i32()+1);
      dtxn->WriteColumn(row, 1, result, TXN_DEFERRED);
      *res = SUCCESS;
      return;
    } END_PIE

    INPUT_PIE(RETWIS_FOLLOW, RETWIS_FOLLOW_1,
              RETWIS_VAR_FOLLOW_2)
    SHARD_PIE(RETWIS_FOLLOW, RETWIS_FOLLOW_1,
              RETWIS_TB, TPCC_VAR_H_KEY)
    BEGIN_PIE(RETWIS_FOLLOW, RETWIS_FOLLOW_1, DF_REAL) {
      mdb::MultiBlob buf(1);
      Value result(0);
      buf[0] = cmd.input[RETWIS_VAR_FOLLOW_2].get_blob();
      auto tbl = dtxn->GetTable(RETWIS_TB);
      auto row = dtxn->Query(tbl, buf);
      dtxn->ReadColumn(row, 1, &result, TXN_BYPASS);
      output[1] = result;
      result.set_i32(result.get_i32()+1);
      dtxn->WriteColumn(row, 1, result, TXN_DEFERRED);
      *res = SUCCESS;
      return;
    } END_PIE

}
} // namespace rococo
