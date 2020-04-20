

#include "deptran/__dep__.h"
#include "deptran/txn_chopper.h"
#include "chopper.h"
#include "piece.h"

namespace deptran {

void RetwisTxn::Init(TxnRequest &req) {
  ws_init_ = req.input_;
  ws_ = req.input_;
  type_ = req.txn_type_;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;
  commit_.store(true);
  switch (req.txn_type_) {
    case RETWIS_ADD_USERS:
        AddUsersInit(req);
        break;
    case RETWIS_FOLLOW:
        FollowInit(req);
        break;
    case RETWIS_POST_TWEET:
        PostTweetInit(req);
        break;
     case RETWIS_GET_TIMELINE:
         GetTimelineInit(req);
         break;
    default:
      verify(0);
  }
}
    :
bool RetwisTxn::start_callback(const std:vector<int> &pi,
                               int res, BatchStartArgsHelper &bsah) {
  return false;
}

bool RetwisTxn::start_callback(int pi,
                               int res,
                               map<int32_t, Value> &output) {
  return false;
}

bool RetwisTxn::IsReadOnly() {
  if (type_ == RETWIS_ADD_USERS)
    return false;
  else if (type_ == RETWIS_FOLLOW)
    return false;
  else if (type_ == RETWIS_POST_TWEET)
      return false;
  else if (type_ == RETWIS_GET_TIMELINE)
      return true;
  else
    verify(0);
}

void RetwisTxn::Reset() {
    TxnCommand::Reset();
    ws_ = ws_init_;
    partition_ids_.clear();
    n_try_++;
    commit_.store(true);
    n_pieces_dispatchable_ = 0;
    n_pieces_dispatch_acked_ = 0;
    n_pieces_dispatched_ = 0;
    switch (type_) {
        case RETWIS_ADD_USERS:
            AddUsersRetry();
            break;
        case RETWIS_FOLLOW:
            FollowRetry();
            break;
        case RETWIS_POST_TWEET:
            PostTweetRetry();
            break;
        case RETWIS_GET_TIMELINE:
            GetTimelineRetry();
            break;
        default:
            verify(0);
    }
}


parid_t RetwisTxn::GetPiecePartitionId(innid_t inn_id) {
    parid_t partition_id = 0;
    auto it = txn_reg_->sharding_input_.find(std::make_pair(type_, inn_id));
    if (it != txn_reg_->sharding_input_.end()) {
        auto &pair = it->second;
        auto tb = pair.first;
        auto& var_ids = pair.second;
        vector<Value> vars;
        for (auto var_id : var_ids) {
            verify(ws_.count(var_id) != 0);
            vars.push_back(ws_.at(var_id));
        }
        MultiValue mv = MultiValue(vars);
        sss_->GetPartition(tb, mv, partition_id);
    } else {
        verify(0);
        partition_id = sharding_[inn_id];
    }
    return partition_id;
}

int RetwisTxn::GetNPieceAll() {
    return n_pieces_all_;
}


RetwisTxn::~RetwisTxn() {
}

}
