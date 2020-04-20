#include "piece.h"
#include <limits>

namespace rococo {

using rococo::RccDTxn;

char RETWIS_TB[] =  "history";



void RetwisPiece::reg_all() {
    RegAddUsers();
    RegFollow();
    RegPostTweet();
    RegGetTimeline();
}

RetwisPiece::RetwisPiece() {}

RetwisPiece::~RetwisPiece() {}

}
