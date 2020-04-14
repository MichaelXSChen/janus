//
// Created by micha on 2020/4/7.
//

#ifndef ROCOCO_DEPTRAN_OCEAN_VISTA_OV_GOSSIPER_H_
#define ROCOCO_DEPTRAN_OCEAN_VISTA_OV_GOSSIPER_H_
#include "deptran/config.h"

namespace rococo {
class OVGossiper {
 public:
  OVGossiper(Config *config, Config::SiteInfo *info) {
    config_ = config;
    site_info_ = info;
    Log_info("OVGossiper Created");
  }

  Config *config_;
  Config::SiteInfo* site_info_;
};
}
#endif //ROCOCO_DEPTRAN_OCEAN_VISTA_OV_GOSSIPER_H_
