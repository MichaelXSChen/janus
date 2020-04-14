//
// Created by micha on 2020/4/7.
//

#ifndef ROCOCO_DEPTRAN_OCEAN_VISTA_OV_GOSSIPER_H_
#define ROCOCO_DEPTRAN_OCEAN_VISTA_OV_GOSSIPER_H_
#include "deptran/config.h"
#include "ov-txn_mgr.h"


namespace rococo {
class ClassicProxy;
class OVGossiper {
 public:
  OVGossiper(Config *config, Config::SiteInfo *info);


  Config *config_;
  Config::SiteInfo* site_info_;
  std::map<siteid_t, ClassicProxy*> peer_gossipers_;
  std::map<std::string, std::string> peer_gossiper_dc_site_map_;

  std::map<std::string, siteid_t> peer_gossiper_dc_siteid_map_;

  std::map<std::string, ov_ts_t> dc_watermarks_; //watermark of each dc.
  std::map<siteid_t, ov_ts_t> site_watermarks_;  //sitewater mark of sites in my dc.
};
}
#endif //ROCOCO_DEPTRAN_OCEAN_VISTA_OV_GOSSIPER_H_
