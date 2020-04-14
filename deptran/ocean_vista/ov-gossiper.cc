//
// Created by micha on 2020/4/7.
//

#include "ov-gossiper.h"

namespace rococo{

OVGossiper::OVGossiper(rococo::Config *config, rococo::Config::SiteInfo *info) {
  config_ = config;
  site_info_ = info;
  Log_info("OVGossiper Created");


  //Get the proxy to peers;
  std::map<std::string, std::set<std::string>> sites_by_dc_;
  for (auto &s: config_->sites_){
    std::string site = s.name;
    std::string proc = config_->site_proc_map_[site];
    std::string host = config_->proc_host_map_[proc];
    std::string dc = config_->host_dc_map_[host];
    if (sites_by_dc_.count(dc) == 0){
      std::set<std::string> sites;
      sites.insert(site);
      sites_by_dc_[dc] = sites;
    }
    else{
      sites_by_dc_[dc].insert(site);
    }
  }
  /*
   * xs TODO: why don't I change the mapping between site and proc to use site_id instead of name
   * not emergent, as these happens during init time.
  */
  for (auto &dc_s: sites_by_dc_){
    ov_ts_t ovts;
    dc_watermarks_[dc_s.first] = ovts;
    Log_info("Init dc watermark for dc [%s] to zero", dc_s.first.c_str());


    if (dc_s.first != info->dcname){
      std::string gossiper_site_name = *(dc_s.second.begin());
      peer_gossiper_dc_site_map_[dc_s.first] = gossiper_site_name;
      int index;
      for (index = 0; index < config_->sites_.size(); index++){
        if (config->sites_[index].name == gossiper_site_name){
          break;
        }
      }
      verify(index != config_->sites_.size());
      peer_gossiper_dc_siteid_map_[dc_s.first] = (siteid_t) index;
      Log_info("Gossiper found peer [%s, id = %d] at dc [%s]", gossiper_site_name.c_str(), index, dc_s.first.c_str());
    }
    else{
      //sites of my dc;
      for (auto& site : dc_s.second){
        int index;
        for (index = 0; index < config_->sites_.size(); index++){
          if (config->sites_[index].name == site){
            break;
          }
        }
        ov_ts_t ovts;
        site_watermarks_[(siteid_t)index] = ovts;
        Log_info("Init site water mark for site_id %d to zero", index);
      }
    }
  }
}


}//namespace rococo
