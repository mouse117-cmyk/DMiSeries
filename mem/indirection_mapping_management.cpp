#include "indirection_mapping_management.h"

#include <iostream>

namespace tsdb::mem{
  IndirectionMappingManagement::IndirectionMappingManagement() : imap_mng_{new IndirectionMapping()}{}

  IndirectionMappingManagement::~IndirectionMappingManagement() {
    for (auto & it : imap_mng_) {
      delete it;
    }
  }

  void IndirectionMappingManagement::alloc_slot(uint64_t sgid, uint64_t mid) {
    lock_.lock();
    while (sgid >= SOURCE_NUM*imap_mng_.size()) {
      auto* new_imap = new IndirectionMapping();
      imap_mng_.emplace_back(new_imap);
    }
    if (mid >= imap_mng_[sgid>>SOURCE_BIT]->size()) {
      imap_mng_[sgid>>SOURCE_BIT]->alloc_slot(sgid%SOURCE_NUM, mid);
    }
    lock_.unlock();
  }

  void IndirectionMappingManagement::set_slot(uint64_t sgid, uint64_t mid, uint64_t val) {
    imap_mng_[sgid>>SOURCE_BIT]->set_slot(sgid%SOURCE_NUM, mid, val);
  }

  bool IndirectionMappingManagement::cas_slot(uint64_t sgid, uint64_t mid, uint64_t old_val, uint64_t new_val) {
    if (sgid >= SOURCE_NUM*imap_mng_.size()) {
      return false;
    }
    return imap_mng_[sgid>>SOURCE_BIT]->cas_slot(sgid%SOURCE_NUM, mid, old_val, new_val);
  }

  uint64_t IndirectionMappingManagement::read_slot(uint64_t sgid, uint64_t mid) {
    if (sgid >= SOURCE_NUM*imap_mng_.size()) {
      return std::numeric_limits<uint64_t>::max();
    }
    return imap_mng_[sgid>>SOURCE_BIT]->read_slot(sgid%SOURCE_NUM, mid);
  }

  void IndirectionMappingManagement::iter(void (*cb)(uint64_t)) {
    for (auto & it : imap_mng_) {
      it->iter(cb);
    }
  }

  std::vector<std::pair<uint64_t, uint64_t>> IndirectionMappingManagement::get_ids() {
    std::vector<std::pair<uint64_t, uint64_t>> ids;
    for (auto & it : imap_mng_) {
      auto tmp = it->get_ids();
      ids.insert(ids.end(), tmp.begin(), tmp.end());
    }
    return ids;
  }

  std::vector<std::vector<std::pair<uint64_t , uint64_t>>> IndirectionMappingManagement::imap_id_arr() {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> id_arr;
    for (auto & it : imap_mng_) {
      auto tmp = it->get_ids();
      id_arr.emplace_back(std::move(tmp));
    }

    return id_arr;
  }

  void IndirectionMappingManagement::print_imap_mng() {
    std::cout<<"map size: "<<imap_mng_.size()<<std::endl;
    for (uint64_t i = 0; i < imap_mng_.size(); i++) {
      std::cout<<"**************** "<<" imap "<<i<<" ****************"<<std::endl;
      std::cout<<"******************* "<<"Metric num: "<<imap_mng_[i]->size()<<" *****************"<<std::endl;

      auto ids = imap_mng_[i]->get_ids();
      for(auto & it : ids) {
        if (it.second == 0) {
          continue;
        }
        std::cout<<"i: "<<it.first<<"\t\tval: "<<it.second<<std::endl;
      }

      std::cout<<"OK"<<std::endl;
    }
  }

  void IndirectionMappingManagement::print_imap_mng(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num) {
    for (uint64_t i = sg_start>>SOURCE_BIT; i < ((sg_start+sg_num)>>SOURCE_BIT)+1; i++) {
      std::cout<<"**************** "<<" imap "<<i<<"  ****************"<<std::endl;
      imap_mng_[i]->print_imap(sg_start%SOURCE_NUM, sg_num, metric_start, metric_num);
    }
  }

}