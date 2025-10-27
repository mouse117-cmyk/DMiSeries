#ifndef TSDB_INDIRECTION_MAPPING_MANAGEMENT_H
#define TSDB_INDIRECTION_MAPPING_MANAGEMENT_H

#pragma once
#include <vector>
#include "indirection_mapping.h"

namespace tsdb::mem{

class IndirectionMappingManagement{
 private:
  std::vector<IndirectionMapping*> imap_mng_;
  disk::SpinLock lock_;

 public:
  IndirectionMappingManagement();

  ~IndirectionMappingManagement();

  inline size_t size() {return imap_mng_.size();}
  inline size_t metric_num(int i) {return imap_mng_[i]->size();}
  inline IndirectionMapping* get_imap(int i) {return imap_mng_[i];}

  void alloc_slot(uint64_t sgid, uint64_t mid);
  void set_slot(uint64_t sgid, uint64_t mid, uint64_t val);
  bool cas_slot(uint64_t sgid, uint64_t mid, uint64_t old_val, uint64_t new_val);
  uint64_t read_slot(uint64_t sgid, uint64_t mid);

  void iter(void (*cb)(uint64_t v));
  std::vector<std::pair<uint64_t, uint64_t>> get_ids();
  std::vector<std::vector<std::pair<uint64_t,  uint64_t>>> imap_id_arr();

  void print_imap_mng();
  void print_imap_mng(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num);

//  leveldb::Status snapshot(const std::string& dir);
//  leveldb::Status recover_from_snapshot(const std::string& dir);
};

}

#endif  // TSDB_INDIRECTION_MAPPING_MANAGEMENT_H
