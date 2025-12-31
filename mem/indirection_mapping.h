#ifndef TSDB_INDIRECTION_MAPPING_H
#define TSDB_INDIRECTION_MAPPING_H

#pragma once

#include <atomic>
#include <vector>

#include "disk/log_manager.h"
#include "leveldb/env.h"

#define PRESERVED_BLOCKS 65536
#define METRIC_NUM 512
#define MAX_METRIC_NUM 512 // 1 << 9
#define SOURCE_NUM 65536  // 1 << 16
#define SOURCE_BIT 16

namespace tsdb {
namespace mem {

class IndirectionMapping {
 private:
  std::vector<std::vector<std::atomic<uint64_t>>> slots_; // slots[metric_id][source_group_id]

 public:
  IndirectionMapping();

  ~IndirectionMapping();

  inline auto size() -> size_t {return slots_.size();}

  void alloc_slot(uint64_t sgid, uint64_t mid);
  void set_slot(uint64_t sgid, uint64_t mid, uint64_t val);
  bool cas_slot(uint64_t sgid, uint64_t mid, uint64_t old_val, uint64_t new_val);
  uint64_t read_slot(uint64_t sgid, uint64_t mid);

  void iter(void (*cb)(uint64_t v));
  std::vector<std::pair<uint64_t, uint64_t>> get_ids();

  void print_imap();
  void print_imap(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num);

  leveldb::Status snapshot(const std::string& dir);
  leveldb::Status recover_from_snapshot(const std::string& dir);
};

}  // namespace mem
}  // namespace tsdb

#endif  // TSDB_INDIRECTION_MAPPING_H
