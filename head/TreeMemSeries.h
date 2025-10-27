#ifndef TSDB_UNITTEST_TREEMEMSERIES_H
#define TSDB_UNITTEST_TREEMEMSERIES_H
#include <atomic>

#include "base/Atomic.hpp"
#include "base/Mutex.hpp"
#include "chunk/XORChunk.hpp"
#include "db/DBUtils.hpp"
#include "disk/log_manager.h"
#include "head/HeadUtils.hpp"
#include "label/Label.hpp"
#include "TreeSeries/slab_management.h"
#include "TreeSeries/TreeSeries.h"

namespace leveldb {
class DB;
class Status;
}  // namespace leveldb

namespace tsdb {
namespace head {

extern int MEM_TUPLE_SIZE;

// Current assumption: data are in-order.
class TreeMemSeries {
 public:
  std::mutex mutex_;
  uint64_t log_pos;
  label::Labels labels;
  int64_t min_time_;
  int64_t max_time_;
  int64_t global_max_time_;
  int num_samples_;
  int64_t tuple_st_;
  chunk::XORChunk chunk_;
  std::unique_ptr<chunk::ChunkAppenderInterface> appender;

  std::atomic<int64_t> flushed_txn_; // log
  std::atomic<int64_t> sample_txn_; // chunk
  int64_t log_clean_txn_;  // used to store the temporary max txn when cleaning
                           // logs.

  std::string key_;
  std::string val_;

  std::string l1_key_;
  std::string l1_val_;

  uint32_t sid_;
  uint8_t item_idx_;
  uint64_t source_id_;
  uint16_t metric_id_;

  int64_t head_flush_time_; // larger timestamps are in head_chunk
  int64_t level_flush_time_; // larger timestamps are in TreeSeries, smaller timestamps are in lsm-tree

  uint64_t chunk_num_;

  // Used for GC.
  std::atomic<uint64_t> access_epoch;
  std::atomic<uint32_t> version;

  int64_t time_boundary_;

  TreeMemSeries();
  TreeMemSeries(uint64_t sgid, uint16_t mid, const label::Labels& labels, uint32_t sid, uint64_t log_pos = 0);

  void update_access_epoch();

  int64_t min_time();
  int64_t max_time();

  int64_t head_flush_time() { return head_flush_time_; }
  int64_t level_flush_time() { return level_flush_time_; }

  inline uint64_t encodeSMid(uint64_t sgid, uint16_t mid) {return (sgid << 9) | mid;}
  inline std::pair<uint64_t , uint16_t> decodeSMid(uint64_t logical_id) {return std::make_pair(logical_id >> 9, logical_id & 0x1ff);}

  leveldb::Status _flush_mirror(slab::TreeSeries* tree_series, int64_t txn);

  leveldb::Status _flush_slab(slab::SlabManagement* slab_m, int64_t txn);
  leveldb::Status _flush_tree(slab::TreeSeries* tree_series, int64_t txn);
  leveldb::Status _flush(leveldb::DB* db, int64_t txn);

  bool append(leveldb::DB* db, int64_t timestamp, double value,
              int64_t txn = 0);

  bool append_slab(slab::SlabManagement* slab_m, int64_t timestamp, double value,
                   int64_t txn = 0);

  bool append(slab::TreeSeries* tree_series, int64_t timestamp, double value,
              int64_t txn = 0);

  void lock() { mutex_.lock(); }
  void unlock() { mutex_.unlock(); }

  void release_labels() { label::Labels().swap(labels); }
};

}  // namespace head
}  // namespace tsdb
#endif  // TSDB_UNITTEST_TREEMEMSERIES_H
