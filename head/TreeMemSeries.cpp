#include "head/TreeMemSeries.h"

#include <chrono>
#include <iostream>

#include "base/Logging.hpp"
#include "base/TSDBException.hpp"
#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "db/DBUtils.hpp"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace tsdb {
namespace head {
TreeMemSeries::TreeMemSeries()
    :
      log_pos(0),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      flushed_txn_(0),
      sample_txn_(0),
      log_clean_txn_(-1),
      chunk_(LEVELDB_VALUE_HEADER_SIZE),
      sid_(std::numeric_limits<uint32_t>::max()),
      item_idx_(0),
      source_id_(0),
      metric_id_(0),
      head_flush_time_(0),
      level_flush_time_(0),
      chunk_num_(0),
      appender(chunk_.appender()),
      access_epoch(0),
      version(0),
      time_boundary_(-1) {
    mutex_.unlock();
}

TreeMemSeries::TreeMemSeries(uint64_t sgid, uint16_t mid,const label::Labels& labels, uint32_t sid, uint64_t pos)
    : mutex_(),
      log_pos(pos),
      labels(std::move(labels)),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      flushed_txn_(0),
      sample_txn_(0),
      log_clean_txn_(-1),
      chunk_(LEVELDB_VALUE_HEADER_SIZE),
      sid_(sid),
      item_idx_(0),
      source_id_(sgid),
      metric_id_(mid),
      head_flush_time_(0),
      level_flush_time_(0),
      chunk_num_(0),
      appender(chunk_.appender()),
      access_epoch(0),
      version(0),
      time_boundary_(-1) {
    mutex_.unlock();
}

int64_t TreeMemSeries::min_time() {
  if (min_time_ == std::numeric_limits<int64_t>::max())
    return std::numeric_limits<int64_t>::min();
  return min_time_;
}

int64_t TreeMemSeries::max_time() {
  if (max_time_ == std::numeric_limits<int64_t>::min())
    return std::numeric_limits<int64_t>::max();
  return max_time_;
}

leveldb::Status TreeMemSeries::_flush_mirror(slab::TreeSeries *tree_series, int64_t txn) {

    key_.clear();
    tree_series->EnCodeKey(&key_,source_id_,metric_id_,tuple_st_);

    chunk_.bstream.stream[0] = 1;
    leveldb::EncodeFixed16(
            reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 1), metric_id_);
    leveldb::EncodeFixed64BE(
            reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 3), source_id_);
    leveldb::Status s;

    while(chunk_num_%5==0 && sample_txn_>flushed_txn_) {
    };

    chunk_num_++;

    while(!tree_series->MirrorPutKV(sid_, source_id_, metric_id_,tuple_st_,
                                    max_time_,(uint8_t *)(&chunk_.bstream.stream[0]), slab::CHUNK_SIZE, txn));
//    std::cout<<sid_<<" "<<max_time_<<" "<<int (tree_series->GetMemSlabInfo(sid_)->nalloc_)<<std::endl;

    appender.reset();
    chunk_ = chunk::XORChunk(LEVELDB_VALUE_HEADER_SIZE);
    appender = chunk_.appender();

    num_samples_ = 0;
    return s;
}

leveldb::Status TreeMemSeries::_flush_slab(slab::SlabManagement* slab_m, int64_t txn) {
  key_.clear();
  return leveldb::Status::OK();
}

leveldb::Status TreeMemSeries::_flush_tree(slab::TreeSeries* tree_series, int64_t txn){
//    if (sid_ == std::numeric_limits<uint32_t>::max()) {
//        while(!tree_series->GetMemSlabID(sid_, source_id_, metric_id_)){
//        }
//    }

  key_.clear();
  tree_series->EnCodeKey(&key_,source_id_,metric_id_,tuple_st_);

  chunk_.bstream.stream[0] = 1;
  leveldb::EncodeFixed16(
        reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 1), metric_id_);
  leveldb::EncodeFixed64BE(
        reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 3), source_id_);
  leveldb::Status s;

  while(chunk_num_%5==0 && sample_txn_>flushed_txn_) {
  };

  chunk_num_++;
  while(!tree_series->ConPutKV(sid_, source_id_, metric_id_,tuple_st_,max_time_,(uint8_t *)(&chunk_.bstream.stream[0]), slab::CHUNK_SIZE, txn));
//  while(!flag){
//      tree_series->GetMemSlabID(sid_, source_id_, metric_id_);
//      flag = tree_series->ConPutKV(sid_, source_id_, metric_id_,tuple_st_,max_time_,(uint8_t *)(&chunk_.bstream.stream[0]), slab::CHUNK_SIZE, txn);
//  }
//  if (tree_series->SlabFull(tree_series->GetMemSlabInfo(sid_))) {
//    while(!tree_series->GetMemSlabID(sid_, source_id_, metric_id_)){
//    }
//    sid_ = std::numeric_limits<uint32_t>::max();
//  }
  appender.reset();
  chunk_ = chunk::XORChunk(LEVELDB_VALUE_HEADER_SIZE);
  appender = chunk_.appender();

  num_samples_ = 0;
  return s;
}

leveldb::Status TreeMemSeries::_flush(leveldb::DB* db, int64_t txn) {
  key_.clear();
//  leveldb::encodeKey(&key_, ref, tuple_st_);
//  chunk_.bstream.stream[0] = 1;  // Has header.
//  leveldb::EncodeFixed64BE(
//      reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 1), logical_id);
//  leveldb::EncodeFixed64BE(
//      reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 9), txn);
//  leveldb::Status s;
//  s = db->Put(leveldb::WriteOptions(), key_,
//              leveldb::Slice(reinterpret_cast<char*>(&chunk_.bstream.stream[0]),
//                             chunk_.bstream.stream.size()));
//
//  appender.reset();
//  chunk_ = chunk::XORChunk(LEVELDB_VALUE_HEADER_SIZE);
//  appender = chunk_.appender();
//
//  num_samples_ = 0;
//  min_time_ = std::numeric_limits<int64_t>::max();
//  max_time_ = std::numeric_limits<int64_t>::min();
//  return s;
}

bool TreeMemSeries::append(leveldb::DB* db, int64_t timestamp, double value,
                       int64_t txn) {
  bool flushed = false;
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;

  int64_t time_boundary =
      timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    _flush(db, txn);
    flushed = true;
  }

  max_time_ = timestamp;
  appender->append(timestamp, value);
  ++num_samples_;

  if (num_samples_ == 1) tuple_st_ = timestamp;
  if (num_samples_ == MEM_TUPLE_SIZE) {
    _flush(db, txn);
    flushed = true;
  }
  time_boundary_ = time_boundary;

  return flushed;
}

bool TreeMemSeries::append_slab(slab::SlabManagement* slab_m, int64_t timestamp, double value, int64_t txn) {
  bool flushed = false;

  if (timestamp > global_max_time_) global_max_time_ = timestamp;
  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;
  int64_t time_boundary = timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;

  int prev_end = chunk_.bstream.end;
  uint8_t prev_byte = chunk_.bstream.stream[prev_end];

  appender->append(timestamp, value);
  int cur_end = chunk_.bstream.end;
  if (cur_end == 120 && chunk_.bstream.tail_count >= 6) {
    _flush_slab(slab_m, txn);
    flushed = true;
  }
  else if (cur_end > 120) {
    chunk_.bstream.end = prev_end;
    chunk_.bstream.stream[prev_end] = prev_byte;
    base::put_uint16_big_endian(chunk_.bstream.bytes(), chunk_.num_samples() - 1);
    for(uint8_t i = prev_end+1; i <= cur_end; i++) {
      chunk_.bstream.stream[i] = 0;
    }
    --num_samples_;

    _flush_slab(slab_m, txn);
    flushed = true;

    appender->append(timestamp, value);
  }

  max_time_ = timestamp;
  ++num_samples_;
  time_boundary_ = time_boundary;

  if (num_samples_ == 1) tuple_st_ = timestamp;

  return flushed;
}

//bool TreeMemSeries::append(slab::TreeSeries* tree_series, int64_t timestamp, double value,
//                       int64_t txn){
//  bool flushed = false;
//
//  if (timestamp > global_max_time_) global_max_time_ = timestamp;
//  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;
//  int64_t time_boundary = timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
//
//  int prev_end = chunk_.bstream.end;
//  uint8_t prev_byte = chunk_.bstream.stream[prev_end-1];
//
//  sample_txn_++;
//  appender->append(timestamp, value);
//  int cur_end = chunk_.bstream.end;
//  if (cur_end == slab::CHUNK_SIZE && chunk_.bstream.tail_count >= 6) {
//    _flush_tree(tree_series, sample_txn_-num_samples_+1);
//    flushed = true;
//    head_flush_time_ = timestamp;
//    min_time_ = std::numeric_limits<int64_t>::max();
//    max_time_ = std::numeric_limits<int64_t>::min();
//  }
//  else if (cur_end > slab::CHUNK_SIZE) {
//    chunk_.bstream.end = prev_end;
//    chunk_.bstream.stream[prev_end-1] = prev_byte;
//    base::put_uint16_big_endian(chunk_.bstream.bytes(), chunk_.num_samples() - 1);
//    for(uint16_t i = prev_end; i <= cur_end; i++) {
//      chunk_.bstream.stream[i] = 0;
//    }
//    --num_samples_;
//
//    _flush_tree(tree_series, sample_txn_-num_samples_+1);
//    flushed = true;
//    head_flush_time_ = max_time_;
//    min_time_ = std::numeric_limits<int64_t>::max();
//    max_time_ = std::numeric_limits<int64_t>::min();
//
//    appender->append(timestamp, value);
//  }
//
//  max_time_ = timestamp;
//  ++num_samples_;
//  time_boundary_ = time_boundary;
//
//  if (num_samples_ == 1) tuple_st_ = timestamp;
//
//  return flushed;
//}

bool TreeMemSeries::append(slab::TreeSeries* tree_series, int64_t timestamp, double value,
                           int64_t txn){
    bool flushed = false;

    if (timestamp > global_max_time_) global_max_time_ = timestamp;
    if (min_time_ == INT64_MAX) min_time_ = timestamp;
    int64_t time_boundary = timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;

    int prev_end = chunk_.bstream.end;
    uint8_t prev_byte = chunk_.bstream.stream[prev_end-1];

    sample_txn_++;
    appender->append(timestamp, value);
    int cur_end = chunk_.bstream.end;
    if (cur_end == slab::CHUNK_SIZE && chunk_.bstream.tail_count >= 6) {
        _flush_mirror(tree_series, sample_txn_-num_samples_+1);
        flushed = true;
        head_flush_time_ = timestamp;
        min_time_ = std::numeric_limits<int64_t>::max();
        max_time_ = std::numeric_limits<int64_t>::min();
    }
    else if (cur_end > slab::CHUNK_SIZE) {
        chunk_.bstream.end = prev_end;
        chunk_.bstream.stream[prev_end-1] = prev_byte;
        base::put_uint16_big_endian(chunk_.bstream.bytes(), chunk_.num_samples() - 1);
        for(uint16_t i = prev_end; i <= cur_end; i++) {
            chunk_.bstream.stream[i] = 0;
        }
        --num_samples_;

        _flush_mirror(tree_series, sample_txn_-num_samples_+1);
        flushed = true;
        head_flush_time_ = max_time_;
        min_time_ = std::numeric_limits<int64_t>::max();
        max_time_ = std::numeric_limits<int64_t>::min();

        appender->append(timestamp, value);
    }

    max_time_ = timestamp;
    ++num_samples_;
    time_boundary_ = time_boundary;

    if (num_samples_ == 1) tuple_st_ = timestamp;

    return flushed;
}


void TreeMemSeries::update_access_epoch() {
  access_epoch.store(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());
}

}  // namespace head
}  // namespace ts