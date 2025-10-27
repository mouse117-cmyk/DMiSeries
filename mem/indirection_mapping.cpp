#include "indirection_mapping.h"

#include <stdlib.h>

#include <algorithm>
#include <iostream>
#include <limits>

#include "util/coding.h"

namespace tsdb {
namespace mem {

IndirectionMapping::IndirectionMapping(){
  slots_.reserve(METRIC_NUM);
  for (int i = 0; i < METRIC_NUM; i++) {
    std::vector<std::atomic<uint64_t>> v(SOURCE_NUM);
    slots_.push_back(std::move(v));
  }
}

IndirectionMapping::~IndirectionMapping() {}

void IndirectionMapping::alloc_slot(uint64_t sgid, uint64_t mid) {
  if (mid >= slots_.size()) {
    for (int i = slots_.size(); i <= mid; i++) {
      std::vector<std::atomic<uint64_t>> v(SOURCE_NUM);
      slots_.push_back(std::move(v));
    }
  }
}

void IndirectionMapping::set_slot(uint64_t sgid, uint64_t mid, uint64_t val) {
//  alloc_slot(sgid, mid);
  slots_[mid][sgid].store(val);
}

bool IndirectionMapping::cas_slot(uint64_t sgid, uint64_t mid, uint64_t old_val, uint64_t new_val) {
  if (mid >= slots_.size() || sgid >= SOURCE_NUM) {
    return false;
  }
  return slots_[mid][sgid].compare_exchange_weak(old_val, new_val);
}

uint64_t IndirectionMapping::read_slot(uint64_t sgid, uint64_t mid) {
  if (mid >= slots_.size() || sgid >= SOURCE_NUM) {
    return std::numeric_limits<uint64_t>::max();
  }
  return slots_[mid][sgid].load();
}

void IndirectionMapping::iter(void (*cb)(uint64_t v)) {
  for (auto & metric : slots_) {
    for (auto & source : metric) {
      cb(source);
    }
  }
}

std::vector<std::pair<uint64_t, uint64_t>> IndirectionMapping::get_ids() {
  std::vector<std::pair<uint64_t, uint64_t>> ids;
  uint64_t counter = 0;
  for (int i = 0; i < SOURCE_NUM; i++) {
    for (int j = 0; j < slots_.size(); j++) {
      ids.emplace_back(counter++, slots_[j][i].load());
    }
  }
  return ids;
}

void IndirectionMapping::print_imap() {
  std::cout<<"################# imap ################"<<std::endl;
  std::cout<<"metric num: "<<slots_.size()<<"\tsource_group num: "<<slots_[0].size()<<std::endl;
  uint64_t cnt = 0;
  for (uint64_t i = 0; i < slots_.size(); i++) {
    for (uint64_t j = 0; j < slots_[i].size(); j++) {
      if (  slots_[i][j] == 0) {
        continue;
      }
      std::cout<<"metric: "<<i<<"\tsource_group: "<<j<<"\tval: "<<slots_[i][j]<<std::endl;
      cnt++;
    }
  }
  std::cout<<"count: "<<cnt<<std::endl;
  std::cout<<"OK"<<std::endl<<std::endl;
}

void IndirectionMapping::print_imap(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num) {
//  std::cout<<"################# imap ################"<<std::endl;
  uint64_t cnt = 0;
  std::cout<<"metric num: "<<metric_num<<"\tsource_group num: "<<sg_num<<std::endl;
  for (uint64_t i = metric_start; i < metric_num+metric_start; i++) {
    for (uint64_t j = sg_start; j < std::min(sg_start+sg_num, slots_[i].size()); j++) {
      std::cout<<"metric: "<<i<<"\tsource_group: "<<j<<"\tval: "<<slots_[i][j]<<std::endl;
      cnt++;
    }
  }
  std::cout<<"count: "<<cnt<<std::endl;
  std::cout<<"OK"<<std::endl<<std::endl;
}

leveldb::Status IndirectionMapping::snapshot(const std::string& dir) {
  leveldb::WritableFile* file;
  leveldb::Status s = leveldb::Env::Default()->NewAppendableFile(dir + "/indirection", &file);
  if (!s.ok()) return s;
  std::string record, header;

  leveldb::PutVarint64(&header, slots_.size()*SOURCE_NUM);
  for(size_t i = 0; i < slots_.size(); i++) {
    for(size_t j = 0; j < slots_[i].size(); j++) {
      leveldb::PutVarint64(&record, slots_[i][j].load());
    }
  }

  file->Append(header);
  file->Append(record);
  header.clear();
  record.clear();

  delete file;

  return leveldb::Status::OK();
}

leveldb::Status IndirectionMapping::recover_from_snapshot(const std::string& dir) {
  leveldb::RandomAccessFile* file;
  uint64_t offset = 0;
  leveldb::Status s = leveldb::Env::Default()->NewRandomAccessFile(dir + "/indirection", &file);
  if (!s.ok()) return s;

  slots_.clear();

  char header[10];
  leveldb::Slice record;
  uint64_t record_size, v;
  int block_id = 0;

  s = file->Read(offset, 10, &record, header);
  if (!s.ok()) return s;
  const char* p = leveldb::GetVarint64Ptr(record.data(), record.data() + 10, &record_size);
  offset += p - record.data();

  char* buf = new char[record_size];
  s = file->Read(offset, record_size, &record, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }

  int midx = 0, sidx = 0;
  while (record.size() > 0) {
    leveldb::GetVarint64(&record, &v);
    slots_[midx][sidx++].store(v);
    if (sidx == SOURCE_NUM) {
      midx++;
      sidx = 0;
    }
  }
  delete[] buf;

  return leveldb::Status::OK();
}

}  // namespace mem
}  // namespace tsdb