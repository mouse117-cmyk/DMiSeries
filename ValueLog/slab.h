#pragma once
#ifndef SLAB_H
#define SLAB_H

#include <cstdint>
#include <stdexcept>
#include "setting.h"
#include "leveldb/include/leveldb/env.h"
#include "leveldb/port/port_stdcxx.h"
#include <atomic>

namespace slab{
#define KB (size_t(1024))
#define MB (size_t(1024) * KB)
#define GB (size_t(1024) * MB)
//#define FC_SLAB_MEMORY (8 * size_t(1024) * MB)
//#define READ_BUFFER_SIZE (size_t(256) * MB)
//#define DISK_READ_GRAN 512
//#define ROUND_UP(x, step) (((x) + (step)-1) / (step) * (step))
#define MAX_SERIES_NUM 16
////static int SLAB_ITEM_SIZE = 128;
//static int SLAB_ITEM_SIZE = 256;
////static int SLAB_ITEM_SIZE = 512;
////static int CHUNK_SIZE = 120;
//static int CHUNK_SIZE = 248;
////static int CHUNK_SIZE = 504;
//static int SLAB_SIZE = 4 * KB;
static int MAX_ALLOC_ITEM;

extern size_t FC_SLAB_MEMORY;
extern size_t READ_BUFFER_SIZE;
extern int SLAB_ITEM_SIZE;
extern int CHUNK_SIZE;
extern int SLAB_SIZE;

struct Item {
  uint64_t timestamp_;
  uint8_t chunk_[1];
};


struct Slab {
  uint8_t data_[1];
  Slab() = default;
};

struct SlabClass{
    std::mutex mtx_;
    uint64_t class_source_id_;
    uint16_t class_metric_id_;
    uint32_t slab_id_;
};

struct SlabInfo {
  uint16_t cid_x_;
  uint16_t cid_y_;
  uint32_t slab_id_;
  uint8_t source_id_[MAX_SERIES_NUM];
  uint8_t metric_id_[MAX_SERIES_NUM];
  uint64_t start_time_[MAX_SERIES_NUM];
  uint64_t end_time_[MAX_SERIES_NUM];
  uint64_t txn_[MAX_SERIES_NUM];
    std::atomic<uint8_t> nalloc_;
    std::mutex mtx_;
  bool mem_ : 1;
  bool free_ : 1;
  uint8_t idx_;
//  SlabInfo() = default;
  SlabInfo() : cid_x_(0), cid_y_(0), slab_id_(0), nalloc_(0), mem_(true), free_(true), idx_(0) {}
//  SlabInfo(uint64_t sgid, uint16_t mid, uint16_t nalloc = SLAB_SIZE / SLAB_ITEM_SIZE) : source_id_(sgid), metric_id_(mid), nalloc_(nalloc){};
};


class Setting {
public:
    Setting() { SetDefault(); }
    size_t max_slab_memory_;
    char *ssd_device_;
    char *ssd_slab_info_;
    size_t slab_size_;
    int port;
    char const *addr;
    uint32_t write_batch_size_;
    uint32_t migrate_batch_size_;

private:
    void SetDefault();
//    void SetDefault() {
//        max_slab_memory_ = FC_SLAB_MEMORY;
//        ssd_device_ = "/tmp/ssd";
//        ssd_slab_info_ = "/tmp/ssd_slab_info";
//        port = 11211;
//        addr = "";
//        slab_size_ = SLAB_SIZE;
//        write_batch_size_ = 1600;
//        migrate_batch_size_ = 6400;
//    }
};

}



#endif  // SLAB_H
