#pragma once
#ifndef SETTING_H
#define SETTING_H
#include <bits/types.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include "slab.h"

namespace slab {
//#define KB (1024)
//#define MB (1024 * KB)
//#define GB (1024 * MB)
//#define FC_SLAB_MEMORY 1024 * MB
//#define READ_BUFFER_SIZE 256 * MB
//#define DISK_READ_GRAN 512
//#define ROUND_UP(x, step) (((x) + (step)-1) / (step) * (step))
//class Setting {
// public:
//  Setting() { SetDefault(); }
//  size_t max_slab_memory_;
//  char *ssd_device_;
//  char *ssd_slab_info_;
//  size_t slab_size_;
//  int port;
//  char const *addr;
//  uint32_t write_batch_size_;
//  uint32_t migrate_batch_size_;
//
// private:
//  void SetDefault() {
//    max_slab_memory_ = FC_SLAB_MEMORY;
//    ssd_device_ = "/tmp/ssd";
//    ssd_slab_info_ = "/tmp/ssd_slab_info";
//    port = 11211;
//    addr = "";
//    slab_size_ = SLAB_SIZE;
//    write_batch_size_ = 1600;
//    migrate_batch_size_ = 6400;
//  }
//};
}
#endif  // SETTING_H