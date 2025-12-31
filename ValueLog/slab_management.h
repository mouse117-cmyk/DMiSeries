#pragma once
#ifndef SLAB_MANAGEMENT_H
#define SLAB_MANAGEMENT_H

#include <assert.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <iostream>

#include "third_party/masstree_wrapper/masstree_wrapper.hpp"
#include "third_party/atomic_queue/include/atomic_queue/atomic_queue.h"
#include "ThreadPool.h"
#include "slab.h"

#include "leveldb/util//coding.h"
namespace slab{



class SlabManagement {
 public:
  SlabManagement()=delete;
  SlabManagement(Setting &setting);
  ~SlabManagement();
  auto InitStable() -> bool;
  auto SSD_DevieSize(const char *path, size_t *size) -> bool;
  inline auto GetMemSlabInfo(uint32_t sid) -> SlabInfo* {return &mstable_[sid];}
  inline auto GetDiskSlabInfo(uint32_t sid) -> SlabInfo* {return &dstable_[sid];}
  inline auto ItemKey(Item *item) const -> uint8_t * { return item->chunk_; }
  inline auto SlabFull(const SlabInfo *sinfo) -> bool {return sinfo->nalloc_ == setting_.slab_size_ / SLAB_ITEM_SIZE;}
  inline auto ValidSid(uint32_t sid) const -> bool {return sid < nmslab_ && sid < ndslab_;}

  void EnCodeKey(std::string* str, uint64_t source_group_id, uint16_t metric_id, uint64_t t) {
    leveldb::PutFixed64BE(str, source_group_id);
    leveldb::PutFixed16(str,metric_id);
    leveldb::PutFixed64BE(str, t);
  }
  void DeCodeKey(std::string str, uint64_t& source_group_id, uint16_t& metric_id, uint64_t& t) {
    source_group_id = leveldb::DecodeFixed64BE(&str.c_str()[0]);
    metric_id = leveldb::DecodeFixed16(&str.c_str()[8]);
    t = leveldb::DecodeFixed64BE(&str.c_str()[10]);
  }

  auto GetMemSlabID(uint32_t &slab_id, uint64_t source_id, uint16_t metric_id)->bool;
  auto GetDiskSlabID(uint32_t &slab_id, uint64_t source_id, uint16_t metric_id)->bool;

  auto PutKV(uint32_t sid,uint64_t source_id, uint16_t metric_id, uint64_t key, const uint8_t* value,uint32_t value_len=CHUNK_SIZE)->bool;
  auto WritableItem(uint32_t slab_id) -> Item*;
  auto GetMemSlab(uint32_t sid) const -> Slab*;
  auto readSlabItem(const Slab* slab, uint32_t idx, size_t size = SLAB_ITEM_SIZE) -> Item*;
  auto SlabToDaddr(const SlabInfo* sidfo) const -> off_t;

  auto BatchWrite()->bool;
  auto WriteToSSD(uint32_t msid, uint32_t dsid) -> bool;

  auto ReadSlabItem(uint32_t sid, uint32_t add) -> std::pair<Item *, uint32_t >;
  auto ReadDiskSlab(SlabInfo* sinfo, off_t addr) -> Slab *;
  auto BatchRead(std::vector<SlabInfo*> sinfo_q) -> std::vector<Slab*>;

  auto InsertMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto RemoveMT(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool ;
  auto BatchRemoveMT(std::vector<SlabInfo*> sinfo_arr) -> bool;
  auto BatchInsertMT(std::vector<SlabInfo*> sinfo_arr) -> bool;
  auto SearchMT(uint64_t sourcce_id, uint16_t metric_id, uint64_t ts) -> SlabInfo*;
  auto UpdateMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto ScanMT(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time) -> std::vector<const SlabInfo*>;

  auto InsertMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto RemoveMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool ;
  auto UpdateMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto ScanMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time) -> std::vector<const SlabInfo*>;

  void ScheduleBGWrite();
  static void BGWork(void* slab_m);
  void BackGroundCall();

  auto PrintItem(Item* item) -> void;
  auto PrintSlab(Slab* slab) -> void;
  auto PrintSlab(uint32_t sid) -> void;

 private:

  Setting setting_;
  uint8_t *mstart_;
  uint8_t *mend_;
  int fd_;
  off_t dstart_;
  off_t dend_;

  uint32_t nmslab_;
  uint32_t ndslab_;
  SlabInfo *mstable_;
  SlabInfo *dstable_;
  uint32_t write_batch_size_;

  Thread_Pool pool_;
  MasstreeWrapper<SlabInfo > mass_tree_;

  uint8_t *read_buf_;
  std::mutex read_buf_mtx_;
  std::atomic<uint32_t> read_buf_offset_{0};

  leveldb::Env* env_;
  leveldb::port::Mutex mutex_;
  bool background_write_scheduled_;
  leveldb::port::CondVar background_work_finished_signal_;

    uint32_t nfree_msinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*> free_msinfoq_{1 << 18};
    uint32_t nused_msinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*> used_msinfoq_{1 << 18};
    uint32_t nfree_dsinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*> free_dsinfoq_{1 << 18};
    uint32_t nused_dsinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*> used_dsinfoq_{1 << 18};
};

}



#endif  // SLAB_MANAGEMENT_H
