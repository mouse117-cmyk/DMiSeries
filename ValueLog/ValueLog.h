#ifndef TSDB_UNITTEST_VALUELOG_H
#define TSDB_UNITTEST_VALUELOG_H
#include <assert.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "gtest/gtest.h"
#include <jemalloc/jemalloc.h>

#include <cstdint>
#include <cstring>
#include <iostream>

#include "third_party/masstree_wrapper/masstree_wrapper.hpp"
#include "third_party/atomic_queue/include/atomic_queue/atomic_queue.h"
#include "ThreadPool.h"
#include "slab.h"

#include "leveldb/util//coding.h"
#include "leveldb/db.h"
#include "leveldb/db/dbformat.h"
#include "leveldb/db/db_iter.h"
#include "leveldb/db/db_impl.h"

namespace leveldb {
    class DB;
    class Status;
}  // namespace leveldb

namespace slab{
extern size_t slab_total_write_bytes;

static const int CX_NUM = 100;
static const int CY_NUM = 10000;


void encode_l1key(std::string* key, uint64_t sgid, uint16_t mid, uint64_t end_time);
void decode_l1key(std::string key, uint64_t& sgid, uint16_t& mid, uint64_t& end_time);

void encode_l1val(std::string* val, bool mem, uint32_t sid, uint8_t item_idx);
void decode_l1val(std::string val, bool& mem, uint32_t& sid, uint8_t& item_idx);

class ValueLog{
 public:
  ValueLog()=delete;
  ValueLog(Setting &setting);
  ~ValueLog();
  auto InitCtable() -> void;
//  auto DestroyCtable() -> void;
  auto InitStable() -> bool;
  auto SSD_DevieSize(const char *path, size_t *size) -> bool;
  inline auto GetMemSlabInfo(uint32_t sid) -> SlabInfo* {return &mstable_[sid];}
  inline auto GetMemSlabInfo(uint64_t source_id,uint16_t metric_id,uint32_t sid,uint8_t &idx) -> SlabInfo* {
      for(uint8_t i=0;i<mstable_[sid].idx_;i++){
          if(mstable_[sid].source_id_[i] == source_id && mstable_[sid].metric_id_[i] == metric_id){
              idx = i;
          }
      }
      return &mstable_[sid];
  }
  inline auto GetDiskSlabInfo(uint32_t sid) -> SlabInfo* {return &dstable_[sid];}
  inline auto ItemKey(Item *item) const -> uint8_t * { return item->chunk_; }
  inline auto SlabFull(const SlabInfo *sinfo) -> bool {return sinfo->nalloc_.load() == setting_.slab_size_ / SLAB_ITEM_SIZE;}
  inline auto ValidSid(uint32_t sid) const -> bool {return sid < nmslab_ && sid < ndslab_;}
  inline auto ValidMemSid(uint32_t msid) const -> bool {return msid < nmslab_;}
  inline auto GetSlabSize() -> uint32_t {return setting_.slab_size_;}
  inline auto GetItemNum() -> uint32_t {return setting_.slab_size_ / SLAB_ITEM_SIZE;}
  inline auto GetMemSlabNum() -> uint32_t {return nmslab_;}
  inline auto GetDiskSlabNum() -> uint32_t {return ndslab_;}
  inline auto GetNFreeBufSlab() -> uint32_t {return nfree_bsinfo_.load();}
  inline auto GetNBufSlab() -> uint32_t {return nbuf_slab_;}
  inline auto GetFreeMemSlabNum() -> uint32_t {return nfree_msinfoq_.load();}
  inline auto GetUsedMemSlabNum() -> uint32_t {return nused_msinfoq_.load();}
  inline auto GetFullMemSlabNum() -> uint32_t {return nfull_msinfoq_.load();}
  inline auto GetFreeDiskSlabNum() -> uint32_t {return nfree_dsinfoq_.load();}
  inline auto GetUsedDiskSlabNum() -> uint32_t {return nused_dsinfoq_.load();}
  inline auto GetBatchWriteNum() -> uint32_t {return setting_.write_batch_size_;}
  inline auto GetMigrateNum() -> uint32_t {return setting_.migrate_batch_size_;}
  inline auto FreeSlab(SlabInfo* sinfo) -> void{
    if(sinfo == nullptr){
      return;
    }
    sinfo->free_ = 1;
    sinfo->nalloc_.store(0);
    for(uint8_t i=0;i<sinfo->idx_;i++){
        sinfo->source_id_[i] = 0;
        sinfo->metric_id_[i] = 0;
        sinfo->start_time_[i] = 0;
        sinfo->end_time_[i] = 0;
    }
    sinfo->idx_ = 0;
  }

  inline auto GetSinfoSourceID(SlabInfo * sinfo,uint8_t idx) -> uint64_t {
      SlabClass* cinfo = &ctable[sinfo->cid_x_][sinfo->cid_y_];
      return (uint64_t)(sinfo->source_id_[idx])*CY_NUM+cinfo->class_source_id_;
  }

  inline auto GetSinfoMetricID(SlabInfo * sinfo,uint8_t idx) -> uint16_t {
      SlabClass* cinfo = &ctable[sinfo->cid_x_][sinfo->cid_y_];
      return (uint16_t)(sinfo->metric_id_[idx])*CX_NUM+cinfo->class_metric_id_;
  }

void EnCodeKey(std::string* str, uint64_t source_group_id, uint16_t metric_id, uint64_t t) {
//    leveldb::PutFixed64BE(str, source_group_id);
//    leveldb::PutFixed16(str,metric_id);
//    leveldb::PutFixed64BE(str, t);

    encode_l1key(str, source_group_id, metric_id, t);
}
void DeCodeKey(std::string str, uint64_t& source_group_id, uint16_t& metric_id, uint64_t& t) {
//    source_group_id = leveldb::DecodeFixed64BE(&str.c_str()[0]);
//    metric_id = leveldb::DecodeFixed16(&str.c_str()[8]);
//    t = leveldb::DecodeFixed64BE(&str.c_str()[10]);

    decode_l1key(str, source_group_id, metric_id, t);
}

  auto GetMemSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id)->bool;
  auto GetDiskSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id)->bool;

  auto GetMemSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id,uint64_t start_time)->bool;
  auto GetDiskSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id, uint64_t start_time)->bool;

  inline auto CopyInfo(SlabInfo *dsinfo, SlabInfo *ssinfo)->bool{
    if(dsinfo== nullptr||ssinfo== nullptr){
      return false;
    }
    dsinfo->cid_x_ = ssinfo->cid_x_;
    dsinfo->cid_y_ = ssinfo->cid_y_;
    for(uint8_t i=0;i<ssinfo->idx_;i++){
        dsinfo->source_id_[i] = ssinfo->source_id_[i];
        dsinfo->metric_id_[i] = ssinfo->metric_id_[i];
        dsinfo->start_time_[i] = ssinfo->start_time_[i];
        dsinfo->end_time_[i] = ssinfo->end_time_[i];
        dsinfo->txn_[i] = ssinfo->txn_[i];
    }
    dsinfo->idx_ = ssinfo->idx_;
    dsinfo->nalloc_ = ssinfo->nalloc_.load();
    return true;
  }

  inline auto InsertMemFullQueue(SlabInfo *msinfo) -> bool{
    if(msinfo == nullptr){
      return false;
    }
    full_msinfoq_->push(msinfo);
    nfull_msinfoq_.fetch_add(1);
    nused_msinfoq_.fetch_sub(1);
    return true;
  }

  auto PutKV(uint32_t& sid,uint64_t source_id, uint16_t metric_id, uint64_t key, uint64_t end_time, const uint8_t* value,uint32_t value_len=CHUNK_SIZE, uint64_t txn = 0)->bool;
  auto ConPutKV(uint32_t& sid,uint64_t source_id, uint16_t metric_id, uint64_t key, uint64_t end_time, const uint8_t* value,uint32_t value_len=CHUNK_SIZE, uint64_t txn = 0)->bool;
  auto MirrorPutKV(uint32_t& sid, uint64_t source_id, uint16_t metric_id, uint64_t key, uint64_t end_time, const uint8_t* value,uint32_t value_len=CHUNK_SIZE, uint64_t txn = 0)->bool;

  auto WritableItem(uint32_t slab_id) -> Item*;
  auto GetMemSlab(uint32_t sid) const -> Slab*;
  auto ReadSlabItem(const Slab* slab, uint32_t idx, size_t size = SLAB_ITEM_SIZE) -> Item*;

  auto SlabToDaddr(const SlabInfo* sidfo) const -> off_t;

  auto BatchWrite() -> bool;
  auto WriteToSSD(uint32_t msid, uint32_t dsid) -> bool;
  auto OptBatchWrite() -> bool;
  auto OptWriteToSSD(uint32_t msid,uint32_t dsid,uint32_t slab_num) -> bool;

  auto OptBatchWriteLazyFlush() -> bool;
  auto ForceFlush() -> bool;
  auto WriteDiskSlabInfo() -> bool;
  auto RecoveryDiskSlabInfo() -> bool;
  auto RecoveryDiskSlabInfoMirror() -> bool;

  auto TraverseMasstree() -> std::vector<std::pair<std::string, const SlabInfo*>>;
  auto WriteDiskMasstree() -> bool;
  auto RecoverDiskMasstree() -> bool;

  auto FreeBufSlab(Slab* buf_slab) -> bool{
    if(buf_slab == nullptr){
      return false;
    }
    free_bsinfoq_->push(buf_slab);
    nfree_bsinfo_.fetch_add(1);
    return true;
  }

  auto ReadSlabItem(SlabInfo* sinfo, uint32_t add) -> std::pair<Item *, uint32_t >;
  auto ReadDiskSlab(SlabInfo* sinfo) -> Slab *;
  auto ReadDiskSlab(const SlabInfo* sinfo) -> Slab *;
  auto BatchRead(std::vector<SlabInfo*>& sinfo_q,std::vector<Slab*>& slab_arr) -> void;
  auto BatchDiskRead(SlabInfo* sinfo_arr[], uint32_t count,std::vector<Slab*>& slab_arr) -> void;

  auto SequentialDiskRead(SlabInfo* sinfo_arr[], uint32_t count, std::vector<Slab*>& slab_arr) -> void;

  auto MigrateRead(std::vector<std::pair<SlabInfo*,Slab*>>&migrate_slab_array, bool& is_seq) -> bool;
  auto MigrateReadLazyFlush(std::vector<std::pair<SlabInfo*,Slab*>>&migrate_slab_array, bool& is_seq) -> bool;

  auto InsertMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto RemoveMT(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool ;
  auto BatchRemoveMT(std::vector<SlabInfo*> sinfo_arr) -> bool;
  auto BatchInsertMT(std::vector<SlabInfo*> sinfo_arr) -> bool;
  auto SearchMT(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> SlabInfo*;
  auto UpdateMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto ScanMT(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time,std::vector<const SlabInfo*>&sinfo_arr)->void;

  auto InsertDB(uint64_t source_id, uint16_t metric_id, uint64_t ts, bool mem, uint32_t sid, uint8_t item_idx) -> bool;
  auto DeleteDB(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool;
  auto SearchDB(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> std::pair<SlabInfo*, uint8_t>;
  auto UpdateDB(uint64_t source_id, uint16_t metric_id, uint64_t ts, bool mem, uint32_t sid, uint8_t item_idx) -> bool;
  auto ScanDB(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time, std::vector<const SlabInfo*>&sinfo_arr) -> void;
  auto TraverseDB() -> void;

  auto InsertMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto RemoveMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool ;
  auto UpdateMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool;
  auto ScanMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time) -> std::vector<const SlabInfo*>;
  auto Scan(uint64_t source_id,uint16_t metric_id, uint64_t start_time, uint64_t end_time,std::vector<std::pair<const SlabInfo*,Slab*>>&slab_arr) -> bool;

  void ScheduleBGWrite();
  static void BGWork(void* slab_m);
  void BackGroundCall();

  auto DecodeChunk(uint8_t * chunk) -> std::pair<std::vector<int64_t>, std::vector<double>>;
  auto PrintChunk(uint8_t * chunk) -> void;
  auto PrintItem(Item* item) -> void;
  auto PrintSlab(Slab* slab) -> void;
  auto PrintSlab(uint32_t sid) -> void;

  void SetDB(leveldb::DB* db) {db_ = db;}
  leveldb::DB* GetDB() { return db_; }

 private:

  leveldb::DB* db_;
  leveldb::WriteOptions write_option_;
  leveldb::ReadOptions read_option_;

  Setting setting_;
  uint8_t *mstart_;
  uint8_t *mend_;
  int fd_;
  int fd_info_;
  off_t dstart_;
  off_t dend_;

  uint32_t nmslab_;
  uint32_t ndslab_;
  uint32_t nbuf_slab_;
  SlabInfo *mstable_;
  SlabInfo *dstable_;
  SlabClass ctable[CX_NUM][CY_NUM];

  Thread_Pool pool_;
  MasstreeWrapper<SlabInfo > mass_tree_;

  uint8_t *read_buf_;
  uint8_t *seq_read_buf_;

  std::atomic<uint32_t> migrate_read_buf_offset_{0};

  leveldb::Env* env_;
  leveldb::port::Mutex mutex_;
  bool background_write_scheduled_;
  leveldb::port::CondVar background_work_finished_signal_;

    std::atomic<uint32_t> nfree_msinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*>* free_msinfoq_;
    std::atomic<uint32_t> nused_msinfoq_;
    std::atomic<uint32_t> nfull_msinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*>* full_msinfoq_;
    std::atomic<uint32_t> nfree_dsinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*>* free_dsinfoq_;
    std::atomic<uint32_t> nused_dsinfoq_;
    atomic_queue::AtomicQueueB2<SlabInfo*>* used_dsinfoq_;
    std::atomic<uint32_t>nfree_bsinfo_;
    atomic_queue::AtomicQueueB2<Slab*>* free_bsinfoq_;

    atomic_queue::AtomicQueueB2<std::pair<SlabInfo*, SlabInfo*>>* mem_evict_sinfoq_;
    std::atomic<uint32_t> n_mem_evict_sinfo_;
    atomic_queue::AtomicQueueB2<SlabInfo*>* disk_evict_sinfoq_;
    std::atomic<uint32_t> n_disk_evict_sinfo_;
    SlabInfo* full_msinfo_wqueue;
    uint8_t* sort_slab_buffer_;
};

auto FindSinfoSubSequence(SlabInfo** full_msinfo_wqueue, uint32_t count) -> std::vector<std::pair<uint32_t, uint32_t>>;

}
#endif  // TSDB_UNITTEST_VALUELOG_H
