#include "ValueLog.h"
#include <iostream>
#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "slab.h"
#include <jemalloc/jemalloc.h>
#include "config.h"

namespace slab{
size_t slab_total_write_bytes = 0;

void *tsmmap(size_t size){
  void *p;
  p = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  return p;
}

int tsmunmap(void *p, std::size_t size) {
  int status;
  status = munmap(p, size);
  return status;
}

ValueLog::ValueLog(Setting &setting):
//      pool_(32),
      pool_(config::tree_series_thread_pool_size),
      mass_tree_(),
      background_write_scheduled_(false),
      background_work_finished_signal_(&mutex_)
      {
    write_option_ = leveldb::WriteOptions();
    read_option_ = leveldb::ReadOptions();
  setting_ = setting;
  assert(setting_.ssd_device_ != nullptr);
  assert(setting_.ssd_slab_info_ != nullptr);
  MAX_ALLOC_ITEM = setting_.slab_size_ / SLAB_ITEM_SIZE;

  env_ = leveldb::Env::Default();

  nfree_msinfoq_.store(0);
  nfree_dsinfoq_.store(0);
  nused_msinfoq_.store(0);
  nfull_msinfoq_.store(0);
  nused_dsinfoq_.store(0);

  n_mem_evict_sinfo_.store(0);
  n_disk_evict_sinfo_.store(0);

  nmslab_ = setting_.max_slab_memory_ / setting_.slab_size_;
 // mstart_ = static_cast<uint8_t *>(malloc(setting_.max_slab_memory_));
  mstart_ = static_cast<uint8_t *>(tsmmap(setting_.max_slab_memory_));
  mend_   = mstart_ + setting_.max_slab_memory_;

  size_t size = 0;
  bool status = SSD_DevieSize(setting_.ssd_device_, &size);
  ndslab_ = size / setting_.slab_size_;
  dstart_ = 0;
  dend_ = dstart_ + size;

  free_msinfoq_ = new atomic_queue::AtomicQueueB2<SlabInfo*>{nmslab_};
  full_msinfoq_ = new atomic_queue::AtomicQueueB2<SlabInfo*>{nmslab_};
  free_dsinfoq_ = new atomic_queue::AtomicQueueB2<SlabInfo*>{ndslab_};
  used_dsinfoq_ = new atomic_queue::AtomicQueueB2<SlabInfo*>{ndslab_};

  mem_evict_sinfoq_ = new atomic_queue::AtomicQueueB2<std::pair<SlabInfo*, SlabInfo*>>{nmslab_};
  disk_evict_sinfoq_ = new atomic_queue::AtomicQueueB2<SlabInfo*>{ndslab_};

  fd_ = open(setting_.ssd_device_, O_RDWR | O_DIRECT, 0644);
  fd_info_ = open(setting_.ssd_slab_info_, O_RDWR, 0644);
  if(fd_ < 0 || fd_info_ < 0){
    throw std::logic_error("fail in open file");
  }
  setting_.write_batch_size_ = nmslab_ / 8;
  setting_.migrate_batch_size_ = ndslab_ / 10;
  //write_batch_size_ = setting_.write_batch_size_;
  read_buf_ = nullptr;
  read_buf_ = static_cast<uint8_t *>(tsmmap(READ_BUFFER_SIZE));
//  read_buf_ = static_cast<uint8_t *>(malloc(READ_BUFFER_SIZE));
  seq_read_buf_ = nullptr;
  seq_read_buf_ = static_cast<uint8_t *>(tsmmap(READ_BUFFER_SIZE));
//  seq_read_buf_ = static_cast<uint8_t *>(malloc(READ_BUFFER_SIZE));
  memset(read_buf_, 0xff, READ_BUFFER_SIZE);
  memset(seq_read_buf_, 0xff, READ_BUFFER_SIZE);
  nbuf_slab_ = READ_BUFFER_SIZE / SLAB_SIZE;
  //nmigrate_slab_ = READ_BUFFER_SIZE / SLAB_SIZE;
  nfree_bsinfo_ = nbuf_slab_;
  free_bsinfoq_ = new atomic_queue::AtomicQueueB2<Slab*>{nbuf_slab_+1};
  for(uint32_t i=0;i<nbuf_slab_;i++){
//    free_bsinfoq_.enqueue((Slab *)(read_buf_ + i* SLAB_SIZE));
      free_bsinfoq_->push((Slab *)(read_buf_ + i* SLAB_SIZE));
  }
  InitCtable();
  InitStable();
}

ValueLog::~ValueLog(){
  free(mstable_);
  mstable_ = nullptr;
  nmslab_ = 0;
  free(dstable_);
  dstable_ = nullptr;
  ndslab_ = 0;
//    free(mstart_);
  tsmunmap(mstart_,setting_.max_slab_memory_);
  mstart_ = nullptr;
//  free(read_buf_);
  tsmunmap(read_buf_,READ_BUFFER_SIZE);
  read_buf_ = nullptr;
  free(seq_read_buf_);
//  tsmunmap(seq_read_buf_,READ_BUFFER_SIZE);
  seq_read_buf_ = nullptr;
  close(fd_);
  close(fd_info_);

  //DestroyCtable();
    delete free_msinfoq_;
    delete free_dsinfoq_;
    delete full_msinfoq_;
    delete used_dsinfoq_;
    delete free_bsinfoq_;

    delete mem_evict_sinfoq_;
    delete disk_evict_sinfoq_;
}

auto ValueLog::InitCtable() -> void {
    for (int i = 0; i < CX_NUM; i++) {
        for (int j = 0; j < CY_NUM; j++) {
            ctable[i][j].class_metric_id_ = i;
            ctable[i][j].class_source_id_ = j;
            ctable[i][j].slab_id_ = UINT32_MAX;
        }
    }
}

//auto ValueLog::DestroyCtable() -> void {
//    for (int i = 0; i < CX_NUM; i++) {
//        for (int j = 0; j < CY_NUM; j++) {
//            delete ctable[i][j];
//        }
//    }
//}

auto ValueLog::InitStable() -> bool {
  SlabInfo *sinfo;
  // mstable_ = static_cast<SlabInfo *>((void *)malloc(sizeof(*mstable_) * nmslab_));
  mstable_ = static_cast<SlabInfo *>(tsmmap(sizeof(*mstable_) * nmslab_));
  if (mstable_ == nullptr) {
    return false;
  }
  for (uint32_t i = 0; i < nmslab_; i++) {
    sinfo = &mstable_[i];

    for (uint32_t j = 0; j < MAX_SERIES_NUM; j++) {
        sinfo->source_id_[j] = 0;
        sinfo->metric_id_[j] = 0;
        sinfo->txn_[j] = 0;
        sinfo->start_time_[j] = 0;
        sinfo->end_time_[j] = 0;
    }
    sinfo->idx_ = 0;
    sinfo->mtx_.unlock();
    sinfo->slab_id_ = i;
    sinfo->nalloc_.store(0);
    sinfo->free_ = true;
    sinfo->mem_ = 1;
    nfree_msinfoq_.fetch_add(1);
    free_msinfoq_->push(&mstable_[i]);
  }
  // dstable_ = static_cast<SlabInfo *>((void *)malloc(sizeof(SlabInfo) * ndslab_+1));
  dstable_ = static_cast<SlabInfo *>(tsmmap(sizeof(*dstable_) * (ndslab_+1)));
  if (dstable_ == nullptr) {
    return false;
  }

  for (uint32_t i = 0; i < ndslab_; i++) {
    sinfo = &dstable_[i];

    for (uint32_t j = 0; j < MAX_SERIES_NUM; j++) {
      sinfo->source_id_[j] = 0;
      sinfo->metric_id_[j] = 0;
      sinfo->txn_[j] = 0;
      sinfo->start_time_[j] = 0;
      sinfo->end_time_[j] = 0;
    }
    sinfo->idx_ = 0;
    sinfo->mtx_.unlock();
    sinfo->slab_id_ = i;
    sinfo->nalloc_.store(0);
    sinfo->mem_ = 0;
    sinfo->free_ = true;
    nfree_dsinfoq_.fetch_add(1);
      free_dsinfoq_->push(&dstable_[i]);
  }
  return true;
}

auto ValueLog::SSD_DevieSize(const char *path, size_t *size) -> bool {
  int status;
  struct stat statiofo;
  int fd;

  status = stat(path, &statiofo);
  if (!S_ISREG(statiofo.st_mode) && !S_ISBLK(statiofo.st_mode)) {
    return false;
  }
  if (S_ISREG(statiofo.st_mode)) {
    *size = static_cast<size_t>(statiofo.st_size);
    return true;
  }

  fd = open(path, O_RDONLY, 0644);
  if (fd < 0) {
    return false;
  }
  status = ioctl(fd, _IOR(0x12,114,size_t), size);
  if (status < 0) {
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

auto ValueLog::GetMemSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id) -> bool {
  SlabInfo* sinfo;
  SlabClass* cinfo = &ctable[metric_id%CX_NUM][source_id%CY_NUM];
 // std::cout<<"***\n";
  cinfo->mtx_.lock();
//  std::cout<<"nmfree: "<<nfree_msinfoq_<<" nmevict:"<<n_mem_evict_sinfo_<<std::endl;
  //std::cout<<"*\n";
  if(cinfo->slab_id_ == UINT32_MAX||mstable_[cinfo->slab_id_].idx_ == MAX_SERIES_NUM|| SlabFull(&mstable_[cinfo->slab_id_])){
      auto flag = free_msinfoq_->try_pop(sinfo);
      if(!flag){
          SlabInfo* dsinfo;
          std::pair<SlabInfo*, SlabInfo*> spair{sinfo, dsinfo} ;
          flag = mem_evict_sinfoq_->try_pop(spair);
          if (!flag ) {
              cinfo->mtx_.unlock();
              std::cout<<"*"<<cinfo->slab_id_<<"*"<<std::endl;
              std::cout<<"nmfree:"<<nfree_msinfoq_<<" nfull:"<<nfull_msinfoq_<<" nused:"<<nused_msinfoq_<<" nmevict:"<<n_mem_evict_sinfo_<<std::endl;
              return flag;
          }
          n_mem_evict_sinfo_.fetch_sub(1);
          nused_msinfoq_.fetch_add(1);
          auto tmp_msinfo = spair.first;
          auto tmp_dsinfo = spair.second;
          for (uint8_t j = 0; j < tmp_msinfo->idx_; j++) {
//              UpdateMT(GetSinfoSourceID(tmp_msinfo,j), GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->start_time_[j], tmp_dsinfo);
//              UpdateMT(GetSinfoSourceID(tmp_msinfo,j), GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->end_time_[j], tmp_dsinfo);
              UpdateDB(GetSinfoSourceID(tmp_msinfo,j),
                       GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->end_time_[j], false, tmp_dsinfo->slab_id_, 0);
          }
//          FreeSlab(sinfo);
//          sid = sinfo->slab_id_;
//          sinfo->free_ = false;
//          sinfo->source_id_[0] = (uint8_t)(source_id / CY_NUM);
//          sinfo->metric_id_[0] = (uint8_t)(metric_id / CX_NUM);
//          sinfo->idx_ = 1;
//          cinfo->slab_id_ = sinfo->slab_id_;
          FreeSlab(tmp_msinfo);
          sid = tmp_msinfo->slab_id_;
          tmp_msinfo->free_ = false;
          tmp_msinfo->source_id_[0] = (uint8_t)(source_id / CY_NUM);
          tmp_msinfo->metric_id_[0] = (uint8_t)(metric_id / CX_NUM);
          tmp_msinfo->idx_ = 1;
          cinfo->slab_id_ = tmp_msinfo->slab_id_;
          tmp_msinfo->cid_x_ = metric_id % CX_NUM;
          tmp_msinfo->cid_y_ = source_id % CY_NUM;
      }else{
          sid = sinfo->slab_id_;
          nused_msinfoq_.fetch_add(1);
          nfree_msinfoq_.fetch_sub(1);
          sinfo->free_ = false;
          sinfo->source_id_[0] = (uint8_t)(source_id / CY_NUM);
          sinfo->metric_id_[0] = (uint8_t)(metric_id / CX_NUM);
          sinfo->idx_ = 1;
          cinfo->slab_id_ = sinfo->slab_id_;
          sinfo->cid_x_ = metric_id % CX_NUM;
          sinfo->cid_y_ = source_id % CY_NUM;
      }
      cinfo->mtx_.unlock();
      return flag;
  }
  sinfo = &mstable_[cinfo->slab_id_];
  sid = sinfo->slab_id_;
  sinfo->source_id_[sinfo->idx_] = (uint8_t)(source_id / CY_NUM);
  sinfo->metric_id_[sinfo->idx_] = (uint8_t)(metric_id / CX_NUM);
  sinfo->idx_++;
  cinfo->mtx_.unlock();
  return true;
}

auto ValueLog::GetDiskSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id) -> bool {
  SlabInfo* sinfo;
    auto flag = free_dsinfoq_->try_pop(sinfo);
  if(!flag){
    return flag;
  }
  sid = sinfo->slab_id_;
    used_dsinfoq_->push(sinfo);
  sinfo->free_ = false;
  sinfo->source_id_[0] = source_id;
  sinfo->metric_id_[0] = metric_id;

  return flag;
}

auto ValueLog::GetMemSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id, uint64_t start_time) -> bool {
    SlabInfo* sinfo;
    SlabClass* cinfo = &ctable[metric_id%CX_NUM][source_id%CY_NUM];
    std::cout<<"**********\n";
    cinfo->mtx_.lock();
    std::cout<<"*\n";
    if(cinfo->slab_id_ == UINT32_MAX||mstable_[cinfo->slab_id_].idx_ == MAX_SERIES_NUM || SlabFull(&mstable_[cinfo->slab_id_])){
        auto flag = free_msinfoq_->try_pop(sinfo);
        if(!flag){
            SlabInfo* dsinfo;
            std::pair<SlabInfo*, SlabInfo*> spair{sinfo, dsinfo} ;
            flag = mem_evict_sinfoq_->try_pop(spair);
            if ( !flag ) {
                cinfo->mtx_.unlock();
                return flag;
            }
            n_mem_evict_sinfo_.fetch_sub(1);
            nused_msinfoq_.fetch_add(1);
            for (uint8_t j = 0; j < sinfo->idx_; j++) {
//                UpdateMT(GetSinfoSourceID(sinfo,j), GetSinfoSourceID(sinfo,j),sinfo->start_time_[j], dsinfo);
//                UpdateMT(GetSinfoSourceID(sinfo,j), GetSinfoSourceID(sinfo,j),sinfo->end_time_[j], dsinfo);
                UpdateDB(GetSinfoSourceID(sinfo,j),
                         GetSinfoMetricID(sinfo,j),sinfo->end_time_[j], false, dsinfo->slab_id_,0);
            }
            FreeSlab(sinfo);
            sid = sinfo->slab_id_;
            sinfo->free_ = false;
            sinfo->source_id_[0] = source_id / CY_NUM;
            sinfo->metric_id_[0] = metric_id / CX_NUM;
            sinfo->idx_ = 1;
            cinfo->slab_id_ = sinfo->slab_id_;
            sinfo->start_time_[0] = start_time;
        }else{
            sid = sinfo->slab_id_;
            nused_msinfoq_.fetch_add(1);
            nfree_msinfoq_.fetch_sub(1);
            sinfo->free_ = false;
            sinfo->source_id_[0] = source_id / CY_NUM;
            sinfo->metric_id_[0] = metric_id / CX_NUM;
            sinfo->idx_ = 1;
            cinfo->slab_id_ = sinfo->slab_id_;
            sinfo->start_time_[0] = start_time;
        }
        cinfo->mtx_.unlock();
        return flag;
    }
    sinfo = &mstable_[cinfo->slab_id_];
    sinfo->source_id_[sinfo->idx_] = source_id / CY_NUM;
    sinfo->metric_id_[sinfo->idx_] = metric_id / CX_NUM;
    sinfo->start_time_[sinfo->idx_] = start_time;
    sinfo->idx_++;
    cinfo->mtx_.unlock();
    return true;
}

auto ValueLog::GetDiskSlabID(uint32_t &sid, uint64_t source_id, uint16_t metric_id, uint64_t start_time) -> bool {
  SlabInfo* sinfo;
    auto flag = free_dsinfoq_->try_pop(sinfo);
  if(!flag){
    return flag;
  }
  sid = sinfo->slab_id_;
  nused_dsinfoq_.fetch_add(1);
  nfree_dsinfoq_.fetch_sub(1);
  used_dsinfoq_->push(sinfo);
  sinfo->free_ = false;
  sinfo->source_id_[0] = source_id;
  sinfo->metric_id_[0] = metric_id;
  sinfo->start_time_[0] = start_time;
  return flag;
}

auto ValueLog::MirrorPutKV(uint32_t &sid, uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time,
                          const uint8_t *value, uint32_t value_len, uint64_t txn) -> bool {
    SlabClass* cinfo = &ctable[metric_id%CX_NUM][source_id%CY_NUM];
    cinfo->mtx_.lock();
    SlabInfo* sinfo = nullptr;
    if(cinfo->slab_id_ == UINT32_MAX){
        auto flag = free_msinfoq_->try_pop(sinfo);
        if(!flag){
            SlabInfo* dsinfo = nullptr;
            std::pair<SlabInfo*, SlabInfo*> spair{sinfo, dsinfo} ;
            flag = mem_evict_sinfoq_->try_pop(spair);
            if (!flag ) {
                cinfo->mtx_.unlock();
                //  std::cout<<"*"<<cinfo->slab_id_<<"*"<<std::endl;
                //  std::cout<<"nmfree:"<<nfree_msinfoq_<<" nfull:"<<nfull_msinfoq_<<" nused:"<<nused_msinfoq_<<" nmevict:"<<n_mem_evict_sinfo_<<std::endl;
                return flag;
            }
            n_mem_evict_sinfo_.fetch_sub(1);
            nused_msinfoq_.fetch_add(1);
            auto tmp_msinfo = spair.first;
            auto tmp_dsinfo = spair.second;
            //assert(sinfo == spair.first);
            for (uint8_t j = 0; j < tmp_msinfo->idx_; j++) {
//                UpdateMT(GetSinfoSourceID(tmp_msinfo,j), GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->start_time_[j], tmp_dsinfo);
//                UpdateMT(GetSinfoSourceID(tmp_msinfo,j), GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->end_time_[j], tmp_dsinfo);
                UpdateDB(GetSinfoSourceID(tmp_msinfo,j),
                         GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->end_time_[j],false,tmp_dsinfo->slab_id_,0);
            }
            FreeSlab(tmp_msinfo);
            sid = tmp_msinfo->slab_id_;
            tmp_msinfo->free_ = false;
            cinfo->slab_id_ = tmp_msinfo->slab_id_;
        }else{
            sid = sinfo->slab_id_;
            nused_msinfoq_.fetch_add(1);
            nfree_msinfoq_.fetch_sub(1);
            sinfo->free_ = false;
            cinfo->slab_id_ = sinfo->slab_id_;
        }
    }
    sid = cinfo->slab_id_;
    sinfo = &mstable_[sid];
    sinfo->cid_y_ = source_id % CY_NUM;
    sinfo->cid_x_ = metric_id % CX_NUM;
    uint8_t f_sgid = source_id / CY_NUM;
    uint8_t f_mid = metric_id / CX_NUM;

    Item* item = WritableItem(sid);     // auto increase sinfo->nalloc_
    item->timestamp_ = start_time;
    if(value_len<slab::CHUNK_SIZE){
        memcpy(ItemKey(item),value,value_len);
    }else{
        memcpy(ItemKey(item), value, CHUNK_SIZE);
    }

    bool is_exist = false;
    uint8_t idx = 0;
    for(uint8_t i=0;i<sinfo->idx_;i++){
        if(sinfo->source_id_[i] == f_sgid && sinfo->metric_id_[i] == f_mid){
            is_exist = true;
            idx = i;
            break;
        }
    }
    if(is_exist){
//        RemoveMT(source_id,metric_id,sinfo->end_time_[idx]);
        DeleteDB(source_id, metric_id, sinfo->end_time_[idx]);
        sinfo->end_time_[idx] = end_time;
//        InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->end_time_[idx],sinfo);
        InsertDB(GetSinfoSourceID(sinfo, idx), GetSinfoMetricID(sinfo, idx), sinfo->end_time_[idx],
                 sinfo->mem_, sinfo->slab_id_, sinfo->nalloc_-1);
    }else{
        idx = sinfo->idx_;
        sinfo->idx_++;
        sinfo->metric_id_[idx] = f_mid;
        sinfo->source_id_[idx] = f_sgid;
        assert(GetSinfoSourceID(sinfo,idx)==source_id);
        assert(GetSinfoMetricID(sinfo,idx)==metric_id);
        sinfo->start_time_[idx]  = start_time;
        sinfo->end_time_[idx] = end_time;
//        InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->start_time_[idx],sinfo);
//        InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->end_time_[idx],sinfo);
        InsertDB(GetSinfoSourceID(sinfo, idx), GetSinfoMetricID(sinfo, idx), sinfo->end_time_[idx],
                 sinfo->mem_, sinfo->slab_id_, sinfo->nalloc_-1);
    }
    if(SlabFull(sinfo)){
        InsertMemFullQueue(sinfo);
        assert(&ctable[sinfo->cid_x_][sinfo->cid_y_]==cinfo);
        ctable[sinfo->cid_x_][sinfo->cid_y_].slab_id_ = UINT32_MAX;
    }
    cinfo->mtx_.unlock();
    return true;
}

auto ValueLog::ConPutKV(uint32_t &sid, uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time,
                          const uint8_t *value, uint32_t value_len, uint64_t txn) -> bool {
    SlabClass* cinfo = &ctable[metric_id%CX_NUM][source_id%CY_NUM];
    cinfo->mtx_.lock();
    SlabInfo* sinfo = nullptr;
    if(cinfo->slab_id_ == UINT32_MAX){
        auto flag = free_msinfoq_->try_pop(sinfo);
        if(!flag){
            SlabInfo* dsinfo = nullptr;
            std::pair<SlabInfo*, SlabInfo*> spair{sinfo, dsinfo} ;
            flag = mem_evict_sinfoq_->try_pop(spair);
            if (!flag ) {
                cinfo->mtx_.unlock();
              //  std::cout<<"*"<<cinfo->slab_id_<<"*"<<std::endl;
              //  std::cout<<"nmfree:"<<nfree_msinfoq_<<" nfull:"<<nfull_msinfoq_<<" nused:"<<nused_msinfoq_<<" nmevict:"<<n_mem_evict_sinfo_<<std::endl;
                return flag;
            }
            n_mem_evict_sinfo_.fetch_sub(1);
            nused_msinfoq_.fetch_add(1);
            auto tmp_msinfo = spair.first;
            auto tmp_dsinfo = spair.second;
            //assert(sinfo == spair.first);
            for (uint8_t j = 0; j < tmp_msinfo->idx_; j++) {
                UpdateMT(GetSinfoSourceID(tmp_msinfo,j), GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->start_time_[j], tmp_dsinfo);
                UpdateMT(GetSinfoSourceID(tmp_msinfo,j), GetSinfoMetricID(tmp_msinfo,j),tmp_msinfo->end_time_[j], tmp_dsinfo);
            }
            FreeSlab(tmp_msinfo);
            sid = tmp_msinfo->slab_id_;
            tmp_msinfo->free_ = false;
            cinfo->slab_id_ = tmp_msinfo->slab_id_;
        }else{
            sid = sinfo->slab_id_;
            nused_msinfoq_.fetch_add(1);
            nfree_msinfoq_.fetch_sub(1);
            sinfo->free_ = false;
            cinfo->slab_id_ = sinfo->slab_id_;
        }
    }
    sid = cinfo->slab_id_;
    sinfo = &mstable_[sid];
    sinfo->cid_y_ = source_id % CY_NUM;
    sinfo->cid_x_ = metric_id % CX_NUM;
    uint8_t f_sgid = source_id / CY_NUM;
    uint8_t f_mid = metric_id / CX_NUM;
    Item* item = WritableItem(sid);
    item->timestamp_ = start_time;
    if(value_len<slab::CHUNK_SIZE){
        memcpy(ItemKey(item),value,value_len);
    }else{
        memcpy(ItemKey(item), value, CHUNK_SIZE);
    }
    bool is_exist = false;
    uint8_t idx = 0;
    for(uint8_t i=0;i<sinfo->idx_;i++){
        if(sinfo->source_id_[i] == f_sgid && sinfo->metric_id_[i] == f_mid){
            is_exist = true;
            idx = i;
            break;
        }
    }
    if(is_exist){
        RemoveMT(source_id,metric_id,sinfo->end_time_[idx]);
        sinfo->end_time_[idx] = end_time;
        InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->end_time_[idx],sinfo);
    }else{
        idx = sinfo->idx_;
        sinfo->idx_++;
        sinfo->metric_id_[idx] = f_mid;
        sinfo->source_id_[idx] = f_sgid;
        assert(GetSinfoSourceID(sinfo,idx)==source_id);
        assert(GetSinfoMetricID(sinfo,idx)==metric_id);
        sinfo->start_time_[idx]  = start_time;
        sinfo->end_time_[idx] = end_time;
        InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->start_time_[idx],sinfo);
        InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->end_time_[idx],sinfo);
    }
    if(SlabFull(sinfo)){
        InsertMemFullQueue(sinfo);
        assert(&ctable[sinfo->cid_x_][sinfo->cid_y_]==cinfo);
        ctable[sinfo->cid_x_][sinfo->cid_y_].slab_id_ = UINT32_MAX;
    }
    cinfo->mtx_.unlock();
    return true;
}

auto ValueLog::PutKV(uint32_t& sid, uint64_t source_id, uint16_t metric_id, uint64_t key, uint64_t end_time,
                           const uint8_t *value, uint32_t value_len, uint64_t txn) -> bool {
  if (!ValidSid(sid)) {
    return false;
  }
  SlabInfo *sinfo = &mstable_[sid];
 // std::cout<<"***\n";
  sinfo->mtx_.lock();
  //std::cout<<"**\n";
  if (SlabFull(sinfo)) {
    //  std::cout<<"***********"<<sinfo->nalloc_<<"*****\n";
    //  std::cout<<"nmfree:"<<nfree_msinfoq_<<" nfull:"<<nfull_msinfoq_<<" nused:"<<nused_msinfoq_<<" nmevict:"<<n_mem_evict_sinfo_<<std::endl;
      sinfo->mtx_.unlock();
     return false;
  }
  uint8_t f_sgid = source_id / CY_NUM;
  uint8_t f_mid = metric_id / CX_NUM;
  Item* item = WritableItem(sid);
  uint8_t idx = 0;
  bool is_exist = false;
  for(uint8_t i=0;i<sinfo->idx_;i++){
      if(sinfo->metric_id_[i] == f_mid && sinfo->source_id_[i] == f_sgid){
          idx = i;
          is_exist = true;
          break;
      }
  }
  if(!is_exist){
      idx = sinfo->idx_;
      sinfo->idx_++;
      sinfo->metric_id_[idx] = f_mid;
      sinfo->source_id_[idx] = f_sgid;
      sinfo->start_time_[idx]  = key;
      std::cout<<"********"<<f_mid<<" "<<f_sgid<<std::endl;
      sinfo->txn_[idx] = (sinfo->txn_[idx] >= txn ? sinfo->txn_[idx] : txn);
      InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->start_time_[idx],sinfo);
  }
  if (item == nullptr) {
      sinfo->mtx_.unlock();
    return false;
  }
  item->timestamp_ = key;
  if(value_len<slab::CHUNK_SIZE){
    memcpy(ItemKey(item),value,value_len);
  }else{
    memcpy(ItemKey(item), value, CHUNK_SIZE);
  }
  sinfo->end_time_[idx] = end_time;
  if(SlabFull(sinfo)){
    InsertMemFullQueue(sinfo);
    for(uint8_t i=0;i<sinfo->idx_;i++){
        InsertMT(GetSinfoSourceID(sinfo,idx), GetSinfoMetricID(sinfo,idx),sinfo->end_time_[i],sinfo);
    }
    ctable[sinfo->cid_x_][sinfo->cid_y_].slab_id_ = UINT32_MAX;
  }
  sinfo->mtx_.unlock();
  return true;
}

auto ValueLog::WritableItem(const uint32_t sid) -> Item * {
  if (!ValidSid(sid)) {
    return nullptr;
  }
  SlabInfo* sinfo = &mstable_[sid];
  Slab* slab = GetMemSlab(sinfo->slab_id_);
  Item* item = ReadSlabItem(slab, sinfo->nalloc_.load(), SLAB_ITEM_SIZE);
  sinfo->nalloc_.fetch_add(1);

  return item;
}

auto ValueLog::GetMemSlab(const uint32_t sid) const -> Slab * {
  off_t off = static_cast<off_t>(sid) * setting_.slab_size_;
  Slab* slab = (Slab*)(mstart_ + off);
  return slab;
}

auto ValueLog::ReadSlabItem(const Slab* slab, const uint32_t idx, const size_t size) -> Item* {
  Item* item = (Item*)((uint8_t *)slab->data_ + (idx*size));
  return item;
}

auto ValueLog::SlabToDaddr(const SlabInfo* sinfo) const -> off_t {
  off_t off = dstart_ + (static_cast<off_t>(sinfo->slab_id_) * setting_.slab_size_);
  return off;
}

void ValueLog::ScheduleBGWrite() {
  if (!background_write_scheduled_) {
    background_write_scheduled_ = true;
    env_->Schedule(&ValueLog::BGWork, this);
  }
}

void ValueLog::BGWork(void* tree_series) {
  reinterpret_cast<ValueLog*>(tree_series)->BackGroundCall();
}

void ValueLog::BackGroundCall() {
  BatchWrite();
  background_write_scheduled_ = false;
  ScheduleBGWrite();
  background_work_finished_signal_.SignalAll();
}

auto ValueLog::BatchWrite() -> bool {
    std::vector<std::future<bool>> future_results;

    SlabInfo* full_msinfo_queue[setting_.write_batch_size_];
    if (full_msinfoq_->was_empty()) {
        return false;
    }
    uint32_t i = 0;
    uint32_t count = 0;
    while (full_msinfoq_->try_pop(full_msinfo_queue[i])) {
        count++;
        i++;
        if (count >= setting_.write_batch_size_ || full_msinfoq_->was_empty()) {
            break;
        }
    }

    if (free_dsinfoq_->was_size() < count) {
        for (uint32_t i = 0; i < count; i++) {
            full_msinfoq_->push(full_msinfo_queue[i]);
        }
        return false;
    }
    SlabInfo* free_dsinfo_queue[count];
    for (uint32_t i = 0; i < count; i++) {
        free_dsinfoq_->try_pop(free_dsinfo_queue[i]);
        used_dsinfoq_->push(free_dsinfo_queue[i]);
    }

    for (uint32_t i = 0; i < count; i++) {
        SlabInfo* dsinfo = free_dsinfo_queue[i];
        SlabInfo* msinfo = full_msinfo_queue[i];
        dsinfo->free_ = false;
        msinfo->free_ = true;
        uint32_t dsid = dsinfo->slab_id_;
        uint32_t msid = full_msinfo_queue[i]->slab_id_;
        future_results.emplace_back(this->pool_.enqueue([this, msid, dsid] {
            return WriteToSSD(msid, dsid);
        }));
    }
    for(auto &it : future_results){
        if (!it.get()) {
            return false;
        }
    }
    for(uint32_t i=0;i<count;i++){
        auto temp_dsinfo = free_dsinfo_queue[i];
        auto temp_msinfo = full_msinfo_queue[i];
        for(uint8_t i=0;i<temp_msinfo->idx_;i++){
//            UpdateMT(GetSinfoSourceID(temp_msinfo,i), GetSinfoMetricID(temp_dsinfo,i),temp_msinfo->start_time_[i],temp_dsinfo);
//            UpdateMT(GetSinfoSourceID(temp_msinfo,i), GetSinfoMetricID(temp_dsinfo,i),temp_msinfo->end_time_[i],temp_dsinfo);
            UpdateDB(GetSinfoSourceID(temp_msinfo,i),
                     GetSinfoMetricID(temp_dsinfo,i),temp_msinfo->end_time_[i],false,temp_dsinfo->slab_id_,0);
        }
        FreeSlab(temp_msinfo);
    }
    for (uint32_t i = 0; i < count; i++) {
        free_msinfoq_->push(full_msinfo_queue[i]);
    }
    nfree_msinfoq_.fetch_add(count);
    nfull_msinfoq_.fetch_sub(count);
    nfree_dsinfoq_.fetch_sub(count);
    nused_dsinfoq_.fetch_add(count);
    return true;
}

auto ValueLog::WriteToSSD(uint32_t msid, uint32_t dsid) -> bool {
  SlabInfo* dsinfo = &dstable_[dsid];
  Slab* slab = GetMemSlab(msid);
  size_t slab_size = setting_.slab_size_;
  off_t offset = SlabToDaddr(dsinfo);
  auto n = pwrite(fd_, slab, slab_size, offset);
  if (static_cast<size_t>(n) < slab_size) {
    throw std::logic_error("fail in WriteToSSD");
  }
  return true;
}

auto FindSinfoSubSequence(SlabInfo** full_msinfo_wqueue, uint32_t count) -> std::vector<std::pair<uint32_t, uint32_t>> {
    int threshold = 2;
    std::vector<std::pair<uint32_t, uint32_t>> subSequence;
    uint32_t start = 0;
    uint32_t end = 0;
    if(start == count){
        subSequence.emplace_back(std::make_pair(start,end));
        return subSequence;
    }
    while (start < count){
        if(full_msinfo_wqueue[end]->slab_id_ + 1 == full_msinfo_wqueue[end+1]->slab_id_){
            end ++;
            if(end == count){
                subSequence.emplace_back(std::make_pair(start,end));
                break;
            }
        }else{
            subSequence.emplace_back(std::make_pair(start,end));
            end ++;
            start = end;
        }
    }
    return subSequence;
}

bool compareSlabInfo(const slab::SlabInfo* a, const slab::SlabInfo* b) {
    return a->slab_id_ < b->slab_id_;
}

auto ValueLog::OptBatchWrite() -> bool {
    if(nfull_msinfoq_.load() < setting_.write_batch_size_ && nfree_dsinfoq_.load() < setting_.write_batch_size_){
        return false;
    }
    std::vector<std::future<bool>> future_results;
    SlabInfo* full_msinfo_wqueue[setting_.write_batch_size_];

    if (full_msinfoq_->was_empty()) {
        return false;
    }
    uint32_t i = 0;
    uint32_t count = 0;
    while (full_msinfoq_->try_pop(full_msinfo_wqueue[i])) {
        count++;
        i++;
        if (count >= setting_.write_batch_size_ || full_msinfoq_->was_empty()) {
            break;
        }
    }

    if (free_dsinfoq_->was_size() < count) {
        for (uint32_t i = 0; i < count; i++) {
            full_msinfoq_->push(full_msinfo_wqueue[i]);
        }
        return false;
    }
    SlabInfo* free_dsinfo_wqueue[count];
    for (uint32_t i = 0; i < count; i++) {
        free_dsinfoq_->try_pop(free_dsinfo_wqueue[i]);
    }

    future_results.emplace_back(this->pool_.enqueue([this] {
        return WriteDiskSlabInfo();
    }));

    std::stable_sort(full_msinfo_wqueue, full_msinfo_wqueue+count, compareSlabInfo);
    auto subSequence = FindSinfoSubSequence(full_msinfo_wqueue, count-1);
    for(auto &it:subSequence){
        if((it.second-it.first)==(free_dsinfo_wqueue[it.second]->slab_id_-free_dsinfo_wqueue[it.first]->slab_id_)){
            auto dsinfo = free_dsinfo_wqueue[it.first];
            auto msinfo = full_msinfo_wqueue[it.first];
            future_results.emplace_back(this->pool_.enqueue([this, dsinfo, msinfo,it] {
                return OptWriteToSSD(msinfo->slab_id_, dsinfo->slab_id_,it.second-it.first+1);
            }));
            for(uint32_t j=it.first;j<=it.second;j++){
                auto dsinfo = free_dsinfo_wqueue[j];
                auto msinfo = full_msinfo_wqueue[j];
                CopyInfo(dsinfo,msinfo);
                used_dsinfoq_->push(dsinfo);
            }
        }else{
            uint32_t midx = it.first;
            uint32_t didx = it.first;
            while(midx<=it.second&&didx<=it.second){
                auto dsinfo = free_dsinfo_wqueue[didx];
                auto msinfo = full_msinfo_wqueue[midx];
                CopyInfo(dsinfo,msinfo);
                future_results.emplace_back(this->pool_.enqueue([this, dsinfo, msinfo] {
                    return OptWriteToSSD(msinfo->slab_id_, dsinfo->slab_id_,1);
                }));
                midx++;
                didx++;
                used_dsinfoq_->push(dsinfo);
            }
        }
    }
    for(uint32_t i=0;i<count;i++){
        auto temp_dsinfo = free_dsinfo_wqueue[i];
        auto temp_msinfo = full_msinfo_wqueue[i];
        for(uint8_t i=0;i<temp_msinfo->idx_;i++){
//            UpdateMT(GetSinfoSourceID(temp_msinfo,i), GetSinfoMetricID(temp_dsinfo,i),temp_msinfo->start_time_[i],temp_dsinfo);
//            UpdateMT(GetSinfoSourceID(temp_msinfo,i), GetSinfoMetricID(temp_dsinfo,i),temp_msinfo->end_time_[i],temp_dsinfo);
            UpdateDB(GetSinfoSourceID(temp_msinfo,i),
                     GetSinfoMetricID(temp_dsinfo,i),temp_msinfo->end_time_[i],false,temp_dsinfo->slab_id_,0);
        }
        FreeSlab(temp_msinfo);
    }
    for (uint32_t i = 0; i < count; i++) {
        free_msinfoq_->push(full_msinfo_wqueue[i]);
    }
    nfree_msinfoq_.fetch_add(count);
    nfull_msinfoq_.fetch_sub(count);
    nfree_dsinfoq_.fetch_sub(count);
    nused_dsinfoq_.fetch_add(count);
    for(auto &it : future_results){
        if (!it.get()) {
            return false;
        }
    }
    return true;
}

    auto ValueLog::OptBatchWriteLazyFlush() -> bool {
        if(nfull_msinfoq_.load() < setting_.write_batch_size_ && (nfree_dsinfoq_.load()+n_disk_evict_sinfo_.load()) < setting_.write_batch_size_){
            return false;
        }
        std::vector<std::future<bool>> future_results;
        SlabInfo* full_msinfo_wqueue[setting_.write_batch_size_];

        if (full_msinfoq_->was_empty()) {
            return false;
        }
        uint32_t i = 0;
        uint32_t count = 0;
        while (full_msinfoq_->try_pop(full_msinfo_wqueue[i])) {
            count++;
            i++;
            if (count >= setting_.write_batch_size_ || full_msinfoq_->was_empty()) {
                break;
            }
        }

        if ((free_dsinfoq_->was_size()+disk_evict_sinfoq_->was_size()) < count) {
            for (uint32_t i = 0; i < count; i++) {
                full_msinfoq_->push(full_msinfo_wqueue[i]);
            }
            return false;
        }
        SlabInfo* free_dsinfo_wqueue[count];
        uint32_t fi = 0;
        uint32_t num = std::min(count, nfree_dsinfoq_.load());
        for (; fi < num; fi++) {
            free_dsinfoq_->try_pop(free_dsinfo_wqueue[fi]);
            nfree_dsinfoq_.fetch_sub(1);
        }
        std::cout<<"fi: "<<fi<<"count: "<<count<<std::endl;
        for (; fi < count; fi++) {
            //assert(false);
            disk_evict_sinfoq_->try_pop(free_dsinfo_wqueue[fi]);
            n_disk_evict_sinfo_.fetch_sub(1);
            for(uint8_t j=0;j<free_dsinfo_wqueue[fi]->idx_;j++){
//                RemoveMT(GetSinfoSourceID(free_dsinfo_wqueue[fi],i), GetSinfoMetricID(free_dsinfo_wqueue[fi],i),free_dsinfo_wqueue[fi]->start_time_[i]);
//                RemoveMT(GetSinfoSourceID(free_dsinfo_wqueue[fi],i), GetSinfoMetricID(free_dsinfo_wqueue[fi],i),free_dsinfo_wqueue[fi]->end_time_[i]);
                DeleteDB(GetSinfoSourceID(free_dsinfo_wqueue[fi],i),
                         GetSinfoMetricID(free_dsinfo_wqueue[fi],i),free_dsinfo_wqueue[fi]->end_time_[i]);
            }
            FreeSlab(free_dsinfo_wqueue[fi]);
        }
        std::stable_sort(free_dsinfo_wqueue, free_dsinfo_wqueue+count, compareSlabInfo);

        future_results.emplace_back(this->pool_.enqueue([this] {
            return WriteDiskSlabInfo();
        }));

        std::stable_sort(full_msinfo_wqueue, full_msinfo_wqueue+count, compareSlabInfo);
        auto subSequence = FindSinfoSubSequence(full_msinfo_wqueue, count-1);
        for(auto &it:subSequence){
            if((it.second-it.first)==(free_dsinfo_wqueue[it.second]->slab_id_-free_dsinfo_wqueue[it.first]->slab_id_)){
                auto dsinfo = free_dsinfo_wqueue[it.first];
                auto msinfo = full_msinfo_wqueue[it.first];
                future_results.emplace_back(this->pool_.enqueue([this, dsinfo, msinfo,it] {
                    return OptWriteToSSD(msinfo->slab_id_, dsinfo->slab_id_,it.second-it.first+1);
                }));
                for(uint32_t j=it.first;j<=it.second;j++){
                    auto dsinfo = free_dsinfo_wqueue[j];
                    auto msinfo = full_msinfo_wqueue[j];
                    CopyInfo(dsinfo,msinfo);
                    mem_evict_sinfoq_->push(std::make_pair(msinfo, dsinfo));
                    used_dsinfoq_->push(dsinfo);
                }
            }else{
                uint32_t midx = it.first;
                uint32_t didx = it.first;
                while(midx<=it.second&&didx<=it.second){
                    auto dsinfo = free_dsinfo_wqueue[didx];
                    auto msinfo = full_msinfo_wqueue[midx];
                    CopyInfo(dsinfo,msinfo);
                    future_results.emplace_back(this->pool_.enqueue([this, dsinfo, msinfo] {
                        return OptWriteToSSD(msinfo->slab_id_, dsinfo->slab_id_,1);
                    }));
                    midx++;
                    didx++;
                    mem_evict_sinfoq_->push(std::make_pair(msinfo, dsinfo));
                    used_dsinfoq_->push(dsinfo);
                }
            }
        }

        nfull_msinfoq_.fetch_sub(count);
        nused_dsinfoq_.fetch_add(count);
        n_mem_evict_sinfo_.fetch_add(count);

        for(auto &it : future_results){
            if (!it.get()) {
                return false;
            }
        }
        return true;
    }

    auto ValueLog::ForceFlush() -> bool {
        for (uint32_t i = 0; i < nmslab_; i++) {
            auto sinfo = GetMemSlabInfo(i);
            if (!sinfo->free_ &&  sinfo->nalloc_.load()!=MAX_ALLOC_ITEM) {
                InsertMemFullQueue(sinfo);
            }
        }
        return true;
    }

auto ValueLog::OptWriteToSSD(uint32_t msid, uint32_t dsid, uint32_t slab_num) -> bool {
  SlabInfo* dsinfo = &dstable_[dsid];
  Slab* slab = GetMemSlab(msid);
  size_t slab_size = setting_.slab_size_;
  off_t offset = SlabToDaddr(dsinfo);
  auto n = pwrite(fd_, slab, slab_size*slab_num, offset);
  if(n==-1){
      std::cout<<"msid: "<<msid<<" dsid: "<<dsid<<" slab_num: "<<slab_num<<std::endl;
      std::cout<<"total_write_bytes: "<<slab_total_write_bytes<<" n: "<<n<<std::endl;
      assert(false);
  }
    slab_total_write_bytes += n;
  if (static_cast<size_t>(n) < slab_size*slab_num) {
    throw std::logic_error("fail in WriteToSSD");
  }
  return true;
}

// auto ValueLog::WriteDiskSlabInfo() -> bool {
//   SlabInfo* disk_slab_info = dstable_;
//   uint32_t size = sizeof(SlabInfo)*ndslab_;
//   auto n = pwrite(fd_info_, disk_slab_info, size, 0);
//   if (static_cast<size_t>(n) < size) {
//     std::cout<<n<<"  xx  "<<size<<std::endl;
//     throw std::logic_error("fail in WriteDiskSlabInfo");
//   }
//   // if (n == -1) {
//   //   throw std::logic_error("fail in WriteDiskSlabInfo");
//   // }
//   return true;
// }

auto ValueLog::WriteDiskSlabInfo() -> bool {
  const void* buf      = dstable_;
  uint64_t    remain   = sizeof(SlabInfo) * ndslab_;
  uint64_t    offset   = 0;
  constexpr uint64_t kChunk = 1ULL << 30;
  while (remain > 0) {
    uint64_t to_write = std::min(remain, kChunk);
    ssize_t written = pwrite(fd_info_,
                             static_cast<const char*>(buf) + offset,
                             to_write,
                             offset);
    if (written < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error("pwrite failed: " +
                               std::string(strerror(errno)));
    }
    if (written == 0) {
      throw std::runtime_error("pwrite returned 0 (disk full?)");
    }
    remain  -= written;
    offset  += written;
  }

  return true;
}


auto ValueLog::RecoveryDiskSlabInfo() -> bool {
  uint32_t size = sizeof(SlabInfo)*ndslab_;
  SlabInfo* disk_slab_info = dstable_;
  auto n = pread(fd_info_, disk_slab_info, size, 0);
  if (n < size) {
    throw std::logic_error("fail in RecoveryDiskSlabInfo");
  }
  for(uint32_t i=0;i<ndslab_;i++){
      SlabInfo *sinfo = &dstable_[i];
      if(!sinfo->free_){
          for(uint8_t j=0;j<sinfo->idx_;j++){
              InsertMT(GetSinfoSourceID(sinfo,j), GetSinfoMetricID(sinfo,j),sinfo->start_time_[j],sinfo);
              InsertMT(GetSinfoSourceID(sinfo,j), GetSinfoMetricID(sinfo,j),sinfo->end_time_[j],sinfo);
          }
      }
  }
  return true;
}

auto ValueLog::RecoveryDiskSlabInfoMirror() -> bool {
    uint32_t size = sizeof(SlabInfo)*ndslab_;
    SlabInfo* disk_slab_info = dstable_;
    auto n = pread(fd_info_, disk_slab_info, size, 0);
    if (n < size) {
        throw std::logic_error("fail in RecoveryDiskSlabInfo");
    }
    for(uint32_t i=0;i<ndslab_;i++){
        SlabInfo *sinfo = &dstable_[i];
        if(!sinfo->free_){
            for(uint8_t j=0;j<sinfo->idx_;j++){
                InsertDB(GetSinfoSourceID(sinfo,j), GetSinfoMetricID(sinfo,j),sinfo->end_time_[j],sinfo->mem_,sinfo->slab_id_,0);
            }
        }
    }
    return true;
}

auto ValueLog::TraverseMasstree() -> std::vector<std::pair<std::string, const SlabInfo *>> {
    std::vector<std::pair<std::string, const SlabInfo*>> kv_arr;
    std::string start_key, end_key;
    EnCodeKey(&start_key, 0, 0, 0);
    EnCodeKey(&end_key, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint16_t>::max(), std::numeric_limits<uint64_t>::max());

    mass_tree_.scan(start_key.c_str(), start_key.size(), false, end_key.c_str(), end_key.size(), true,
                    {
                            [](const MasstreeWrapper<SlabInfo>::leaf_type *leaf, uint64_t version,
                               bool &continue_flag) {
                                (void)leaf;
                                (void)version;
                                (void)continue_flag;
                                return;
                            },
                            [&kv_arr](const MasstreeWrapper<SlabInfo>::Str &key, const SlabInfo *val, bool &continue_flag) {
                                kv_arr.emplace_back(key, val);

                                (void)val;
                                (void)continue_flag;
                                return;
                            }});
    return kv_arr;
}

// todo
auto ValueLog::WriteDiskMasstree() -> bool {
    auto kv_arr = TraverseMasstree();

    for (auto &kv : kv_arr) {
        auto enckey = kv.first;
        auto sinfo = kv.second;
        leveldb::PutFixed32(&enckey, sinfo->slab_id_);
        enckey.append(static_cast<uint8_t>(sinfo->mem_), 1);

    }


    return true;
}

auto ValueLog::ReadSlabItem(SlabInfo* sinfo, uint32_t idx) -> std::pair<Item *, uint32_t > {
  Slab* slab = nullptr;
  if(sinfo->mem_){
    slab = GetMemSlab(sinfo->slab_id_);
  }else{
    slab = ReadDiskSlab(sinfo);
  }
  return std::make_pair(ReadSlabItem(slab, idx, SLAB_ITEM_SIZE), sinfo->nalloc_-idx);
}

auto ValueLog::ReadDiskSlab(slab::SlabInfo *sinfo) -> Slab * {
  Slab* buf_addr;
    while(!free_bsinfoq_->try_pop(buf_addr)){}
  nfree_bsinfo_.fetch_sub(1);
  off_t off = SlabToDaddr(sinfo);
  auto n = pread(fd_, buf_addr, SLAB_SIZE, off);
  if (n < SLAB_SIZE) {
    return nullptr;
  }
  return (Slab*)(buf_addr);
}
auto ValueLog::ReadDiskSlab(const slab::SlabInfo *sinfo) -> Slab * {
  Slab* buf_addr;
    while(!free_bsinfoq_->try_pop(buf_addr)){}
  nfree_bsinfo_.fetch_sub(1);
  off_t off = SlabToDaddr(sinfo);
  auto n = pread(fd_, buf_addr, SLAB_SIZE, off);
  if (n < SLAB_SIZE) {
    return nullptr;
  }
  return (Slab*)(buf_addr);
}

auto ValueLog::BatchRead(std::vector<SlabInfo*> &sinfo_q,std::vector<Slab*> &slab_arr) -> void {
  std::vector<std::future<Slab*>> future_results;
  for (auto it : sinfo_q) {
    future_results.emplace_back(this->pool_.enqueue([this, it]{
      return ReadDiskSlab(it);
    }));
  }
  for (auto &future :future_results) {
    slab_arr.emplace_back(future.get());
  }
}

auto ValueLog::BatchDiskRead(slab::SlabInfo* sinfo_arr[], uint32_t count, std::vector<Slab *> &slab_arr) -> void {
    std::vector<std::future<Slab*>> future_results;
    for (uint32_t i=0;i<count;i++) {
        future_results.emplace_back(this->pool_.enqueue([this, sinfo_arr,i]{
            return ReadDiskSlab(sinfo_arr[i]);
        }));
    }
    for (auto &future :future_results) {
        slab_arr.emplace_back(future.get());
    }
}

auto ValueLog::SequentialDiskRead(SlabInfo* sinfo_arr[], uint32_t count, std::vector<Slab*>& slab_arr) -> void {
    off_t off = SlabToDaddr(sinfo_arr[0]);
    auto n = pread(fd_, seq_read_buf_, SLAB_SIZE * count, off);
    if (static_cast<size_t>(n) < SLAB_SIZE*count) {
        throw std::logic_error("fail in WriteToSSD");
    }
    for (uint32_t i = 0; i < count; i++) {
        slab_arr.emplace_back((Slab*)(seq_read_buf_+ i*SLAB_SIZE));
    }
}

auto ValueLog::MigrateRead(std::vector<std::pair<SlabInfo *, Slab *>>&migrate_slab_array, bool& is_seq) -> bool {
    if(nused_dsinfoq_.load() < setting_.migrate_batch_size_){
        return false;
    }
    std::vector<std::future<Slab*>> future_results;
    SlabInfo* used_dsinfo_queue[nbuf_slab_];
    uint32_t i = 0;
    uint32_t count = 0;
    if (used_dsinfoq_->was_size() < nbuf_slab_) {
        return false;
    }
    while (used_dsinfoq_->try_pop(used_dsinfo_queue[i])) {
        count++;
        i++;
        if (count >= nbuf_slab_ || used_dsinfoq_->was_empty()) {
            break;
        }
    }
    nused_dsinfoq_.fetch_sub(count);
    std::vector<Slab*>slab_arr;

    if (used_dsinfo_queue[count-1]->slab_id_ - used_dsinfo_queue[0]->slab_id_ + 1 == count) {
        SequentialDiskRead(used_dsinfo_queue, count, slab_arr);
        is_seq = true;
    } else {
        BatchDiskRead(used_dsinfo_queue, count, slab_arr);
        is_seq = false;
    }

    if(slab_arr.size() != count){
        for (uint32_t i = 0; i < count; i++) {
            used_dsinfoq_->push(used_dsinfo_queue[i]);
        }
        return false;
    }
    for(uint32_t i=0;i<count;i++){
        migrate_slab_array.emplace_back(used_dsinfo_queue[i],slab_arr[i]);
    }

    for(uint32_t i=0;i<count;i++){
        for(uint8_t j=0;j<used_dsinfo_queue[i]->idx_;j++){
//            RemoveMT(GetSinfoSourceID(used_dsinfo_queue[i],j), GetSinfoMetricID(used_dsinfo_queue[i],j),used_dsinfo_queue[i]->start_time_[j]);
//            RemoveMT(GetSinfoSourceID(used_dsinfo_queue[i],j), GetSinfoMetricID(used_dsinfo_queue[i],j),used_dsinfo_queue[i]->end_time_[j]);
            DeleteDB(GetSinfoSourceID(used_dsinfo_queue[i],j),
                     GetSinfoMetricID(used_dsinfo_queue[i],j),used_dsinfo_queue[i]->end_time_[j]);
        }
    }
    for (uint32_t i = 0; i < count; i++) {
        free_dsinfoq_->push(used_dsinfo_queue[i]);
    }
    nfree_dsinfoq_.fetch_add(count);
    std::cout<<"nmfree: "<<nfree_msinfoq_<<" nmused: "<<nused_msinfoq_<<" nmfull: "<<nfull_msinfoq_<<std::endl;
    std::cout<<"ndfree: "<<nfree_dsinfoq_<<" ndused: "<<nused_dsinfoq_<<std::endl;
    std::cout<<"nbfree: "<<nfree_bsinfo_<<std::endl;
    return true;
}

    auto ValueLog::MigrateReadLazyFlush(std::vector<std::pair<SlabInfo *, Slab *>>&migrate_slab_array, bool& is_seq) -> bool {
        if(nused_dsinfoq_.load() < setting_.migrate_batch_size_){
            return false;
        }
        std::vector<std::future<Slab*>> future_results;
        SlabInfo* used_dsinfo_queue[nbuf_slab_];
        uint32_t i = 0;
        uint32_t count = 0;
        if (used_dsinfoq_->was_size() < nbuf_slab_) {
            return false;
        }
        while (used_dsinfoq_->try_pop(used_dsinfo_queue[i])) {
            count++;
            i++;
            if (count >= nbuf_slab_ || used_dsinfoq_->was_empty()) {
                break;
            }
        }

        nused_dsinfoq_.fetch_sub(count);
        std::vector<Slab*>slab_arr;

        if (used_dsinfo_queue[count-1]->slab_id_ - used_dsinfo_queue[0]->slab_id_ + 1 == count) {
            SequentialDiskRead(used_dsinfo_queue, count, slab_arr);
            is_seq = true;
        } else {
            BatchDiskRead(used_dsinfo_queue, count, slab_arr);
            is_seq = false;
        }

        if(slab_arr.size() != count){
            for (uint32_t i = 0; i < count; i++) {
                used_dsinfoq_->push(used_dsinfo_queue[i]);
            }
            return false;
        }
        for(uint32_t i=0;i<count;i++){
            migrate_slab_array.emplace_back(used_dsinfo_queue[i],slab_arr[i]);
        }

        for (uint32_t i = 0; i < count; i++) {
            disk_evict_sinfoq_->push(used_dsinfo_queue[i]);
        }
        n_disk_evict_sinfo_.fetch_add(count);

        std::cout<<"nmfree: "<<nfree_msinfoq_<<" nmused: "<<nused_msinfoq_<<" nmfull: "<<nfull_msinfoq_<<std::endl;
        std::cout<<"ndfree: "<<nfree_dsinfoq_<<" ndused: "<<nused_dsinfoq_<<std::endl;
        std::cout<<"nbfree: "<<nfree_bsinfo_<<std::endl;
        std::cout<<"nmevict: "<<n_mem_evict_sinfo_<<std::endl;
        std::cout<<"ndevict: "<<n_disk_evict_sinfo_<<std::endl;
        return true;
    }

auto ValueLog::InsertMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
  std::string packed_key;
  EnCodeKey(&packed_key,source_id,metric_id,ts);
  return mass_tree_.insert_value(packed_key.c_str(), packed_key.size(), sinfo);
}

auto ValueLog::RemoveMT(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool {
  std::string packed_key;
  EnCodeKey(&packed_key,source_id,metric_id,ts);
  return mass_tree_.remove_value(packed_key.c_str(), packed_key.size());
}

auto ValueLog::SearchMT(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> SlabInfo* {
  std::string packed_key;
  EnCodeKey(&packed_key,source_id,metric_id,ts);
  return mass_tree_.get_value(packed_key.c_str(), packed_key.size());
}

auto ValueLog::UpdateMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
  std::string packed_key;
  EnCodeKey(&packed_key,source_id,metric_id,ts);
  return mass_tree_.update_value(packed_key.c_str(), packed_key.size(), sinfo, false);
}

auto ValueLog::ScanMT(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time, std::vector<const SlabInfo*>&sinfo_arr) -> void {
  std::string start_key, end_key;
  EnCodeKey(&start_key, source_id, metric_id, start_time);
  EnCodeKey(&end_key, source_id, metric_id, end_time);

  mass_tree_.scan(start_key.c_str(), start_key.size(), false, end_key.c_str(), end_key.size(), true,
                  {
                      [](const MasstreeWrapper<SlabInfo>::leaf_type *leaf, uint64_t version,
                         bool &continue_flag) {
                        (void)leaf;
                        (void)version;
                        (void)continue_flag;
                        return;
                      },
                      [&sinfo_arr](const MasstreeWrapper<SlabInfo>::Str &key, const SlabInfo *val, bool &continue_flag) {
                        if(sinfo_arr.size()!=0){
                          if(val != sinfo_arr[sinfo_arr.size()-1]){
                            sinfo_arr.emplace_back(val);
                          }
                        }else{
                          sinfo_arr.emplace_back(val);
                        }
                        (void)val;
                        (void)continue_flag;
                        return;
                      }});
}

auto ValueLog::InsertDB(uint64_t source_id, uint16_t metric_id, uint64_t ts, bool mem, uint32_t sid, uint8_t item_idx) -> bool {
    std::string key;
    encode_l1key(&key, source_id, metric_id, ts);
    std::string val;
    encode_l1val(&val, mem, sid, item_idx);
    auto status = db_->Put(write_option_, key, val);
    return status.ok();
}

auto ValueLog::DeleteDB(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool {
    std::string key;
    encode_l1key(&key, source_id, metric_id, ts);
    auto status = db_->Delete(write_option_, key);
    return status.ok();
}

auto ValueLog::SearchDB(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> std::pair<SlabInfo*, uint8_t> {
    std::string key;
    encode_l1key(&key, source_id, metric_id, ts);
    std::string val;
    auto status = db_->Get(read_option_, key, &val);
    bool mem;
    uint32_t sid;
    uint8_t item_idx;
    decode_l1val(val, mem, sid, item_idx);
    SlabInfo* sinfo = nullptr;
    if (mem) {
        sinfo = &mstable_[sid];
    } else {
        sinfo = &dstable_[sid];
    }
    return std::make_pair(sinfo, item_idx);
}

auto ValueLog::UpdateDB(uint64_t source_id, uint16_t metric_id, uint64_t ts, bool mem, uint32_t sid, uint8_t item_idx) -> bool {
    std::string key;
    encode_l1key(&key, source_id, metric_id, ts);
    std::string val;
    encode_l1val(&val, mem, sid, item_idx);
    auto status = db_->Put(write_option_, key, val);
    return status.ok();
}

// refer to querier/tsdb_querier.cc
auto ValueLog::ScanDB(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time,
                        std::vector<const SlabInfo *> &sinfo_arr) -> void {
    auto iter = db_->NewIterator(read_option_);
    std::string key;
    encode_l1key(&key, source_id, metric_id, start_time);
    leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
    // find the first key which end_time >= query start_time, thus the previous key's end_time < query start_time
    // no need to search in previous key-value
    iter->Seek(lk.internal_key());
    if (!iter->Valid()) {
        return;
    }
//    iter->Prev();
//    if (!iter->Valid()) {
//        iter->Seek(lk.internal_key());
//    }

//    uint64_t cnt = 0;
    bool last = false;
    for (; iter->Valid(); iter->Next()) {
        uint64_t sgid;
        uint16_t mid;
        uint64_t ts;
        std::string key(iter->key().data(), iter->key().size());
        decode_l1key(key, sgid, mid, ts);
        if (sgid != source_id || mid != metric_id) {
            break;
        }
        if (ts >= end_time) {
            last = true;
        }

        std::string val(iter->value().data(), iter->value().size());
        bool mem;
        uint32_t sid;
        uint8_t item_idx;
        decode_l1val(val, mem, sid, item_idx);

//        std::cout<<"sgid:"<<sgid<<"mid:"<<mid<<"ts:"<<ts<<"sid:"<<sid<<" \n";
//        cnt++;

        SlabInfo* sinfo = nullptr;
        if (mem) {
            sinfo = &mstable_[sid];
        } else {
            sinfo = &dstable_[sid];
        }
        sinfo_arr.emplace_back(sinfo);

        // if iter already finds the last block that may contain query time range
        if (last) {
            return;
        }
    }

//    std::cout<<"query count: "<<cnt<<std::endl;
    return;
}

auto ValueLog::TraverseDB() -> void {
    auto iter = db_->NewIterator(read_option_);
    std::vector<std::string> keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        uint64_t sgid;
        uint16_t mid;
        uint64_t ts;
        std::string key(iter->key().data(), iter->key().size());
        keys.push_back(key);
        decode_l1key(key, sgid, mid, ts);
        std::string val(iter->value().data(), iter->value().size());
        bool mem;
        uint32_t sid;
        uint8_t item_idx;
        decode_l1val(val, mem, sid, item_idx);
        std::cout<<"sgid: "<<sgid<<" mid: "<<mid<<" end_time: "<<ts<<" sid: "<<sid<<" item_idx: "<<item_idx<<std::endl;
    }
}

auto ValueLog::InsertMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
  this->pool_.enqueue([this, source_id,metric_id, ts,sinfo]{
    this->InsertMT( source_id,metric_id, ts,sinfo);
  });
  return true;
}

auto ValueLog::RemoveMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool {
  this->pool_.enqueue([this, source_id,metric_id, ts]{
    this->RemoveMT( source_id,metric_id, ts);
  });
  return true;
}

auto ValueLog::UpdateMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
  this->pool_.enqueue([this, source_id,metric_id, ts,sinfo]{
    this->UpdateMT( source_id,metric_id, ts,sinfo);
  });
  return true;
}

auto ValueLog::ScanMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time) -> std::vector<const SlabInfo*> {
  std::vector<const SlabInfo*> sinfo_arr;
  this->pool_.enqueue([this, source_id,metric_id, start_time,end_time, &sinfo_arr]{
   this->ScanMT( source_id,metric_id, start_time,end_time,sinfo_arr);
  });
  return sinfo_arr;
}

auto ValueLog::Scan(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time, std::vector<std::pair<const SlabInfo*,Slab*>>& slab_arr) -> bool {
  std::vector<const SlabInfo*> sinfo_arr;
//    ScanMT(source_id,metric_id,start_time,end_time,sinfo_arr);
  ScanDB(source_id, metric_id, start_time, end_time, sinfo_arr);
  std::vector<std::variant<Slab*,std::future<Slab*>>>future_slab_arr;
  for(auto &it:sinfo_arr){
      if (it->slab_id_ == std::numeric_limits<uint32_t>::max()) continue;
    if(it->mem_){
      future_slab_arr.emplace_back(GetMemSlab(it->slab_id_));
    }else{
      future_slab_arr.emplace_back(pool_.enqueue([this,it]{
        return this->ReadDiskSlab(it);
      }));
    }
  }
  uint32_t idx = 0;
  for(auto &it:future_slab_arr){
    if (std::holds_alternative<Slab*>(it)){
      std::pair<const SlabInfo *, Slab *> item = std::make_pair(sinfo_arr[idx],std::get<Slab*>(it));
      slab_arr.emplace_back(item);
    }else{
      std::pair<const SlabInfo *, Slab *> item = std::make_pair(sinfo_arr[idx],std::get<std::future<Slab*>>(it).get());
      slab_arr.emplace_back(item);
    }
    idx++;
  }
  return true;
}

auto ValueLog::BatchRemoveMT(std::vector<SlabInfo*> sinfo_arr) -> bool {
  for (auto &it : sinfo_arr) {
    for(uint8_t i=0;i<it->idx_;i++){
     //   RemoveMTAsyn(it->source_id_[i], it->metric_id_[i], it->start_time_[i]);
        RemoveMTAsyn(GetSinfoSourceID(it,i), GetSinfoMetricID(it,i), it->start_time_[i]);
    }
  }
  return true;
}

auto ValueLog::BatchInsertMT(std::vector<SlabInfo*> sinfo_arr) -> bool {
  for (auto &it : sinfo_arr) {
      for(uint8_t i=0;i<it->idx_;i++){
      //    InsertMTAsyn(it->source_id_[i], it->metric_id_[i], it->start_time_[i],it);
          InsertMTAsyn(GetSinfoSourceID(it,i), GetSinfoMetricID(it,i), it->start_time_[i],it);
      }
  }
  return true;
}

auto ValueLog::DecodeChunk(uint8_t *chunk) -> std::pair<std::vector<int64_t>, std::vector<double>> {
    if (chunk == nullptr) return std::make_pair(std::vector<int64_t>(), std::vector<double>());

    std::vector<int64_t> t;
    std::vector<double> v;

    auto s = leveldb::Slice(reinterpret_cast<const char *>(chunk), slab::CHUNK_SIZE);
    leveldb::Slice tmp_value;
    uint64_t tmp;
    uint16_t mid = leveldb::DecodeFixed16(s.data() + 1);
    uint64_t sgid = leveldb::DecodeFixed64BE(s.data() + 3);
    tmp_value = leveldb::Slice(s.data() + 11, s.size() - 11);

    tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()), tmp_value.size());
    auto iter = c.xor_iterator();
    while (iter->next()) {
        t.push_back(iter->at().first);
        v.push_back((iter->at().second));
    }
    return std::make_pair(t, v);
}

auto ValueLog::PrintChunk(uint8_t *chunk) -> void {
    if (chunk == nullptr) return;
    auto s = leveldb::Slice(reinterpret_cast<const char *>(chunk), slab::CHUNK_SIZE);
    leveldb::Slice tmp_value;
        uint64_t tmp;
        uint16_t mid = leveldb::DecodeFixed16(s.data() + 1);
        uint64_t sgid = leveldb::DecodeFixed64BE(s.data() + 3);
        tmp_value = leveldb::Slice(s.data() + 11, s.size() - 11);

    tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()), tmp_value.size());
    auto iter = c.xor_iterator();
    uint32_t i = 0;
    while (iter->next()) {
        uint64_t ts = iter->at().first;
        double val = iter->at().second;
        std::cout << "i: " << i << " ts: " << ts << " val: " << val << std::endl;
        i++;
    }
}

auto ValueLog::PrintItem(Item* item) -> void {
  if (item == nullptr) return;
  uint64_t timestamp = item->timestamp_;
  std::cout<< "Item start timestamp: " << timestamp <<std::endl;
  PrintChunk(item->chunk_);
  std::cout<<"OK\n";
}

auto ValueLog::PrintSlab(uint32_t sid) -> void {
  std::cout << "********** slab " << sid <<" ************" <<std::endl;
  auto read_res = ReadSlabItem(GetMemSlabInfo(sid), 0);
  Item* item = read_res.first;
  uint32_t num = read_res.second;
  for (uint32_t i = 0; i < num; i++) {
    Item* item = ReadSlabItem(GetMemSlabInfo(sid), i).first;
    PrintItem(item);
  }
}

auto ValueLog::PrintSlab(slab::Slab *slab) -> void {
  std::cout << "********** slab ************" <<std::endl;
  for (uint8_t i = 0; i < setting_.slab_size_/SLAB_ITEM_SIZE; i++) {
    Item* item = ReadSlabItem(slab, i);
    PrintItem(item);
  }
}

void encode_l1key(std::string* key, uint64_t sgid, uint16_t mid, uint64_t end_time) {
    leveldb::PutFixed64BE(key, sgid);
    leveldb::PutFixed16BE(key, mid);
    leveldb::PutFixed64BE(key, end_time);
}

void decode_l1key(std::string key, uint64_t &sgid, uint16_t &mid, uint64_t &end_time) {
    sgid = leveldb::DecodeFixed64BE(key.data());
    mid = leveldb::DecodeFixed16BE(key.data()+8);
    end_time = leveldb::DecodeFixed64BE(key.data()+10);
}

void encode_l1val(std::string *val, bool mem, uint32_t sid, uint8_t item_idx) {
    val->push_back(mem);
    leveldb::PutFixed32BE(val, sid);
    val->push_back(item_idx);
}

void decode_l1val(std::string val, bool& mem, uint32_t &sid, uint8_t &item_idx) {
    mem = val[0];
    sid = leveldb::DecodeFixed32BE(val.data()+1);
    item_idx = val[5];
}

}