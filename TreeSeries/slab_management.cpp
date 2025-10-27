//#include "slab_management.h"
//
//#include <iostream>
//
//#include "chunk/XORChunk.hpp"
//#include "chunk/XORIterator.hpp"
//#include "slab.h"
//namespace slab {
//void *smmap(uint32_t size){
//  void *p;
//  p = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
//  return p;
//}
//
//int smunmap(void *p, std::size_t size) {
//  int status;
//  status = munmap(p, size);
//  return status;
//}
//
//SlabManagement::SlabManagement(Setting &setting)
//    :
//      pool_(16),
//      mass_tree_(),
////      env_(),
//      background_write_scheduled_(false),
//      background_work_finished_signal_(&mutex_)
////      free_dsinfoq_(),
////      used_dsinfoq_(),
////      free_msinfoq_(),
////      used_msinfoq_()
//    {
//  setting_ = setting;
//  assert(setting_.ssd_device_ != nullptr);
//  MAX_ALLOC_ITEM = setting_.slab_size_ / SLAB_ITEM_SIZE;
//
//  env_ = leveldb::Env::Default();
//
//  nfree_msinfoq_ = 0;
//  nfree_dsinfoq_ = 0;
//  nused_msinfoq_ = 0;
//  nused_dsinfoq_ = 0;
//
//  nmslab_ = setting_.max_slab_memory_ / setting_.slab_size_;
//  mstart_ = static_cast<uint8_t *>(smmap(setting_.max_slab_memory_));
//  mend_   = mstart_ + setting_.max_slab_memory_;
//
//  size_t size = 0;
//  bool status = SSD_DevieSize(setting_.ssd_device_, &size);
//  ndslab_ = size / setting_.slab_size_;
//  dstart_ = 0;
//  dend_ = dstart_ + size;
//
//  fd_ = open(setting_.ssd_device_, O_RDWR | O_DIRECT, 0644);
//  write_batch_size_ = setting_.write_batch_size_;
//
//  read_buf_ = nullptr;
//  read_buf_ = static_cast<uint8_t *>(smmap(READ_BUFFER_SIZE));
//  memset(read_buf_, 0xff, setting_.slab_size_);
//
//  InitStable();
//}
//
//SlabManagement::~SlabManagement(){
//  free(mstable_);
//  mstable_ = nullptr;
//  nmslab_ = 0;
//  free(dstable_);
//  dstable_ = nullptr;
//  ndslab_ = 0;
//  smunmap(mstart_,setting_.max_slab_memory_);
//  mstart_ = nullptr;
//  smunmap(read_buf_,READ_BUFFER_SIZE);
//  read_buf_ = nullptr;
//  close(fd_);
//}
//
//auto SlabManagement::InitStable() -> bool {
//  SlabInfo *sinfo;
//  //====================初始化内存中slab info============================
//  mstable_ = static_cast<SlabInfo *>((void *)malloc(sizeof(*mstable_) * nmslab_));
//  if (mstable_ == nullptr) {
//    return false;
//  }
//  for (uint32_t i = 0; i < nmslab_; i++) {
//    sinfo = &mstable_[i];
//
//    sinfo->slab_id_ = i;
//    sinfo->source_id_ = 0;
//    sinfo->metric_id_ = 0;
//    sinfo->nalloc_ = 0;
////    sinfo->mem_ = 1;
//    sinfo->free_ = true;
//   // sinfo->slab_id_ = i;
//    sinfo->mem_ = 1;
//    nfree_msinfoq_++;
////    free_msinfoq_.enqueue(&mstable_[i]);
//      free_msinfoq_.push(&mstable_[i]);
//  }
//  //====================初始化SSD中slab info============================
//  dstable_ = static_cast<SlabInfo *>((void *)malloc(sizeof(*dstable_) * ndslab_));
//  if (dstable_ == nullptr) {
//    return false;
//  }
//
//  for (uint32_t i = 0; i < ndslab_; i++) {
//    sinfo = &dstable_[i];
//
//    sinfo->slab_id_ = i;
//    sinfo->source_id_ = 0;
//    sinfo->metric_id_ = 0;
//    sinfo->nalloc_ = 0;
//    sinfo->mem_ = 0;
//    sinfo->free_ = true;
//   // sinfo->addr_ = i;
//    nfree_dsinfoq_++;
////    free_dsinfoq_.enqueue(&dstable_[i]);
//      free_dsinfoq_.push(&dstable_[i]);
//  }
//  return true;
//}
//
//auto SlabManagement::SSD_DevieSize(const char *path, size_t *size) -> bool {
//  int status;
//  struct stat statiofo;
//  int fd;
//
//  status = stat(path, &statiofo);
//  if (!S_ISREG(statiofo.st_mode) && !S_ISBLK(statiofo.st_mode)) {
//    return false;
//  }
//  if (S_ISREG(statiofo.st_mode)) {
//    *size = static_cast<size_t>(statiofo.st_size);
//    return true;
//  }
//
//  fd = open(path, O_RDONLY, 0644);
//  if (fd < 0) {
//    return false;
//  }
//  status = ioctl(fd, _IOR(0x12,114,size_t), size);
//  if (status < 0) {
//    close(fd);
//    return false;
//  }
//  close(fd);
//
//  return true;
//}
//
//auto SlabManagement::GetMemSlabID(uint32_t &slab_id, uint64_t source_id, uint16_t metric_id) -> bool {
//  SlabInfo* sinfo;
////  auto flag = free_msinfoq_.try_dequeue(sinfo);
//    auto flag = free_msinfoq_.try_pop(sinfo);
//  if(!flag){
//    return flag;
//  }
//  slab_id = sinfo->slab_id_;
////  used_msinfoq_.enqueue(sinfo);
//    used_msinfoq_.push(sinfo);
//  sinfo->free_ = false;
//  sinfo->source_id_ = source_id;
//  sinfo->metric_id_ = metric_id;
//
//  return flag;
//}
//
//auto SlabManagement::GetDiskSlabID(uint32_t &slab_id, uint64_t source_id, uint16_t metric_id) -> bool {
//  SlabInfo* sinfo;
////  auto flag = free_dsinfoq_.try_dequeue(sinfo);
//    auto flag = free_dsinfoq_.try_pop(sinfo);
//  if(!flag){
//    return flag;
//  }
//  slab_id = sinfo->slab_id_;
////  used_dsinfoq_.enqueue(sinfo);
//    used_dsinfoq_.push(sinfo);
//  sinfo->free_ = false;
//  sinfo->source_id_ = source_id;
//  sinfo->metric_id_ = metric_id;
//
//  return flag;
//}
//
//auto SlabManagement::PutKV(uint32_t sid, uint64_t source_id, uint16_t metric_id, uint64_t key,
//                           const uint8_t *value, uint32_t value_len) -> bool {
//  if (!ValidSid(sid)) {
//    return false;
//  }
//  SlabInfo *sinfo = &mstable_[sid];
//  if (sinfo->nalloc_ == 0) {
//      sinfo->start_time_ = key;
//      sinfo->source_id_ = source_id;
//      sinfo->metric_id_ = metric_id;
//  }
//  if (SlabFull(sinfo)) {
//    return false;
//  }
//
//  Item* item = WritableItem(sid);
//  if (item == nullptr) {
//    return false;
//  }
//  item->timestamp_ = key;
//  if(value_len<120){
//    memcpy(ItemKey(item),value,value_len);
//  }else{
//     memcpy(ItemKey(item), value, 120);
//  }
//
//
//  std::string slab_key;
//  EnCodeKey(&slab_key,source_id, metric_id,key);
//  mass_tree_.insert_value(slab_key.c_str(), sizeof(uint64_t)*3, sinfo);
//
////  PrintItem(item);
////  PrintItem(ReadSlabItem(sid, GetSlabInfo(sid)->nalloc_-1).first);
//
//  if (SlabFull(sinfo)) {
//    sinfo->end_time_ = key;
////  PrintSlab(sinfo->slab_id_);
//  }
//
//  return true;
//}
//
//auto SlabManagement::WritableItem(const uint32_t sid) -> Item * {
//  if (!ValidSid(sid)) {
//    return nullptr;
//  }
//  SlabInfo* sinfo = &mstable_[sid];
//  Slab* slab = GetMemSlab(sinfo->slab_id_);
//  Item* item = readSlabItem(slab, sinfo->nalloc_, SLAB_ITEM_SIZE);
//  sinfo->nalloc_++;
//
//  return item;
//}
//
//auto SlabManagement::GetMemSlab(const uint32_t sid) const -> Slab * {
//  off_t off = static_cast<off_t>(sid) * setting_.slab_size_;
//  Slab* slab = (Slab*)(mstart_ + off);
//  return slab;
//}
//
//auto SlabManagement::readSlabItem(const Slab* slab, const uint32_t idx, const size_t size) -> Item* {
//  Item* item = (Item*)((uint8_t *)slab->data_ + (idx*size));
//  return item;
//}
//
//auto SlabManagement::SlabToDaddr(const SlabInfo* sinfo) const -> off_t {
//  off_t off = dstart_ + (static_cast<off_t>(sinfo->slab_id_) * setting_.slab_size_);
//  return off;
//}
//
//// todo 后台线程批量落盘，更新 MT
//void SlabManagement::ScheduleBGWrite() {
//  if (!background_write_scheduled_) {
//      background_write_scheduled_ = true;
//      env_->Schedule(&SlabManagement::BGWork, this);
//  }
//}
//
//void SlabManagement::BGWork(void* slab_m) {
//  reinterpret_cast<SlabManagement*>(slab_m)->BackGroundCall();
//}
//
//void SlabManagement::BackGroundCall() {
//  BatchWrite();
//  background_write_scheduled_ = false;
//  ScheduleBGWrite();
//  background_work_finished_signal_.SignalAll();
//}
//
//auto SlabManagement::BatchWrite() -> bool {
//    std::vector<std::future<bool>> future_results;
//
//    SlabInfo* full_msinfo_queue[setting_.write_batch_size_];
////  auto count = used_msinfoq_.try_dequeue_bulk(full_msinfo_queue, write_batch_size_);
////  if (count == 0) {
////    return false;
////  }
//
//    if (used_msinfoq_.was_empty()) {
//        return false;
//    }
//    uint32_t i = 0;
//    uint32_t count = 0;
//    while (used_msinfoq_.try_pop(full_msinfo_queue[i])) {
//        count++;
//        i++;
//        if (count >= write_batch_size_ || used_msinfoq_.was_empty()) {
//            break;
//        }
//    }
//
//    if (free_dsinfoq_.was_size() < count) {
//        for (uint32_t i = 0; i < count; i++) {
//            used_msinfoq_.push(full_msinfo_queue[i]);
//        }
//        return false;
//    }
//    SlabInfo* free_dsinfo_queue[count];
//    for (uint32_t i = 0; i < count; i++) {
//        free_dsinfoq_.try_pop(free_dsinfo_queue[i]);
//        used_dsinfoq_.push(free_dsinfo_queue[i]);
//    }
//
////  auto ds_count = free_dsinfoq_.try_dequeue_bulk(free_dsinfo_queue, count);
////  if (ds_count != count) {
////    free_dsinfoq_.enqueue_bulk(free_dsinfo_queue, ds_count);
////    used_msinfoq_.enqueue_bulk(full_msinfo_queue, count);
////    return false;
////  }
////  used_dsinfoq_.enqueue_bulk(free_dsinfo_queue, count);
//
//    for (uint32_t i = 0; i < count; i++) {
//        SlabInfo* dsinfo = free_dsinfo_queue[i];
//        SlabInfo* msinfo = full_msinfo_queue[i];
//        dsinfo->free_ = false;
//        msinfo->free_ = true;
//        uint32_t dsid = dsinfo->slab_id_;
//        uint32_t msid = full_msinfo_queue[i]->slab_id_;
//        future_results.emplace_back(this->pool_.enqueue([this, msid, dsid] {
//            return WriteToSSD(msid, dsid);
//        }));
//    }
//    for(auto &it : future_results){
//        if (!it.get()) {
//            return false;
//        }
//    }
////  free_msinfoq_.enqueue_bulk(full_msinfo_queue, count);
//
//    for (uint32_t i = 0; i < count; i++) {
//        free_msinfoq_.push(full_msinfo_queue[i]);
//    }
//
//    nfree_msinfoq_ += count;
//    nused_msinfoq_ -= count;
//    nfree_dsinfoq_ -= count;
//    nused_dsinfoq_ += count;
//    return true;
//}
//
//auto SlabManagement::WriteToSSD(uint32_t msid, uint32_t dsid) -> bool {
//  SlabInfo* dsinfo = &dstable_[dsid];
//  Slab* slab = GetMemSlab(msid);
//  size_t slab_size = setting_.slab_size_;
//  off_t offset = SlabToDaddr(dsinfo);
//  auto n = pwrite(fd_, slab, slab_size, offset);
//  if (static_cast<size_t>(n) < slab_size) {
//    throw std::logic_error("fail in WriteToSSD");
//  }
//  return true;
//}
//
//// 读取 slab 内从 idx 开始的所有 item
//auto SlabManagement::ReadSlabItem(uint32_t sid, uint32_t idx) -> std::pair<Item *, uint32_t > {
//  SlabInfo* sinfo = GetMemSlabInfo(sid);
//  Slab* slab = GetMemSlab(sinfo->slab_id_);
//  return std::make_pair(readSlabItem(slab, idx, SLAB_ITEM_SIZE), sinfo->nalloc_-idx);
//}
//
//auto SlabManagement::ReadDiskSlab(slab::SlabInfo *sinfo, off_t addr) -> Slab * {
//  off_t off = SlabToDaddr(sinfo) + addr*setting_.slab_size_;
//  uint32_t read_offset = read_buf_offset_.fetch_add(SLAB_SIZE);
//  auto n = pread(fd_, read_buf_+read_offset, SLAB_SIZE, off);
//  if (n < SLAB_SIZE) {
//    return nullptr;
//  }
//
//  return (Slab*)(read_buf_+read_offset);
//}
//
//auto SlabManagement::BatchRead(std::vector<SlabInfo*> sinfo_q) -> std::vector<Slab*> {
//  std::vector<std::future<Slab*>> future_results;
//  for (auto it : sinfo_q) {
//    off_t off = it->slab_id_;
//    future_results.emplace_back(this->pool_.enqueue([this, it, off]{
//      return ReadDiskSlab(it, off);
//    }));
//  }
//  std::vector<Slab*> slabs;
//  for (auto &future :future_results) {
//    slabs.emplace_back(future.get());
//  }
//  return slabs;
//}
//
//
//auto SlabManagement::InsertMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
//  std::string packed_key;
//  EnCodeKey(&packed_key,source_id,metric_id,ts);
//  return mass_tree_.insert_value(packed_key.c_str(), packed_key.size(), sinfo);
//}
//
//auto SlabManagement::RemoveMT(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool {
//  std::string packed_key;
//  EnCodeKey(&packed_key,source_id,metric_id,ts);
//  return mass_tree_.remove_value(packed_key.c_str(), packed_key.size());
//}
//
//auto SlabManagement::SearchMT(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> SlabInfo* {
//  std::string packed_key;
//  EnCodeKey(&packed_key,source_id,metric_id,ts);
//  return mass_tree_.get_value(packed_key.c_str(), packed_key.size());
//}
//
//auto SlabManagement::UpdateMT(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
//  std::string packed_key;
//  EnCodeKey(&packed_key,source_id,metric_id,ts);
//  return mass_tree_.update_value(packed_key.c_str(), packed_key.size(), sinfo, true);
//}
//
//auto SlabManagement::ScanMT(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time) -> std::vector<const SlabInfo*> {
//  std::string start_key, end_key;
//  EnCodeKey(&start_key, source_id, metric_id, start_time);
//  EnCodeKey(&end_key, source_id, metric_id, end_time);
//
//  std::vector<const SlabInfo*> sinfo_arr;
//  mass_tree_.scan(start_key.c_str(), start_key.size(), false, end_key.c_str(), end_key.size(), true,
//                  {
//                      [](const MasstreeWrapper<SlabInfo>::leaf_type *leaf, uint64_t version,
//                            bool &continue_flag) {
//                     (void)leaf;
//                     (void)version;
//                     (void)continue_flag;
//                     return;
//                   },
//                   [&sinfo_arr](const MasstreeWrapper<SlabInfo>::Str &key, const SlabInfo *val, bool &continue_flag) {
//                     sinfo_arr.emplace_back(val);
//                     return;
//                   }});
//  return sinfo_arr;
//}
//
//
//auto SlabManagement::InsertMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
//  this->pool_.enqueue([this, source_id,metric_id, ts,sinfo]{
//    this->InsertMT( source_id,metric_id, ts,sinfo);
//  });
//  return true;
// // return mass_tree_.insert_value(packed_key.c_str(), sizeof (uint64_t ) * 3, sinfo);
//}
//
//auto SlabManagement::RemoveMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts) -> bool {
//  this->pool_.enqueue([this, source_id,metric_id, ts]{
//    this->RemoveMT( source_id,metric_id, ts);
//  });
//  return true;
//}
//
//auto SlabManagement::UpdateMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t ts, SlabInfo* sinfo) -> bool {
//  this->pool_.enqueue([this, source_id,metric_id, ts,sinfo]{
//    this->UpdateMT( source_id,metric_id, ts,sinfo);
//  });
//  return true;
//}
//
//auto SlabManagement::ScanMTAsyn(uint64_t source_id, uint16_t metric_id, uint64_t start_time, uint64_t end_time) -> std::vector<const SlabInfo*> {
//  std::vector<const SlabInfo*> sinfo_arr;
//  this->pool_.enqueue([this, source_id,metric_id, start_time,end_time, &sinfo_arr]{
//    sinfo_arr = this->ScanMT( source_id,metric_id, start_time,end_time);
//  });
//  return sinfo_arr;
//}
//
//auto SlabManagement::BatchRemoveMT(std::vector<SlabInfo*> sinfo_arr) -> bool {
//  for (auto &it : sinfo_arr) {
//    RemoveMTAsyn(it->source_id_, it->metric_id_, it->start_time_);
//  }
//  return true;
//}
//
//auto SlabManagement::BatchInsertMT(std::vector<SlabInfo*> sinfo_arr) -> bool {
//  for (auto &it : sinfo_arr) {
//    InsertMTAsyn(it->source_id_, it->metric_id_, it->start_time_, it);
//  }
//  return true;
//}
//
//auto SlabManagement::PrintItem(Item* item) -> void {
//  uint64_t timestamp = item->timestamp_;
//  tsdb::chunk::BitStream* bstream = new tsdb::chunk::BitStream(item->chunk_, CHUNK_SIZE);
//  tsdb::chunk::XORIterator* it = new tsdb::chunk::XORIterator(*bstream, false);
//  std::cout<< "Item start timestamp: " << timestamp <<std::endl;
//  uint32_t i = 0;
//  while (it->next()) {
//    uint64_t ts = it->at().first;
//    double val = it->at().second;
//    std::cout << "i: " << i << " ts: " << ts << " val: " << val << std::endl;
//    i++;
//  }
//  std::cout<<"OK\n";
//}
//
//auto SlabManagement::PrintSlab(uint32_t sid) -> void {
//  std::cout << "********** slab " << sid <<" ************" <<std::endl;
//  auto read_res = ReadSlabItem(sid, 0);
//  Item* item = read_res.first;
//  uint32_t num = read_res.second;
//  for (uint32_t i = 0; i < num; i++) {
//    Item* item = ReadSlabItem(sid, i).first;
//    PrintItem(item);
//  }
//}
//
//auto SlabManagement::PrintSlab(slab::Slab *slab) -> void {
//  std::cout << "********** slab ************" <<std::endl;
//  for (uint8_t i = 0; i < setting_.slab_size_/SLAB_ITEM_SIZE; i++) {
//    Item* item = readSlabItem(slab, i);
//    if (item->timestamp_ == 0) {
//      return;
//    }
//    PrintItem(item);
//  }
//}
//
//}