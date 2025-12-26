#define GLOBAL_VALUE_DEFINE
#include "ValueLog.h"

#include <cstdio>

#include "../chunk/ChunkAppenderInterface.hpp"
#include "../chunk/ChunkInterface.hpp"
#include "../chunk/ChunkMeta.hpp"
#include "../chunk/ChunkReader.hpp"
#include "../chunk/ChunkWriter.hpp"
#include "../chunk/XORChunk.hpp"
#include "../head/Head.hpp"
#include "../head/HeadUtils.hpp"
#include "../leveldb/include/leveldb/db.h"
#include "gtest/gtest.h"
#include "slab.h"
#include "head/TreeMemSeries.h"
#include "head/SpanHead.h"


namespace slab{

class ValueLogTest : public testing::Test {
 public:
  void setup() {
    path = "/home/dell/project/SSD/tree_series_test";
   // boost::filesystem::remove_all(path);
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
   // ftruncate(fd, 5*GB);

    setting = new Setting();
    setting->ssd_device_ = "/home/dell/project/SSD/tree_series_test";

    info_path = "/home/dell/project/SSD/tree_series_info_test";
    int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
    setting->ssd_slab_info_ = "/home/dell/project/SSD/tree_series_info_test";

    leveldb::Options options;
    options.create_if_missing = true;
    options.max_imm_num = 5;
    options.write_buffer_size = 256 * 1024 * 1024;
    options.max_file_size = 256 * 1024 * 1024;
    options.use_log = false;

    std::string db_path = "/mnt/nvme1/db_test";
    leveldb::DB* db = nullptr;
    leveldb::Status st = leveldb::DB::Open(options, db_path, &db);

    tree_series = new ValueLog(*setting);
    tree_series->SetDB(db);
  }

  std::shared_ptr<tsdb::chunk::ChunkMeta> generate_chunk(uint64_t sid, int64_t starting_time,
                                                         int num) {
    auto c = new tsdb::chunk::XORChunk(tsdb::head::LEVELDB_VALUE_HEADER_SIZE);
    auto app = c->appender();
    for (int i = 0; i < num; i++)
      app->append(starting_time + i * 10, starting_time + i * 10);

    c->bstream.stream[0] = 1;
    leveldb::EncodeFixed16(reinterpret_cast<char*>(&c->bstream.stream[0] + 1), 0);
    leveldb::EncodeFixed64BE(reinterpret_cast<char*>(&c->bstream.stream[0] + 3), 0);

//    tree_series->PrintChunk((uint8_t *)(&(c->bstream.stream[0])));

    return std::shared_ptr<tsdb::chunk::ChunkMeta>(new tsdb::chunk::ChunkMeta(
        tsdb::head::pack_chunk_id(sid, 0), std::shared_ptr<tsdb::chunk::ChunkInterface>(c),
        starting_time, starting_time + (num - 1) * 10));
  }

  void validate_chunk(tsdb::chunk::ChunkInterface* c, int64_t starting_time, int num) {
    auto it = c->iterator();
    int count = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ(starting_time + count * 10, p.first);
      ASSERT_EQ(starting_time + count * 10, p.second);
      count++;
    }
    ASSERT_EQ(num, count);
  }

  std::string path;
  std::string info_path;
  Setting *setting;
  ValueLog* tree_series;
  tsdb::head::SpanHead* head_;
};

TEST_F(ValueLogTest, WriteDiskMasstreeTest) {
    setup();
    uint64_t cnt = 0;
    uint64_t ts = 1;
    for (uint16_t mid = 1; mid < 512; mid++) {
        for (uint64_t sgid = 1; sgid < 1000; sgid++) {
            uint64_t start_time = cnt*100;
            uint64_t end_time = (cnt+1)*100;
            SlabInfo* sinfo = tree_series->GetMemSlabInfo(cnt);
            sinfo->source_id_[0] = sgid;
            sinfo->metric_id_[0] = mid;
            sinfo->start_time_[0] = start_time;
            sinfo->end_time_[0] = end_time;
            tree_series->InsertMT(sgid, mid, start_time, sinfo);
            tree_series->InsertMT(sgid, mid, end_time, sinfo);
            cnt++;
        }
    }

    auto kv_arr = tree_series->TraverseMasstree();
    uint64_t sgid = 0;
    uint16_t mid = 0;
    uint64_t rcnt = 0;
    for (auto &kv : kv_arr) {
        std::string key = kv.first;
        auto sinfo = kv.second;
        tree_series->DeCodeKey(key, sgid, mid, ts);
        assert(sgid == sinfo->source_id_[0]);
        assert(mid == sinfo->metric_id_[0]);
        assert(ts >= sinfo->start_time_[0] && ts <= sinfo->end_time_[0]);
        rcnt++;
    }
    // Each sinfo is related to two keys, one encoded with start_time and another with end_time
    assert(cnt*2 == rcnt);
}

    bool compareSlabInfo(const slab::SlabInfo* a, const slab::SlabInfo* b) {
        return a->slab_id_ < b->slab_id_;
    }
TEST_F(ValueLogTest, SubSequenceTest) {
    setup();
    slab::SlabInfo* vec[105];
    uint32_t count = 50;
    uint32_t idx = 0;
    for (uint32_t i = count; i > 0; i--) {
        if (i % 5 == 0) {
            vec[idx] = (tree_series->GetMemSlabInfo(i*100));
        } else {
            vec[idx] = (tree_series->GetMemSlabInfo(i));
        }
        idx++;
    }

    std::stable_sort(vec, vec+count, compareSlabInfo);
    auto subSequence = FindSinfoSubSequence(vec, count-1);
    for (const auto& subSeq : subSequence) {
        std::cout << "Subsequence from index " << subSeq.first << " to " << subSeq.second << ": ";
        for (int i = subSeq.first; i <= subSeq.second; ++i) {
            std::cout << vec[i]->slab_id_ << " ";
        }
        std::cout << std::endl;
    }
}

TEST_F(ValueLogTest, ITEM_RW_TEST) {
  setup();

  uint32_t sid1 = 1;
  uint32_t sid2 = 2;
  auto chkmt = generate_chunk(1, 100, 50);
  tree_series->PutKV(sid1, 0,0,  100,100, chkmt->chunk->all_bytes(), chkmt->chunk->all_size());
  chkmt = generate_chunk(1, 20000, 10);
  tree_series->PutKV(sid1, 0, 0, 20000, 20000,chkmt->chunk->all_bytes(), chkmt->chunk->all_size());
  chkmt = generate_chunk(19, 1000, 3);
  tree_series->PutKV(sid2, 0, 0, 1000,1000, chkmt->chunk->all_bytes(), chkmt->chunk->all_size());

  auto slab = tree_series->GetMemSlab(1);
  tree_series->PrintSlab(1);
  slab = tree_series->GetMemSlab(2);
  tree_series->PrintSlab(2);
}

TEST_F(ValueLogTest, ITEM_SIZE_BOUND_TEST) {
  setup();

  tsdb::label::Labels lset;
  lset.emplace_back("hostname", "101");
  tsdb::head::TreeMemSeries* memSeries = new tsdb::head::TreeMemSeries(0,0,lset,0);
  for (uint32_t i = 0; i < 100000; i++) {
    memSeries->append(tree_series, i*100, i*10);
  }

  for (uint32_t i = 0; i < 10; i++) {
    tree_series->PrintSlab(i);
  }
}

TEST_F(ValueLogTest, MTKeyTest) {
  uint64_t n_sg, source_group_id = 1234;
  uint16_t n_metric_id, metric_id = 3478;
  uint64_t n_ts, ts = 5678;

  std::string packed_key;
  tree_series->EnCodeKey(&packed_key, source_group_id, metric_id, ts);
  tree_series->DeCodeKey(packed_key, n_sg, n_metric_id, n_ts);

  std::cout<<n_sg<<" "<<""<<n_metric_id<<" "<<n_ts<<std::endl;
}

TEST_F(ValueLogTest, DBOperationTest) {
    setup();

    uint16_t metric_id = 1;
    uint64_t source_id = 1, ts = 1;

    for(uint32_t i = 1; i <= 10; i++) {
        SlabInfo* sinfo = tree_series->GetMemSlabInfo(i);
        tree_series->InsertDB(source_id, metric_id, ts*i*10, sinfo->mem_, sinfo->slab_id_, sinfo->nalloc_+i);
//        tree_series->InsertDB(source_id, metric_id, ts*i*10+1, sinfo->mem_, sinfo->slab_id_, sinfo->nalloc_+i);
    }
    std::cout<<"insert finish"<<std::endl;

    std::vector<const SlabInfo*> sinfo_arr1;
    tree_series->ScanDB(source_id, metric_id, 1, 100, sinfo_arr1);
    std::cout<<"scan result size: "<<sinfo_arr1.size()<<std::endl;
    for (auto &it : sinfo_arr1) {
        std::cout<<" " <<it->slab_id_<<std::endl;
    }

    for(uint32_t i = 1; i <= 10; i++) {
        SlabInfo* sinfo;
        auto res = tree_series->SearchDB(source_id, metric_id, ts*i*10);
        sinfo = res.first;
        uint8_t item_idx = res.second;
        std::cout<<i<<" : "<<sinfo->slab_id_<<" item_idx: "<<int(item_idx)<<std::endl;
    }

    for(uint32_t i = 1; i <= 10; i++) {
        SlabInfo* sinfo = tree_series->GetMemSlabInfo(i*10+1);
        tree_series->UpdateDB(source_id, metric_id, ts*i*10, sinfo->mem_, sinfo->slab_id_, sinfo->nalloc_+i+10);
    }

    std::cout<<"******************"<<std::endl;
    for(uint32_t i = 1; i <= 10; i++) {
        SlabInfo* sinfo = new SlabInfo();
        sinfo->slab_id_ = i;
        auto res = tree_series->SearchDB(source_id, metric_id, ts*i*10);
        uint8_t item_idx = res.second;
        std::cout<<i<<" : "<<sinfo->slab_id_<<" item_idx: "<<int(item_idx)<<std::endl;
    }
    //sleep(5);
    std::cout<<"******************"<<std::endl;
    std::vector<const SlabInfo*> sinfo_arr;
    tree_series->ScanDB(source_id, metric_id, 1, 100, sinfo_arr);
    std::cout<<"scan result size: "<<sinfo_arr.size()<<std::endl;
    for (auto &it : sinfo_arr) {
        std::cout<<" " <<it->slab_id_<<std::endl;
    }


    std::cout<<"******************"<<std::endl;
    for(uint32_t i = 1; i <= 10; i++) {
        SlabInfo* sinfo = new SlabInfo();
        sinfo->slab_id_ = i;
        auto res = tree_series->SearchDB(source_id, metric_id, ts*i*10);
        uint8_t item_idx = res.second;
        std::cout<<i<<" : "<<sinfo->slab_id_<<" item_idx: "<<int(item_idx)<<std::endl;
    }

    for(uint32_t i = 1; i <= 10; i++) {
        SlabInfo* sinfo = new SlabInfo();
        sinfo->slab_id_ = i;
        tree_series->DeleteDB(source_id*i, metric_id*i, ts*i*10);
    }
}

TEST_F(ValueLogTest, MTOperationTest) {
  setup();

  uint16_t metric_id = 1;
  uint64_t source_id = 1, ts = 1;

  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo* sinfo = tree_series->GetMemSlabInfo(i);
    tree_series->InsertMT(source_id, metric_id, ts*i*10, sinfo);
    tree_series->InsertMT(source_id, metric_id, ts*i*10+1, sinfo);
  }
  std::cout<<"insert finish"<<std::endl;

  std::vector<const SlabInfo*> sinfo_arr1;
  tree_series->ScanMT(source_id, metric_id, 1, 100,sinfo_arr1);
  std::cout<<"scan result size: "<<sinfo_arr1.size()<<std::endl;
  for (auto &it : sinfo_arr1) {
    std::cout<<" " <<it->slab_id_<<std::endl;
  }

  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo* sinfo;
    sinfo = tree_series->SearchMT(source_id, metric_id, ts*i*10);
    std::cout<<i<<" : "<<sinfo->slab_id_<<std::endl;
  }

  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo* sinfo = tree_series->GetMemSlabInfo(i*10+1);
    tree_series->UpdateMT(source_id, metric_id, ts*i*10, sinfo);
  }

  std::cout<<"******************"<<std::endl;
  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo* sinfo = new SlabInfo();
    sinfo->slab_id_ = i;
    sinfo = tree_series->SearchMT(source_id, metric_id, ts*i*10);
    std::cout<<i<<" : "<<sinfo->slab_id_<<std::endl;
  }
  //sleep(5);
  std::cout<<"******************"<<std::endl;
  std::vector<const SlabInfo*> sinfo_arr;
  tree_series->ScanMT(source_id, metric_id, 1, 100,sinfo_arr);
  std::cout<<"scan result size: "<<sinfo_arr.size()<<std::endl;
  for (auto &it : sinfo_arr) {
    std::cout<<" " <<it->slab_id_<<std::endl;
  }


  std::cout<<"******************"<<std::endl;
  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo* sinfo = new SlabInfo();
    sinfo->slab_id_ = i;
    sinfo = tree_series->SearchMT(source_id, metric_id, ts*i*10);
    std::cout<<i<<" : "<<sinfo->slab_id_<<std::endl;
  }


  for(uint32_t i = 1; i <= 10; i++) {
//    SlabInfo sinfo{i};
    SlabInfo* sinfo = new SlabInfo();
    sinfo->slab_id_ = i;
    tree_series->RemoveMT(source_id*i, metric_id*i, ts*i*10);
  }
}

TEST_F(ValueLogTest, MT_Multi_Thread) {
  setup();
  for (int i=0; i<10; i++){
    std::cout<<"insert "<<i<<std::endl;
    for (int j=0; j<50; j++) {
      SlabInfo* sinfo = tree_series->GetMemSlabInfo(i*10+j);
      tree_series->InsertMTAsyn(i*10+j,j,j,sinfo);
    }
    sleep(4);
    std::cout<<"search "<<i<<std::endl;
    for (int j=0; j<50; j++) {
      SlabInfo* sinfo ;
      sinfo = tree_series->SearchMT(i*10+j,j,j);
      std::cout<<j<<" : "<<sinfo->slab_id_<<std::endl;
    }
    sleep(4);
    std::cout<<"remove "<<i<<std::endl;
    for (int j=0; j<50; j++) {
      tree_series->RemoveMTAsyn(i*10+j,j,j);
    }
    sleep(4);
  }

}

TEST_F(ValueLogTest, MT_Batch_Operation) {
  setup();

  for (int i=0; i<10; i++) {
    std::vector<SlabInfo* > sinfo_arr;
    for(int j=0; j<10; j++) {
      SlabInfo* sinfo = tree_series->GetMemSlabInfo(i*10+j);
      sinfo->start_time_[0] = i*100+j*10;
      sinfo->idx_ = 1;
      sinfo_arr.emplace_back(sinfo);
    }
    tree_series->BatchInsertMT(sinfo_arr);
    sleep(5);
    std::vector<const SlabInfo*>res;
    tree_series->ScanMT(0, 0, i*100, i*100+90,res);
    std::cout<<"start time: "<<i*100<<"  end time: "<<i*100+90<<std::endl;
    std::cout<<"res size: "<<res.size()<<std::endl;
    for(auto &it : res) {
      std::cout<<"sid: "<<it->slab_id_<<" start time: "<<it->start_time_[0]<<std::endl;
    }
    sleep(5);
    tree_series->BatchRemoveMT(sinfo_arr);
  }

}

TEST_F(ValueLogTest, BatchWrite_Test) {
  setup();
  uint32_t slab_id;
  for (uint32_t i=0; i<160000; i++) {
    auto sinfo = tree_series->GetMemSlabID(slab_id, 0, 0);
    for(int j=0;j<32;j++){
      auto chkmt = generate_chunk(slab_id, (j+1)*100, 30);
//        tsdb::chunk::BitStream* bstream = new tsdb::chunk::BitStream(chkmt->chunk->bytes(), CHUNK_SIZE);
//        tsdb::chunk::XORIterator* it = new tsdb::chunk::XORIterator(*bstream, false);
//        uint32_t i = 0;
//        while (it->next()) {
//            uint64_t ts = it->at().first;
//            double val = it->at().second;
//            std::cout << "i: " << i << " ts: " << ts << " val: " << val << std::endl;
//            i++;
//        }
      tree_series->PutKV(slab_id, 0,0,  (j+1)*100, (j+1)*100,chkmt->chunk->all_bytes(), chkmt->chunk->all_size());
    }
    auto slab = tree_series->GetMemSlab(slab_id);
//    tree_series->PrintSlab(slab);
  }
  //  slab_m->ScheduleBGWrite();
  for(uint32_t i=0;i<10;i++){
    tree_series->BatchWrite();
  }
  std::vector<SlabInfo*>sinfo_arr;
  for(uint32_t i=0;i<160;i++){
    auto sinfo = tree_series->GetDiskSlabInfo(i);
    sinfo_arr.emplace_back(sinfo);
  }
  std::cout<<"===================================\n";
  std::vector<Slab*>slab_arr;
  tree_series->BatchRead(sinfo_arr,slab_arr);
  //slab_m->WriteToSSD(0, 0);
  for(auto &it:slab_arr){
      tree_series->PrintSlab(it);
//    auto item_num = tree_series->GetItemNum();
//    for(int i=0;i<item_num;i++){
//      Item* item = (Item*)(it + SLAB_ITEM_SIZE*i);
////      tree_series->PrintItem(item);
//        tsdb::chunk::BitStream* bstream = new tsdb::chunk::BitStream(item->chunk_, CHUNK_SIZE);
//        tsdb::chunk::XORIterator* it = new tsdb::chunk::XORIterator(*bstream, false);
//        uint32_t iii = 0;
//        while (it->next()) {
//            uint64_t ts = it->at().first;
//            double val = it->at().second;
//            std::cout << "iii: " << iii << " ts: " << ts << " val: " << val << std::endl;
//            iii++;
//        }
//    }
  }
}

TEST_F(ValueLogTest, OptBatchWrite_Test) {
  setup();
  uint32_t slab_id;
  std::vector<Slab*> mem_slab_arr;
  for (uint32_t i=0; i<16000; i++) {
    tree_series->GetMemSlabID(slab_id, i*10, i*2,i);
    auto sinfo = tree_series->GetMemSlabInfo(slab_id);
    ASSERT_EQ(sinfo->metric_id_[0],i*2);
    ASSERT_EQ(sinfo->source_id_[0],i*10);
    ASSERT_EQ(sinfo->start_time_[0],i);
    ASSERT_EQ(sinfo->slab_id_,i);
   // tree_series->InsertMT(sinfo->source_id_, sinfo->metric_id_, sinfo->start_time_, sinfo);
    for(int j=0;j<32;j++){
      auto chkmt = generate_chunk(slab_id, i+j*100, 30);
      tree_series->PutKV(slab_id, i*10,i*2,  i+j*100, i+j*100,chkmt->chunk->all_bytes(), chkmt->chunk->all_size());
    }
    auto slab = tree_series->GetMemSlab(slab_id);
    ASSERT_EQ(sinfo->slab_id_,i);
    mem_slab_arr.emplace_back(slab);
  }
  for(uint32_t i=0;i<16000;i++){
    auto sinfo = tree_series->SearchMT(i*10,i*2,i);
    ASSERT_EQ(sinfo->metric_id_[0],i*2);
    ASSERT_EQ(sinfo->source_id_[0],i*10);
    ASSERT_EQ(sinfo->start_time_[0],i);
    ASSERT_EQ(sinfo->slab_id_,i);
  }
  //  slab_m->ScheduleBGWrite();
  for(uint32_t i=0;i<10;i++){
      tree_series->OptBatchWrite();
  }
  std::vector<SlabInfo*>sinfo_arr;
  for(uint32_t i=0;i<16000;i++){
    auto dsinfo = tree_series->GetDiskSlabInfo(i);
    sinfo_arr.emplace_back(dsinfo);
    auto msinfo = tree_series->GetMemSlabInfo(i);
    ASSERT_EQ(dsinfo->metric_id_[0],i*2);
    ASSERT_EQ(dsinfo->source_id_[0],i*10);
    ASSERT_EQ(dsinfo->start_time_[0],i);
//    ASSERT_EQ(msinfo->start_time_[0],0);
    ASSERT_EQ(msinfo->free_,true);
    ASSERT_EQ(msinfo->nalloc_,0);
  }
  std::cout<<"===================================\n";
  std::vector<Slab*>slab_arr;
  tree_series->BatchRead(sinfo_arr,slab_arr);
  uint32_t idx=0;
  for(auto &it:slab_arr){
    auto flag = std::memcmp(it,mem_slab_arr[idx],SLAB_SIZE);
    if(flag!=0||idx==2560){
       auto item_num = tree_series->GetItemNum();
        for(int i=0;i<item_num;i++){
          Item* item = (Item*)(it + SLAB_ITEM_SIZE * i);
          tree_series->PrintItem(item);
          std::cout<<"==================\n";
          item = (Item*)(mem_slab_arr[idx] + SLAB_ITEM_SIZE * i);
          tree_series->PrintItem(item);
          break;
        }
    }
    idx++;
    ASSERT_EQ(flag,0);
//   auto item_num = tree_series->GetItemNum();
//    for(int i=0;i<item_num;i++){
//      Item* item = (Item*)(it + SLAB_ITEM_SIZE*i);
//      tree_series->PrintItem(item);
//    }
  }
}

TEST_F(ValueLogTest, SSDInfo_Test){
  setup();
  auto disk_slab_num = tree_series->GetDiskSlabNum();
  for(uint32_t i =0;i<disk_slab_num;i++){
    tree_series->GetDiskSlabID(i,i*1,0,i*3);
    auto sinfo = tree_series->GetDiskSlabInfo(i);
    ASSERT_EQ(sinfo->metric_id_[0],0);
    ASSERT_EQ(sinfo->source_id_[0],i*1);
    ASSERT_EQ(sinfo->start_time_[0],i*3);
    ASSERT_EQ(sinfo->slab_id_,i);
  }
  auto flag = tree_series->WriteDiskSlabInfo();
  ASSERT_EQ(flag,true);
  flag = tree_series->RecoveryDiskSlabInfo();
  ASSERT_EQ(flag,true);
  for(uint32_t i=0;i<disk_slab_num;i++){
    auto sinfo = tree_series->GetDiskSlabInfo(i);
    ASSERT_EQ(sinfo->metric_id_[0],0);
    ASSERT_EQ(sinfo->source_id_[0],i*1);
    ASSERT_EQ(sinfo->start_time_[0],i*3);
    ASSERT_EQ(sinfo->slab_id_,i);

    auto mt_st_sinfo = tree_series->SearchMT(sinfo->source_id_[0], sinfo->metric_id_[0], sinfo->start_time_[0]);
    ASSERT_EQ(sinfo, mt_st_sinfo);
    ASSERT_EQ(sinfo->metric_id_[0],mt_st_sinfo->metric_id_[0]);
    ASSERT_EQ(sinfo->source_id_[0],mt_st_sinfo->source_id_[0]);
    ASSERT_EQ(sinfo->start_time_[0],mt_st_sinfo->start_time_[0]);
    ASSERT_EQ(sinfo->slab_id_,mt_st_sinfo->slab_id_);
    auto mt_ed_sinfo = tree_series->SearchMT(sinfo->source_id_[0], sinfo->metric_id_[0], sinfo->end_time_[0]);
    ASSERT_EQ(sinfo, mt_ed_sinfo);
    ASSERT_EQ(sinfo->metric_id_[0],mt_st_sinfo->metric_id_[0]);
    ASSERT_EQ(sinfo->source_id_[0],mt_st_sinfo->source_id_[0]);
    ASSERT_EQ(sinfo->end_time_[0],mt_st_sinfo->end_time_[0]);
    ASSERT_EQ(sinfo->slab_id_,mt_st_sinfo->slab_id_);
  }
}

TEST_F(ValueLogTest, SCAN_Test){
  setup();
  uint32_t slab_id;
  std::vector<Slab*> mem_slab_arr;
  uint32_t source_id = 10;
  uint16_t metric_id = 20;
  for (uint64_t i=0; i<16000; i++) {
    tree_series->GetMemSlabID(slab_id, source_id, metric_id,i*1000000);
    auto sinfo = tree_series->GetMemSlabInfo(slab_id);
    ASSERT_EQ(sinfo->metric_id_[0],metric_id);
    ASSERT_EQ(sinfo->source_id_[0],source_id);
    ASSERT_EQ(sinfo->start_time_[0],i*1000000);
    ASSERT_EQ(sinfo->slab_id_,i);
    for(uint64_t j=0;j<32;j++){
      auto chkmt = generate_chunk(slab_id, i*1000000+j*10000, 30);
      tree_series->PutKV(slab_id, source_id,metric_id,  i*1000000+j*10000, i*1000000+j*10000, chkmt->chunk->all_bytes(), chkmt->chunk->all_size());
    }
    auto slab = tree_series->GetMemSlab(slab_id);
    ASSERT_EQ(sinfo->slab_id_,i);
    mem_slab_arr.emplace_back(slab);
  }
  uint64_t start_time = 0;
  uint64_t end_time = 16000;
  end_time *= 1000000;
  std::vector<std::pair<const SlabInfo*,Slab*>>ans1;
  tree_series->Scan(source_id,metric_id,start_time,end_time,ans1);
  ASSERT_EQ(ans1.size(),16000);
  uint32_t idx = 0;
  for(auto &it:ans1){
    auto flag = std::memcmp(it.second,mem_slab_arr[idx],SLAB_SIZE);
    idx++;
    ASSERT_EQ(flag,0);
  //  std::cout<<idx<<"*"<<it.first->mem_<<std::endl;
    if(!it.first->mem_){
      auto flag1 = tree_series->FreeBufSlab(it.second);
      ASSERT_EQ(flag1,true);
    }
  }
  tree_series->OptBatchWrite();
  std::vector<std::pair<const SlabInfo*,Slab*>>ans2;
  tree_series->Scan(source_id,metric_id,start_time,end_time,ans2);
  ASSERT_EQ(ans2.size(),16000);
  idx = 0;
  for(auto &it:ans2){
    auto flag = std::memcmp(it.second,mem_slab_arr[idx],SLAB_SIZE);
    idx++;
    ASSERT_EQ(flag,0);
    ASSERT_EQ(it.first->slab_id_,idx-1);
    if(!it.first->mem_){
    //  std::cout<<idx<<" "<<it.first->mem_<<std::endl;
      auto flag1 = tree_series->FreeBufSlab(it.second);
      ASSERT_EQ(flag1,true);
    }
  }
  ASSERT_EQ(tree_series->GetNBufSlab(),tree_series->GetNFreeBufSlab());

  tree_series->OptBatchWrite();
  std::vector<std::pair<const SlabInfo*,Slab*>>ans3;
  tree_series->Scan(source_id,metric_id,start_time,end_time,ans3);
  ASSERT_EQ(ans3.size(),16000);
  idx = 0;
  for(auto &it:ans3){
    auto flag = std::memcmp(it.second,mem_slab_arr[idx],SLAB_SIZE);
    idx++;
    ASSERT_EQ(flag,0);
    ASSERT_EQ(it.first->slab_id_,idx-1);
    if(!it.first->mem_){
      //  std::cout<<idx<<" "<<it.first->mem_<<std::endl;
      auto flag1 = tree_series->FreeBufSlab(it.second);
      ASSERT_EQ(flag1,true);
    }
  }
  ASSERT_EQ(tree_series->GetNBufSlab(),tree_series->GetNFreeBufSlab());
}

}
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}