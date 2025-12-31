#define GLOBAL_VALUE_DEFINE
#include "slab_management.h"

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

namespace slab{

class SlabTest : public testing::Test {
 public:
  void setup() {
    path = "/tmp/slab_test";
    boost::filesystem::remove_all(path);
    int fd = ::open(path.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
    ftruncate(fd, MB);

    setting = new Setting();
    setting->ssd_device_ = "/tmp/slab_test";
    slab_m = new SlabManagement(*setting);
  }

  std::shared_ptr<tsdb::chunk::ChunkMeta> generate_chunk(uint64_t sid, int64_t starting_time,
                                            int num) {
    auto c = new tsdb::chunk::XORChunk();
    auto app = c->appender();
    for (int i = 0; i < num; i++)
      app->append(starting_time + i * 1000, starting_time + i * 1000);
    return std::shared_ptr<tsdb::chunk::ChunkMeta>(new tsdb::chunk::ChunkMeta(
        tsdb::head::pack_chunk_id(sid, 0), std::shared_ptr<tsdb::chunk::ChunkInterface>(c),
        starting_time, starting_time + (num - 1) * 1000));
  }

  void validate_chunk(tsdb::chunk::ChunkInterface* c, int64_t starting_time, int num) {
    auto it = c->iterator();
    int count = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ(starting_time + count * 1000, p.first);
      ASSERT_EQ(starting_time + count * 1000, p.second);
      count++;
    }
    ASSERT_EQ(num, count);
  }
  std::string path;
  Setting *setting;
  SlabManagement* slab_m;
};

TEST_F(SlabTest, ITEM_RW_TEST) {
  setup();

  auto chkmt = generate_chunk(1, 100, 100);
  slab_m->PutKV(1, 0,0,  100, chkmt->chunk->bytes(), chkmt->chunk->size());
  chkmt = generate_chunk(1, 20000, 10);
  slab_m->PutKV(1, 0, 0, 20000, chkmt->chunk->bytes(),chkmt->chunk->size());
  chkmt = generate_chunk(19, 1000, 3);
  slab_m->PutKV(19, 0, 0, 1000, chkmt->chunk->bytes(),chkmt->chunk->size());

  auto slab = slab_m->GetMemSlab(1);
  slab_m->PrintSlab(slab);
  slab = slab_m->GetMemSlab(19);
  slab_m->PrintSlab(slab);
}

TEST_F(SlabTest, ITEM_SIZE_BOUND_TEST) {
  setup();

  tsdb::label::Labels lset;
  lset.emplace_back("hostname", "101");
  tsdb::head::MemSeries* memSeries = new tsdb::head::MemSeries(lset, 1);
  for (uint32_t i = 0; i < 100000; i++) {
//    memSeries->append_slab(slab_m, i*100, i*10);
  }

  for (uint32_t i = 0; i < 10; i++) {
    slab_m->PrintSlab(i);
  }

}

TEST_F(SlabTest, MTKeyTest) {
  uint64_t n_sg, source_group_id = 1234;
  uint16_t n_metric_id, metric_id = 3478;
  uint64_t n_ts, ts = 5678;

  std::string packed_key;
  slab_m->EnCodeKey(&packed_key, source_group_id, metric_id, ts);
  slab_m->DeCodeKey(packed_key, n_sg, n_metric_id, n_ts);

  std::cout<<n_sg<<" "<<""<<n_metric_id<<" "<<n_ts<<std::endl;
}

TEST_F(SlabTest, MTOperationTest) {
  setup();

  uint16_t metric_id = 1;
  uint64_t source_id = 1, ts = 1;

  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo* sinfo = slab_m->GetMemSlabInfo(i);
    slab_m->InsertMT(source_id, metric_id, ts*i*10, sinfo);
  }
  std::cout<<"insert finish"<<std::endl;

  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo sinfo;
    sinfo = *slab_m->SearchMT(source_id, metric_id, ts*i*10);
    std::cout<<i<<" : "<<sinfo.slab_id_<<std::endl;
  }

  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo* sinfo = slab_m->GetMemSlabInfo(i*10+1);
    slab_m->UpdateMT(source_id, metric_id, ts*i*10, sinfo);
  }

  std::cout<<"******************"<<std::endl;
  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo sinfo{i};
    sinfo = *slab_m->SearchMT(source_id, metric_id, ts*i*10);
    std::cout<<i<<" : "<<sinfo.slab_id_<<std::endl;
  }

  std::cout<<"******************"<<std::endl;
  auto sinfo_arr = slab_m->ScanMT(source_id, metric_id, 1, 100);
  std::cout<<"scan result size: "<<sinfo_arr.size()<<std::endl;
  for (auto &it : sinfo_arr) {
    std::cout<<" " <<it->slab_id_<<std::endl;
  }


  for(uint32_t i = 1; i <= 10; i++) {
    SlabInfo sinfo{i};
    slab_m->RemoveMT(source_id*i, metric_id*i, ts*i*10);
  }

}

TEST_F(SlabTest, MT_Multi_Thread) {
  setup();
  for (int i=0; i<10; i++){
    std::cout<<"insert "<<i<<std::endl;
    for (int j=0; j<50; j++) {
      SlabInfo* sinfo = slab_m->GetMemSlabInfo(i*10+j);
      slab_m->InsertMTAsyn(i*10+j,j,j,sinfo);
    }
    sleep(4);
    std::cout<<"search "<<i<<std::endl;
    for (int j=0; j<50; j++) {
      SlabInfo* sinfo ;
      sinfo = slab_m->SearchMT(i*10+j,j,j);
      std::cout<<j<<" : "<<sinfo->slab_id_<<std::endl;
    }
    sleep(4);
    std::cout<<"remove "<<i<<std::endl;
    for (int j=0; j<50; j++) {
      slab_m->RemoveMTAsyn(i*10+j,j,j);
    }
    sleep(4);
  }

}

TEST_F(SlabTest, MT_Batch_Operation) {
  setup();

  for (int i=0; i<10; i++) {
    std::vector<SlabInfo* > sinfo_arr;
    for(int j=0; j<10; j++) {
      SlabInfo* sinfo = slab_m->GetMemSlabInfo(i*10+j);
      sinfo->start_time_ = i*100+j*10;
      sinfo_arr.emplace_back(sinfo);
    }
    slab_m->BatchInsertMT(sinfo_arr);
    sleep(5);
    auto res = slab_m->ScanMT(0, 0, i*100, i*100+90);
    std::cout<<"start time: "<<i*100<<"  end time: "<<i*100+90<<std::endl;
    std::cout<<"res size: "<<res.size()<<std::endl;
    for(auto &it : res) {
      std::cout<<"sid: "<<it->slab_id_<<" start time: "<<it->start_time_<<std::endl;
    }
    sleep(5);
    slab_m->BatchRemoveMT(sinfo_arr);
  }

}

TEST_F(SlabTest, BackGround_Schudule_Test) {
  setup();
  uint32_t slab_id;
  for (uint32_t i=0; i<16; i++) {
    auto sinfo = slab_m->GetMemSlabID(slab_id, 0, 0);
    for(int j=0;j<32;j++){
      auto chkmt = generate_chunk(slab_id, (j+1)*100, 10);
      slab_m->PutKV(slab_id, 0,0,  (j+1)*100, chkmt->chunk->bytes(), chkmt->chunk->size());
    }
    auto slab = slab_m->GetMemSlab(slab_id);
    slab_m->PrintSlab(slab);
  }
//  slab_m->ScheduleBGWrite();
  slab_m->BatchWrite();
  //slab_m->WriteToSSD(0, 0);
}


}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}