#define GLOBAL_VALUE_DEFINE

#include "indirection_mapping.h"
#include "indirection_mapping_management.h"
#include "gtest/gtest.h"
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


namespace tsdb::mem{

class IndirectionMappingTest : public testing::Test {
 public:

  void batch_insert(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num) {
    imap_mng->alloc_slot(sg_start+sg_num-1, metric_start+metric_num-1);
    for (uint64_t i = sg_start; i < sg_start+sg_num; i++) {
      for (uint64_t j = metric_start; j < metric_start+metric_num; j++) {
        imap_mng->set_slot(i, j, i*1000+j*10+1);
      }
    }
  }

  void batch_read(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num) {
    uint64_t count = 0;
    for (uint64_t i = sg_start; i < sg_start+sg_num; i++) {
      for (uint64_t j = metric_start; j < metric_start+metric_num; j++) {
        uint64_t val = imap_mng->read_slot(i, j);
        assert(val == i*1000+j*10+1);
        count++;
      }
    }
    assert(count == sg_num * metric_num);
  }

  void batch_update(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num) {
    uint64_t count = 0;
    for (uint64_t i = sg_start; i < sg_start+sg_num; i++) {
      for (uint64_t j = metric_start; j < metric_start+metric_num; j++) {
        uint64_t val = imap_mng->cas_slot(i, j, i*1000+j*10+1, i*100000+j*100+10);
        assert(val == 1);
        count++;
      }
    }
    assert(count == sg_num * metric_num);

    count = 0;
    for (uint64_t i = sg_start; i < sg_start+sg_num; i++) {
      for (uint64_t j = metric_start; j < metric_start+metric_num; j++) {
        uint64_t val = imap_mng->read_slot(i, j);
        assert(val == i*100000+j*100+10);
        count++;
      }
    }
    assert(count == sg_num * metric_num);
  }

  void print_idr_map() {
    std::cout<<"map size: "<<idr_map->size()<<std::endl;

    auto ids = idr_map->get_ids();
    for (auto & it : ids) {
      if (it.second == 0) {
        continue;
      }
      std::cout<<"i: "<<it.first<<"\t\tval: "<<it.second<<std::endl;
    }
  }

  void print_imap_mng() {
    std::cout<<"map size: "<<imap_mng->size()<<std::endl;

    for (uint64_t i = 0; i < imap_mng->size(); i++) {
      std::cout<<"******************* "<<" imap "<<i<<" *****************"<<std::endl;
      std::cout<<"******************* "<<"Metric num: "<<imap_mng->metric_num(i)<<" *****************"<<std::endl;

      auto ids = imap_mng->get_imap(i)->get_ids();
      for(auto & it : ids) {
       if (it.second == 0) {
         continue;
       }
       std::cout<<"i: "<<it.first<<"\t\tval: "<<it.second<<std::endl;
      }

      std::cout<<"OK"<<std::endl;
    }
  }

  void print_imap_mng(uint64_t sg_start, uint64_t sg_num, uint64_t metric_start, uint64_t metric_num) {
    imap_mng->print_imap_mng(sg_start, sg_num, metric_start, metric_num);
  }

  void setup() {
    idr_map = new IndirectionMapping();
    imap_mng = new IndirectionMappingManagement();
  }

 private:
  IndirectionMapping* idr_map{};
  IndirectionMappingManagement* imap_mng;
};

TEST_F(IndirectionMappingTest, IMAP_RW_TEST) {
  setup();

  int sg_start = 0;
  int sg_num = 5;
  int metric_start = 0;
  int metric_num = 20;
  batch_insert(sg_start, sg_num, metric_start, metric_num);
  batch_read(sg_start, sg_num, metric_start, metric_num);
//  print_idr_map();
  print_imap_mng();
}

TEST_F(IndirectionMappingTest, IMAP_RW_CAS_TEST) {
  setup();

  int sg_start = 0;
  int sg_num = 10;
  int metric_start = 16;
  int metric_num = 10;
  batch_insert(sg_start, sg_num, metric_start, metric_num);
  batch_read(sg_start, sg_num, metric_start, metric_num);
  batch_update(sg_start, sg_num, metric_start, metric_num);
  print_imap_mng();
}

TEST_F(IndirectionMappingTest, IMAP_MANAGEMENT_TEST) {
  setup();

  uint64_t sg_start = 0;
  uint64_t sg_num = 10;
  uint64_t metric_start = 0;
  uint64_t metric_num = 20;
  for (uint64_t i = 0; i < 5; i++) {
    batch_insert(sg_start, sg_num, metric_start, metric_num);
    batch_read(sg_start, sg_num, metric_start, metric_num);
    batch_update(sg_start, sg_num, metric_start, metric_num);
    print_imap_mng(sg_start, sg_num, metric_start, metric_num);

    sg_start += 1<<SOURCE_BIT;
  }
//  print_imap_mng();
}

}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  RUN_ALL_TESTS();
}