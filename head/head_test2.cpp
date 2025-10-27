#define GLOBAL_VALUE_DEFINE

#include "TreeHead.h"
#include <iostream>
#include "gtest/gtest.h"
#include "third_party/thmap/thmap.h"

namespace tsdb::head{

class HeadTest : public testing::Test {
 public:
  void setup() {
    hd = new TreeHead("/tmp/head_test2");
  }
// private:
  TreeHead* hd;
};


TEST_F(HeadTest, GET_OR_CREATE_TEST) {
  setup();

  label::Labels lset;
  int metric_num = 20;
  int source_num = 10;

  for (int i = 0; i < metric_num; i++) {
    lset.emplace_back("__name__", "metric_"+std::to_string(i));
    for (int j = 0; j < source_num; j++) {
      lset.emplace_back("key_"+std::to_string(j), "val_"+std::to_string(j*10));
      auto ret = hd->get_or_create_by_thmap(lset,0);
      std::cout<<"j: "<<j<<"\tnewly created: "<<ret.second<<std::endl;
      auto ms = ret.first;
      std::cout<<"label set: "<<label::lbs_string(ms->labels)<<"\tsgid: "<<ms->source_id_<<"\tmid: "<<ms->metric_id_<<std::endl;
      lset.pop_back();
    }
    lset.clear();
  }
  for (int i = 0; i < metric_num; i++) {
    lset.emplace_back("__name__", "metric_"+std::to_string(i));
    for (int j = 0; j < source_num; j++) {
      lset.emplace_back("key_"+std::to_string(j), "val_"+std::to_string(j*10));
      auto ret = hd->get_or_create_by_thmap(lset,0);
      std::cout<<"j: "<<j<<"\tnewly created: "<<ret.second<<std::endl;
      auto ms = ret.first;
      std::cout<<"label set: "<<label::lbs_string(ms->labels)<<"\tsgid: "<<ms->source_id_<<"\tmid: "<<ms->metric_id_<<std::endl;
      lset.pop_back();
    }
    lset.clear();
  }

}

TEST_F(HeadTest, FORWARD_INDEX_TEST) {
  setup();

  label::Labels lset;
  int metric_num = 20;
  int source_num = 10;

  for (int i = 0; i < metric_num; i++) {
    lset.emplace_back("__name__", "metric_"+std::to_string(i));
    for (int j = 0; j < source_num; j++) {
      lset.emplace_back("key_"+std::to_string(j), "val_"+std::to_string(j*10));
      auto ret = hd->get_or_create_by_thmap(lset,0);
      auto ms = ret.first;
      hd->set_to_forward_index(j, i,  ms);
      auto nms = hd->get_from_forward_index(j, i);
      std::cout<<"label set: "<<label::lbs_string(nms->labels)<<"\tsgid: "<<nms->source_id_<<"\tmid: "<<nms->metric_id_<<std::endl;
      lset.pop_back();
    }
    lset.clear();
  }

}

TEST_F(HeadTest, MULTI_THREAD_FORWARD_INDEX_TEST) {
  setup();
  int thread_num = 16;
  int metric_num = 5;
  int source_num = 20;

  auto process_labels = [&] (int metric_start, int sg_start) {
    label::Labels lset;
    for (int i = metric_start; i < metric_start+metric_num; i++) {
      lset.emplace_back("__name__", "metric_"+std::to_string(i));
      for (int j = sg_start; j < sg_start+source_num; j++) {
        lset.emplace_back("key_"+std::to_string(j), "val_"+std::to_string(j*10));
        auto ret = hd->get_or_create_by_thmap(lset,0);
        auto ms = ret.first;
        hd->set_to_forward_index(j, i,  ms);
//        auto nms = hd->get_from_forward_index(j, i);
//        std::cout<<"label set: "<<label::lbs_string(nms->labels)<<"\tid: "<<nms->ref<<"\tsgid: "<<nms->source_id_<<"\tmid: "<<nms->metric_id_<<std::endl;
        lset.pop_back();
      }
      lset.clear();
    }
  };
  std::vector<std::thread> threads;
  threads.reserve(thread_num);
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(process_labels, i+metric_num, i+source_num);
  }
  for (auto & th : threads) {
    th.join();
  }


  auto get = [&] (int metric_start, int sg_start) {
    for (int i = metric_start; i < metric_start+metric_num; i++) {
      for (int j = sg_start; j < sg_start+source_num; j++) {
        auto nms = hd->get_from_forward_index(j, i);
        std::cout<<"label set: "<<label::lbs_string(nms->labels)<<"\tsgid: "<<nms->source_id_<<"\tmid: "<<nms->metric_id_<<std::endl;
      }
    }
  };
  std::vector<std::thread> get_threads;
  get_threads.reserve(thread_num);
  for (int i = 0; i < thread_num; i++) {
    get_threads.emplace_back(get, i+metric_num, i+source_num);
  }
  for (auto & th : get_threads) {
    th.join();
  }

}

TEST_F(HeadTest, MULTI_THREAD_GET_OR_CREATE_TEST) {
  setup();
  int thread_num = 16;
  int metric_num = 5;
  int source_num = 20;

  auto process_labels = [&] (int metric_start, int sg_start) {
    label::Labels lset;
    for (int i = metric_start; i < metric_start+metric_num; i++) {
      lset.emplace_back("__name__", "metric_"+std::to_string(i));
      for (int j = sg_start; j < sg_start+source_num; j++) {
        lset.emplace_back("key_"+std::to_string(j), "val_"+std::to_string(j*10));
        auto ret = hd->get_or_create_by_thmap(lset,0);
//        std::cout<<"j: "<<j<<"\tnewly created: "<<ret.second<<std::endl;
        auto ms = ret.first;
//        std::cout<<"label set: "<<label::lbs_string(ms->labels)<<"\tid: "<<ms->ref<<"\tsgid: "<<ms->source_id_<<"\tmid: "<<ms->metric_id_<<std::endl;
        if (ret.second) {
//          std::cout<<"label set: "<<label::lbs_string(ms->labels)<<"\tid: "<<ms->ref<<"\tsgid: "<<ms->source_id_<<"\tmid: "<<ms->metric_id_<<std::endl;
        }
//        std::cout<<ms->ref<<std::endl;
        lset.pop_back();
      }
      lset.clear();
    }
  };

//  for (int i = 0; i < thread_num; i++) {
//    process_labels(i+metric_num/2, i+source_num/2);
//  }

  std::vector<std::thread> threads;
  threads.reserve(thread_num);
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(process_labels, i+metric_num/2, i+source_num/2);
  }
  for (auto & th : threads) {
    th.join();
  }
  //std::cout<<hd->get_last_series_id()<<std::endl;
  std::cout<<hd->get_last_source_group_id()<<std::endl;
  std::cout<<hd->get_last_metric_id()<<std::endl;
}

}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  RUN_ALL_TESTS();
}