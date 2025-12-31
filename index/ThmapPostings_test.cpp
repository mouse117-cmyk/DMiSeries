#define GLOBAL_VALUE_DEFINE
#include "head/Head.hpp"
#include "ThmapPostings.h"
#include "gtest/gtest.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/thread_pool.h"

namespace tsdb::index{
class ThmapPostingsTest : public testing::Test {
 public:
  ThmapPostings * tp;
};

std::vector<::tsdb::label::Labels> read_labels(int num,
                                               const std::string& name) {
  std::vector<::tsdb::label::Labels> lsets;
  std::ifstream file(name);
  std::string line;
  while (getline(file, line) && lsets.size() < num) {
    rapidjson::Document d;
    d.Parse(line.c_str());
    ::tsdb::label::Labels lset;
    for (auto& m : d.GetObject())
      lset.emplace_back(m.name.GetString(), m.value.GetString());
    std::sort(lset.begin(), lset.end());
    lsets.push_back(lset);
  }
  return lsets;
}

TEST_F(ThmapPostingsTest, TEST1){
  tp = new ThmapPostings("/mnt/nvme/tsdb_big");

  std::vector<::tsdb::label::Labels> lsets = read_labels(20000, "../test/timeseries.json");
  int i = 0;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      tp->add(i, l);
      i++;
    }
  //  break;
  }

  int j = 0;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      auto p = tp->get(l);
      std::cout<<"*************   "<<j<<"   *************"<<std::endl;
      p->posting_list_.reset_cursor();
      while (p->posting_list_.next()) {
        std::cout<<p->posting_list_.at()<<std::endl;
      }
      j++;
    }
  }


  delete tp;
}

TEST_F(ThmapPostingsTest, TEST2){
    tp = new ThmapPostings("/mnt/nvme/tsdb_big");

    std::vector<::tsdb::label::Labels> lsets = read_labels(20000, "../test/timeseries.json");
    int i = 0;
    for (const auto& lset : lsets) {
        for (const auto& l : lset) {
            tp->add(i, l);
        }
        i++;
    }
    int j = 0;
    for (const auto& lset : lsets) {
        for (const auto& l : lset) {
            std::vector<uint64_t>listing;
            auto status = tp->get_and_read(l,listing);
            std::cout<<"*************   "<<j<<"   *************"<<std::endl;
            for(auto &id:listing){
                std::cout<<" "<<id;
            }
            std::cout<<std::endl;
            j++;
        }
    }
    delete tp;
}

    TEST_F(ThmapPostingsTest, TEST3){
        tp = new ThmapPostings("/mnt/nvme/tsdb_big");

        std::vector<::tsdb::label::Labels> lsets = read_labels(20000, "../test/timeseries.json");
        auto func = [](ThmapPostings* tp,std::vector<::tsdb::label::Labels>& lsets, uint32_t left, uint32_t right,base::WaitGroup* _wg){
            for(uint32_t i = left;i<right&&i<lsets.size();i++){
                tp->add(i,lsets[i]);
            }
            _wg->done();
        };
        std::cout<<"****\n";
        uint32_t tuple_num = 20;
        uint32_t num_thread = 20;
        ThreadPool pool(num_thread);
        base::WaitGroup wg;
        uint32_t num = lsets.size()/tuple_num + 1;
        for (int i = 0; i < num; i++) {
            wg.add(1);
            pool.enqueue(std::bind(func, tp,lsets,i*tuple_num,(i+1)*tuple_num,&wg));
        }
        wg.wait();
        int j = 0;
        for (const auto& lset : lsets) {
            for (const auto& l : lset) {
                std::vector<uint64_t>listing;
                auto status = tp->get_and_read(l,listing);
                std::cout<<"*************   "<<j<<"   *************"<<std::endl;
                for(auto &id:listing){
                    std::cout<<" "<<id;
                }
                std::cout<<std::endl;
                j++;
            }
        }
        delete tp;
    }

    TEST_F(ThmapPostingsTest, TEST4){
        tp = new ThmapPostings("/mnt/nvme/tsdb_big");

        std::vector<::tsdb::label::Labels> lsets = read_labels(20000, "../test/timeseries.json");
        auto func = [](ThmapPostings* tp,std::vector<::tsdb::label::Labels>& lsets, uint32_t left, uint32_t right,base::WaitGroup* _wg){
            for(uint32_t i = left;i<right&&i<lsets.size();i++){
                tp->add(i,lsets[i]);
            }
            _wg->done();
        };
        std::cout<<"****\n";
        uint32_t tuple_num = 20;
        uint32_t num_thread = 20;
        ThreadPool pool(num_thread);
        base::WaitGroup wg;
        uint32_t num = lsets.size()/tuple_num + 1;
        for (int i = 0; i < num; i++) {
            wg.add(1);
            pool.enqueue(std::bind(func, tp,lsets,i*tuple_num,(i+1)*tuple_num,&wg));
        }
        wg.wait();
        int j = 0;

        uint32_t mig_num = tp->migrate(1);
        std::cout<<"migrate num: "<<mig_num<<std::endl;
        std::cout<<std::endl;

        j = 0;
        auto iter = tp->iterator();
        iter->SeekToFirst();
        while (iter->Valid()) {
            std::cout<<"*************   "<<j<<"   *************"<<std::endl;
            auto key = iter->key();
            auto val = iter->value();
            uint32_t id_num;
            leveldb::GetFixed32(&val, &id_num);
            std::cout<<key.data()<<" "<<key.size()<<" id num: "<<id_num;
            uint64_t id = 0;
            for (uint32_t i = 0; i < id_num; i++) {
                leveldb::GetFixed64(&val, &id);
                std::cout<<" id: "<<id;
            }
            std::cout<<std::endl;
            j++;
            iter->Next();
        }

        j = 0;
        for (const auto& lset : lsets) {
            for (const auto& l : lset) {
                std::vector<uint64_t>listing = tp->read_from_mergeset(l);
                if (listing.size() > 0) {
                    std::cout<<listing.size();
                    std::cout<<"*********   "<<j<<"   *********"<<std::endl;
                    for(auto &id:listing){
                        std::cout<<" "<<id;
                    }
                    std::cout<<std::endl;
                    j++;
                }
            }
        }


        delete tp;
    }

}



int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}