#define GLOBAL_VALUE_DEFINE
#include "disk/log_manager.h"
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#include <signal.h>

#include <boost/filesystem.hpp>
#include <jemalloc/jemalloc.h>

#include "chunk/XORChunk.hpp"
#include "db/version_set.h"
#include "head/Head.hpp"
#include "head/HeadAppender.hpp"
#include "head/MemSeries.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"
#include "leveldb/cache.h"
#include "port/port.h"
#include "querier/tsdb_querier.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/thread_pool.h"

#include <time.h>

#include <boost/filesystem.hpp>
#include <chrono>  // 计时
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "util/random.h"
#include "util/testutil.h"

namespace tsdb {
namespace disk {

class LogManagerTest : public testing::Test {};

TEST_F(LogManagerTest, Test1) {
  const int num_threads = 4;
  const int num_records = 5000000;
  const int records_per_thread = num_records / num_threads;
  std::string base_path = "/tmp/test_log_manager";
  std::string prefix_name = "kkp_thread";

  boost::filesystem::remove_all(base_path);

  std::vector<std::shared_ptr<LogManager>> lms =
      LogManager::creat_multi_LogManager(num_threads, base_path, prefix_name);

  std::vector<std::vector<std::string>> records(num_threads);
  std::vector<std::vector<uint64_t>> pos(num_threads);

  auto write_records = [](std::shared_ptr<LogManager> lm, int start, int count,
                          std::vector<std::string>& records,
                          std::vector<uint64_t>& pos) {
    leveldb::Random rnd(start);
    for (int i = 0; i < count; ++i) {
      std::string s;
      leveldb::test::RandomString(&rnd, rnd.Uniform(500), &s);
      records.push_back(s);
      pos.push_back(lm->add_record(records.back()));
    }
  };
  //start-time
  auto start_write = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> write_threads;
  for (int i = 0; i < num_threads; ++i) {
    write_threads.push_back(
        std::thread(write_records, lms[i], i + 1, records_per_thread,
                    std::ref(records[i]), std::ref(pos[i])));
  }

  for (auto& t : write_threads) {
    t.join();
  }
  //end-time
  auto end_write = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> write_duration = end_write - start_write;
  std::cout << "Write time: " << write_duration.count() << " seconds."
            << std::endl;

  auto start_read = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> verify_threads;
  for (int i = 0; i < num_threads; ++i) {
    verify_threads.push_back(std::thread([&, i]() {
      for (size_t j = 0; j < records[i].size(); ++j) {
        auto p = lms[i]->read_record(pos[i][j]);
        ASSERT_EQ(records[i][j], std::string(p.first.data(), p.first.size()));
        delete[] p.second;
      }
    }));
  }

  for (auto& t : verify_threads) {
    t.join();
  }

  auto end_read = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> read_duration = end_read - start_read;
  std::cout << "Read time: " << read_duration.count() << " seconds."
            << std::endl;
}

}  // namespace disk
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
