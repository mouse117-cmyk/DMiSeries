#ifndef TSDB_UNITTEST_FREE_LOCK_WAL_H
#define TSDB_UNITTEST_FREE_LOCK_WAL_H

// #include "leveldb/db/log_format.h"
#include "third_party/moodycamel/concurrentqueue.h"
#include "third_party/atomic_queue/include/atomic_queue/atomic_queue.h"
#include <leveldb/include/leveldb/env.h>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#define WriteDiskBound (4 * 1024)
namespace tsdb {
namespace parallel_wal {
class WalManagement {
  class Wal {
   public:
    Wal(const int &max_buf_size, const std::string &name);
    ~Wal();
    bool AppendAsync(const uint8_t *src, const uint64_t &len);
    void AppendToBufAsync(const uint8_t *src, const uint64_t &len);
    bool Flush();
    void SetStop();
    bool IsStop();

   private:
    bool ShouldStop();
    void WriteToDiskLoop();
    bool Append(const char *start, const uint32_t &len);
    uint8_t *buf_ = nullptr;
    std::ofstream fd_;
    uint32_t next_write_;

    std::atomic<bool> running_;
    std::atomic<bool> should_stop_;

    std::mutex mu_;
    std::condition_variable cv_;
//    moodycamel::ConcurrentQueue<std::pair<char *, uint32_t>> cq_;
    atomic_queue::AtomicQueueB2<std::pair<char*, uint32_t>> cq_{64 * 1024};

    std::thread t_;
  };

 public:
  WalManagement(const int &wal_num, const int &max_buf_size, const std::vector<std::string> &names);
  WalManagement(const int &wal_num, const int &max_buf_size);

  void AppendAsync(const int &idx, const uint8_t *src, const uint64_t &len);

  Wal* getTagWal() {return tag_wal_.get();}

  bool Flush(const int &idx = -1);

 private:
  std::vector<std::unique_ptr<Wal>> wals_;
  const int wal_sz_;
  std::unique_ptr<Wal>tag_wal_;
};
}  // namespace parallel_wal
}  // namespace tsdb
#endif  // TSDB_UNITTEST_FREE_LOCK_WAL_H
