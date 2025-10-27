#include "free_lock_wal.h"
#include <unistd.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <utility>
#include <string>

namespace tsdb {
namespace parallel_wal {
const std::string wal_path = "/tmp/tsdb_big";
// ================================================
// Wal
// ================================================
WalManagement::Wal::Wal(const int &max_buf_size, const std::string &name)
    : buf_(new uint8_t[max_buf_size]),
      next_write_(0),
      running_(true),
      should_stop_(false),
      mu_(),
      cv_(),
//      cq_(),
      t_(&WalManagement::Wal::WriteToDiskLoop, this) {
  fd_.open(name, std::ios::app | std::ios::out);
}

WalManagement::Wal::~Wal() {
  SetStop();
  //cv_.notify_all();
  if (t_.joinable()) {
    t_.join();
  }
  running_ = false;
  fd_.close();
  delete[] buf_;
  buf_ = nullptr;
}

bool WalManagement::Wal::AppendAsync(const uint8_t *src, const uint64_t &len) {
  auto buf = new char[len];
  std::memcpy(buf, src, len);
//  auto ret = cq_.enqueue(std::make_pair(buf, len));
    auto ret = cq_.try_push(std::make_pair(buf, len));
    // cv_.notify_all();
  return ret;
}

void WalManagement::Wal::AppendToBufAsync(const uint8_t *src, const uint64_t &len) {
    if (len >= WriteDiskBound) {
        char* buf = new char[len];
        std::memcpy(buf, src, len);
//        cq_.enqueue(std::make_pair(buf, len));
        cq_.push(std::make_pair(buf, len));
        return;
    }
    std::memcpy(buf_ + next_write_, src, len);
    next_write_ += len;
    if (next_write_ >= WriteDiskBound) {
        char* buf = new char[len];
        std::memcpy(buf, src, len);
//        cq_.enqueue(std::make_pair(buf, len));
        cq_.push(std::make_pair(buf, len));
        next_write_ = 0;
        return;
    }
}

bool WalManagement::Wal::Flush() {
  bool res = true;
  std::pair<char *, uint32_t> node = {nullptr, 0};
//  while (cq_.try_dequeue(node)) {
//    auto ret = Append(node.first, node.second);
//    delete node.first;
//    node.first = nullptr;
//    if (!ret) {
//      res = false;
//    }
//  }
    while (cq_.try_pop(node)) {
        auto ret = Append(node.first, node.second);
        delete node.first;
        node.first = nullptr;
        if (!ret) {
            res = false;
        }
    }
  if (next_write_) {
    fd_.write(reinterpret_cast<const char *>(buf_), next_write_);
    next_write_ = 0;
  }
  return res && !!fd_;
}

void WalManagement::Wal::SetStop() {
  should_stop_ = true;
  //cv_.notify_all();
}

bool WalManagement::Wal::IsStop() { return !running_; }

bool WalManagement::Wal::ShouldStop() { return should_stop_; }

void WalManagement::Wal::WriteToDiskLoop() {
  std::pair<char *, uint32_t> node = {nullptr, 0};
//  bool ret = false;
//  while (!ShouldStop()) {
//    std::unique_lock lock(mu_);
//    cv_.wait(lock, [this, &node, &ret]() {
//      ret = cq_.try_dequeue(node) || ShouldStop();
//      return ret;
//    });
//
//    if (ShouldStop()) {
//      delete node.first;
//      node.first = nullptr;
//      goto error;
//    }
//
//    do {
//      auto ret = Append(node.first, node.second);
//      delete node.first;
//      node.first = nullptr;
//      if (!ret) {
//        goto error;
//      }
//    } while (cq_.try_dequeue(node));
//  }
//error:
//  Flush();
//  SetStop();
  while (!ShouldStop()){
//      while(cq_.try_dequeue(node)){
////        Append(node.first, node.second);
//        fd_.write(reinterpret_cast<const char *>(node.first), node.second);
//        delete node.first;
//        node.first = nullptr;
//      }
      while(cq_.try_pop(node)){
//        Append(node.first, node.second);
          fd_.write(reinterpret_cast<const char *>(node.first), node.second);
          delete node.first;
          node.first = nullptr;
      }
  }

}

bool WalManagement::Wal::Append(const char *start, const uint32_t &len) {
  std::memcpy(buf_ + next_write_, start, len);
  next_write_ += len;
  if (next_write_ > WriteDiskBound) {
    fd_.write(reinterpret_cast<const char *>(buf_), next_write_);
    next_write_ = 0;
  }
  return !!fd_;
}

// ================================================
// WalManagement
// ================================================
WalManagement::WalManagement(const int &wal_num, const int &max_buf_size, const std::vector<std::string> &names)
    : wal_sz_(wal_num) {
  wals_.reserve(wal_num);
  for (int i = 0; i < wal_num; i++) {
    wals_.emplace_back(std::make_unique<Wal>(max_buf_size, names[i]));
  }
}

WalManagement::WalManagement(const int &wal_num, const int &max_buf_size)
    : wal_sz_(wal_num) {
  wals_.reserve(wal_num);
  std::vector<std::string>path;
  leveldb::Env* env = leveldb::Env::Default();
  for (int i = 0; i < wal_num; i++) {
    std::string dir = wal_path+std::string("/metric") + std::to_string(i);
    auto s = env->CreateDir(dir);
    if (!s.ok()) {
      return;
    }
    path.emplace_back(dir + std::string("/sample") + std::to_string(i));
  }
  for (int i = 0; i < wal_num; i++) {
    wals_.emplace_back(std::make_unique<Wal>(max_buf_size, path[i]));
  }
  std::string tag_wal_path = wal_path + "/tag_log";
  tag_wal_ = std::make_unique<Wal>(max_buf_size, tag_wal_path);
}

void WalManagement::AppendAsync(const int &idx, const uint8_t *src, const uint64_t &len) {
//  wals_[idx % wal_sz_]->AppendAsync(src, len);
    wals_[idx % wal_sz_]->AppendToBufAsync(src, len);
}

bool WalManagement::Flush(const int &idx) { return wals_[idx % wal_sz_]->Flush(); }

}  // namespace parallel_wal
}  // namespace tsdb