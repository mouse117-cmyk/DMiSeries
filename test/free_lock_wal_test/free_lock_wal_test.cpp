#include <cstdlib>
#include <functional>
#include <string>
#include <thread>
#include <cstdio>
#include <vector>
#include <boost/filesystem.hpp>
#include "parallel_wal/free_lock_wal.h"

const int loop = 1000;
const int wal_num = 100;
const int thread_num = wal_num * 2;
const int max_buf_size = WriteDiskBound * 2;

void Write(tsdb::parallel_wal::WalManagement &wal_management, const int &idx) {
  int loop = ::loop;
  auto buf = new char[max_buf_size];
  while (loop--) {
    int len = rand() % max_buf_size;
    len = 32;
    for (int i = 0; i < len; i++) {
      buf[i] = static_cast<char>((rand() % 26) + 'a');
    }
    wal_management.AppendAsync(idx, reinterpret_cast<const uint8_t *>(buf), len);
  }
  //wal_management.Flush(idx);
}

int main() {
  std::vector<std::string> names;
  names.reserve(wal_num);
  for (int i = 0; i < wal_num; i++) {
    names.emplace_back(std::string("wal_test_file") + std::to_string(i));
  }
  for(auto path:names){
    std::remove(path.c_str());
  }
  auto wal_management = tsdb::parallel_wal::WalManagement(wal_num, max_buf_size, names);
  std::vector<std::thread> ts;
  ts.reserve(thread_num);
  for (int i = 0; i < thread_num; i++) {
    ts.emplace_back(Write, std::ref(wal_management), i % wal_num);
  }
  for (int i = 0; i < thread_num; i++) {
    if (ts[i].joinable()) {
      ts[i].join();
    }
  }
  return 0;
}