#ifndef THMAPPOSTINGS_H
#define THMAPPOSTINGS_H

#include "PostingsInterface.hpp"
#include "prefix_postings.h"
#include "label/Label.hpp"
#include "third_party/thmap/thmap.h"
#include "third_party/atomic_queue/include/atomic_queue/atomic_queue.h"
#include "mem/inverted_index.h"
#include <shared_mutex>
#include <algorithm>
#include "mem/mergeset.h"
#include "mem/mergeset_version_set.h"
#include "MergeSetPostings.h"
#define LABEL_NAME_LOCK_NUM 1<<8
#define LABEL_VALUE_LOCK_NUM 1<<16
#define HOT_QUEUE_SIZE 1<<16
namespace tsdb::index {
class ThmapPostings{
 private:
  thmap_t* key_map_;
  int label_name_lock_num = LABEL_NAME_LOCK_NUM;
  int label_value_lock_num_ = LABEL_VALUE_LOCK_NUM;
  mem::SpinLock* label_name_lock_;
  mem::SpinLock* label_value_lock_;
  std::atomic<uint32_t> label_num_{0};
  atomic_queue::AtomicQueueB2<label::Label> hot_queue_{HOT_QUEUE_SIZE};
  std::mutex mtx_;
  bool queue_clear_;
  std::vector<std::pair<label::Label,uint32_t>>hot_list_;
  std::vector<std::pair<label::Label,uint32_t>>cold_list_;

  MergeSetPostings mergeset_postings_;

 public:
  ThmapPostings(std::string dir);
  ~ThmapPostings();
    static bool cmp(std::pair<label::Label,uint32_t> a, std::pair<label::Label,uint32_t> b){
        return a.second > b.second;
    }
  ConcurrencyPostingList* get(const label::Label & l);
  bool get_and_read(const label::Label &l, std::vector<uint64_t> &listing);
  void add(uint64_t id, const label::Label & l);
  void add(uint64_t id, const label::Labels & ls);

  void del(uint64_t id, const label::Label & l);
  bool remove_postinglist(const label::Label& l);

  uint32_t migrate(double percent = 0.1);
  std::vector<uint64_t> read_from_mergeset(const label::Label& l);
  leveldb::Iterator* iterator();
};
}

#endif  // THMAPPOSTINGS_H
