#include "ThmapPostings.h"

#include "VectorPostings.hpp"

namespace tsdb::index{

    ThmapPostings::ThmapPostings(std::string dir) : mergeset_postings_(dir) {
        queue_clear_ = false;
        key_map_ = thmap_create(0, nullptr, 0);
        label_name_lock_ = (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock) * label_name_lock_num);
        label_value_lock_ = (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock) * label_value_lock_num_);

        for (int i = 0; i < label_name_lock_num; i++) {
            label_name_lock_[i].reset();
        }
        for (int i = 0; i < label_value_lock_num_; i++) {
            label_value_lock_[i].reset();
        }
    }

  ThmapPostings::~ThmapPostings() {
    thmap_destroy(key_map_);
    free(label_value_lock_);
    free(label_name_lock_);
  }

  ConcurrencyPostingList* ThmapPostings::get(const label::Label &l) {
    void* ret_value_map = thmap_get(key_map_, l.label.data(), l.label.size());
    if (ret_value_map == NULL) {
        return nullptr;
    }
    auto value_map = (thmap_t*) ret_value_map;
    void* ret_prefix_posting = thmap_get(value_map, l.value.data(), l.value.size());
    if (ret_prefix_posting == NULL) {
        auto id_arr = read_from_mergeset(l);
        if (id_arr.size() == 0) {
            return nullptr;
        }
        auto posting = new ConcurrencyPostingList();
        for (auto id : id_arr) {
            posting->mtx_.lock();
            posting->posting_list_.insert(id);
            posting->hot_value_.fetch_add(1);
            posting->mtx_.unlock();
            thmap_put(key_map_, l.label.data(), l.label.size(), value_map);
            thmap_put(value_map, l.value.data(), l.value.size(), posting);
            hot_queue_.push(l);
            label_num_.fetch_add(1);
        }
        return posting;
    }
    return (ConcurrencyPostingList*)ret_prefix_posting;
  }

bool ThmapPostings::get_and_read(const label::Label &l, std::vector<uint64_t> &listing) {
      void* ret_value_map = thmap_get(key_map_, l.label.data(), l.label.size());
      if (ret_value_map == NULL) {
          return false;
      }
      auto value_map = (thmap_t*) ret_value_map;
      void* ret_prefix_posting = thmap_get(value_map, l.value.data(), l.value.size());
      if (ret_prefix_posting == NULL) {
          auto id_arr = read_from_mergeset(l);
          if (id_arr.size() == 0) {
              return false;
          }
          for (auto id : id_arr) {
              listing.emplace_back(id);
          }
          return true;
      }
      auto p = (ConcurrencyPostingList*)ret_prefix_posting;
      p->mtx_.lock();
      p->posting_list_.reset_cursor();
      while (p->posting_list_.next()) {
          listing.emplace_back(p->posting_list_.at());
      }
      p->posting_list_.reset_cursor();
      p->mtx_.unlock();
      return true;
  }

  void ThmapPostings::add(uint64_t id, const label::Label& l) {
    if(hot_queue_.was_size() > HOT_QUEUE_SIZE / 2){
        mtx_.lock();
        if(queue_clear_){
            mtx_.unlock();
        }else{
            queue_clear_ = true;
            mtx_.unlock();
            label::Label l;
            while (hot_queue_.try_pop(l)){
                hot_list_.emplace_back(l, get(l)->hot_value_.load());
            }
            queue_clear_ = false;
        }
    }
    void* ret_value_map = thmap_get(key_map_, l.label.data(), l.label.size());
    if (ret_value_map != NULL) {
      auto value_map = (thmap_t*) ret_value_map;
      void* ret_prefix_posting = thmap_get(value_map, l.value.data(), l.value.size());
      if (ret_prefix_posting != NULL) {
        auto posting = (ConcurrencyPostingList*)ret_prefix_posting;
        posting->mtx_.lock();
        posting->posting_list_.insert(id);
        posting->hot_value_.fetch_add(1);
        posting->mtx_.unlock();
        return;
      } else {
        uint64_t hash_id = label::lb_value_label_hash(l);
        label_value_lock_[hash_id % label_value_lock_num_].lock();
        void* check = thmap_get(value_map, l.value.data(), l.value.size());
        if(check){
            auto check_posting = (ConcurrencyPostingList*) check;
            check_posting->mtx_.lock();
            check_posting->posting_list_.insert(id);
            check_posting->hot_value_.fetch_add(1);
            check_posting->mtx_.unlock();
            label_value_lock_[hash_id % label_value_lock_num_].unlock();
            return;
        }
        auto posting = new ConcurrencyPostingList();
        posting->mtx_.lock();
        posting->posting_list_.insert(id);
        posting->hot_value_.fetch_add(1);
        posting->mtx_.unlock();
        thmap_put(key_map_, l.label.data(), l.label.size(), value_map);
        thmap_put(value_map, l.value.data(), l.value.size(), posting);
        label_value_lock_[hash_id % label_value_lock_num_].unlock();
        hot_queue_.push(l);
        label_num_.fetch_add(1);
        return;
      }
    } else {
      uint64_t hash_id1 = label::lb_name_label_hash(l);
      label_name_lock_[hash_id1 % label_name_lock_num].lock();
      ret_value_map = thmap_get(key_map_, l.label.data(), l.label.size());
      if(ret_value_map){
          auto value_map = (thmap_t*) ret_value_map;
          void* ret_prefix_posting = thmap_get(value_map, l.value.data(), l.value.size());
          if(ret_prefix_posting){
              auto check_posting = (ConcurrencyPostingList*) ret_prefix_posting;
              check_posting->mtx_.lock();
              check_posting->posting_list_.insert(id);
              check_posting->hot_value_.fetch_add(1);
              check_posting->mtx_.unlock();
          }else{
              uint64_t  hash_id2 = label::lb_value_label_hash(l);
              label_value_lock_[hash_id2 % label_value_lock_num_].lock();
              ret_prefix_posting = thmap_get(value_map, l.value.data(), l.value.size());
              if(ret_prefix_posting){
                  auto check_posting = (ConcurrencyPostingList*) ret_prefix_posting;
                  check_posting->mtx_.lock();
                  check_posting->posting_list_.insert(id);
                  check_posting->hot_value_.fetch_add(1);
                  check_posting->mtx_.unlock();
              }else{
                  void* check = thmap_get(value_map, l.value.data(), l.value.size());
                  if(check){
                      auto check_posting = (ConcurrencyPostingList*) check;
                      check_posting->mtx_.lock();
                      check_posting->posting_list_.insert(id);
                      check_posting->hot_value_.fetch_add(1);
                      check_posting->mtx_.unlock();
                  }else{
                      auto posting = new ConcurrencyPostingList();
                      posting->mtx_.lock();
                      posting->posting_list_.insert(id);
                      posting->mtx_.unlock();
                      thmap_put(value_map, l.value.data(), l.value.size(), posting);
                  }
              }
              label_value_lock_[hash_id2 % label_value_lock_num_].unlock();
              hot_queue_.push(l);
              label_num_.fetch_add(1);
          }
          label_name_lock_[hash_id1 % label_name_lock_num].unlock();
          return;
      }
      auto value_map = thmap_create(0, nullptr, 0);
      auto posting = new ConcurrencyPostingList();
      posting->mtx_.lock();
      posting->posting_list_.insert(id);
      posting->hot_value_.fetch_add(1);
      posting->mtx_.unlock();
      thmap_put(key_map_, l.label.data(), l.label.size(), value_map);
      thmap_put(value_map, l.value.data(), l.value.size(), posting);
      label_name_lock_[hash_id1 % label_name_lock_num].unlock();
      hot_queue_.push(l);
      label_num_.fetch_add(1);
      return;
    }
  }

  void ThmapPostings::add(uint64_t id, const label::Labels& ls) {
    for (auto &it : ls) {
      if(it.label == "__name__")continue;
      add(id, it);
    }
    return;
  }

  void ThmapPostings::del(uint64_t id, const label::Label& l) {
    void* ret_value_map = thmap_get(key_map_, l.label.data(), l.label.size());
    if (ret_value_map == NULL) {
      return;
    }
    auto value_map = (thmap_t*) ret_value_map;
    void* posting = thmap_get(value_map, l.value.data(), l.value.size());
    if (posting == NULL) {
      return;
    }
    auto ret_posting = (ConcurrencyPostingList*)posting;
    ret_posting->posting_list_.remove(id);
    return;
  }

  bool ThmapPostings::remove_postinglist(const label::Label &l) {
      void* ret_value_map = thmap_get(key_map_, l.label.data(), l.label.size());
      if (ret_value_map == NULL) {
          return false;
      }
      auto value_map = (thmap_t*) ret_value_map;
      void* ret_prefix_posting = thmap_get(value_map, l.value.data(), l.value.size());
      if (ret_prefix_posting == NULL) {
          return false;
      }
      thmap_del(value_map,l.value.data(),l.value.size());
      return true;
  }

    uint32_t ThmapPostings::migrate(double percent) {
        for(uint32_t i=0;i<hot_list_.size();i++){
            hot_list_[i].second = get(hot_list_[i].first)->hot_value_.load();
        }
        uint32_t batch_size = label_num_*percent;
        label::Label l;
        while(hot_queue_.try_pop(l)){
            auto num = get(l)->hot_value_.load();
            hot_list_.emplace_back(label::Label(l), num);
        }
        std::sort(hot_list_.begin(),hot_list_.end(),cmp);
        for(uint32_t i=hot_list_.size()-batch_size;i<hot_list_.size();i++){
            mergeset_postings_.AddMergeSet(leveldb::WriteOptions(), hot_list_[i].first, get(hot_list_[i].first));
        }

        for (uint32_t i=hot_list_.size()-batch_size;i<hot_list_.size();i++){
            void* ret_value_map = thmap_get(key_map_, hot_list_[i].first.label.data(), hot_list_[i].first.label.size());
            auto value_map = (thmap_t*) ret_value_map;
            thmap_del(value_map, hot_list_[i].first.value.data(), hot_list_[i].first.value.size());
        }

        mergeset_postings_.CompactAll();

        return batch_size;
    }

    std::vector<uint64_t> ThmapPostings::read_from_mergeset(const label::Label& l) {
        return mergeset_postings_.GetAndReadMergeSet(leveldb::ReadOptions(), l);
    }

    leveldb::Iterator* ThmapPostings::iterator() {
        return mergeset_postings_.iterator(leveldb::ReadOptions());
    }
}