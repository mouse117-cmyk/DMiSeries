#ifndef TEMPUSCSTORE_SPANHEAD_H
#define TEMPUSCSTORE_SPANHEAD_H

#include <stdint.h>

#include <unordered_map>
#include <unordered_set>
#include <string>
#include "base/Atomic.hpp"
#include "base/Error.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "db/AppenderInterface.hpp"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "disk/log_manager.h"
#include "index/MemPostings.hpp"
#include "leveldb/db.h"
#include "leveldb/db/log_writer.h"
#include "leveldb/env.h"
#include "mem/go_art.h"
#include "mem/indirection_manager.h"
#include "mem/indirection_mapping_management.h"
#include "mem/inverted_index.h"
#include "third_party/art.h"
#include "third_party/thmap/thmap.h"
#include "tsdbutil/tsdbutils.hpp"
#include "parallel_wal/free_lock_wal.h"
#include "TreeSeries/TreeSeries.h"
#include "TreeMemSeries.h"
#include "index/ThmapPostings.h"
#include "parallel_wal/parallel_wal_span.h"
#include "mem/mem_postings.h"
#include "leveldb/db/memtable.h"


namespace tsdb{
    namespace head{
        class SpanHead {
        private:
            friend class SpanHeadAppender;
            friend class HeadChunkReader;
            friend class HeadIndexReader;
            friend class HeadIndexWriter;

            parallel_wal::PWalManagementSpan pwal_m_;
            std::string log_path_;

            base::AtomicInt64 min_time;
            base::AtomicInt64 max_time;
            base::AtomicInt64 valid_time;  // Shouldn't be lower than the max_time of the
            // last persisted block
            std::atomic<uint64_t> last_source_group_id_;
            std::atomic<uint16_t> last_metric_id_;
            std::vector<std::unique_ptr<mem::IndirectionManager>> indirection_manager_;


            int source_lock_num_ = SOURCE_NUM; // 2^16
            int metric_lock_num_ = METRIC_NUM; // 2^9
            mem::SpinLock* source_locks_;
            mem::SpinLock* metric_locks_;
            thmap_t* metric_thmap_; // label -> metric_id
            thmap_t* source_thmap_;

            index::ThmapPostings inverted_index_;
            std::vector<thmap_t*> forward_index_; // metric_id -> source_id -> memseries
            uint64_t* flat_forward_index_;
            uint16_t migrate_mid_;
            uint64_t migrate_sgid_;
            std::atomic<uint64_t> flat_version_;

            base::RWMutexLock mutex_;
            mem::art_tree symbols_;
            std::unique_ptr<mem::InvertedIndex> posting_list;

            std::vector<std::unordered_map<uint64_t, std::vector<uint64_t>>> hash_shards_;
            mem::SpinLock* hash_locks_;

            error::Error err_;

            std::string key_;

            // the first lsm-tree for KV-separated lsm-stores
            std::string dir_sep_;
            leveldb::DB* db_sep_;

            // the second lsm-tree for full store
            std::string dir_;
            leveldb::DB* db_;

            //update in-place
            slab::TreeSeries* tree_series_;

            bool sync_api_;
            bool no_log_;

            std::atomic<bool> bg_flush_;
            std::atomic<bool> bg_clean_samples_logs_;
            uint64_t flush_cnt_;

            uint8_t* slab_buffer_;

            /******************** Concurrency ********************/
            bool concurrency_enabled_;
            mem::Epoch* local_epochs_[MAX_WORKER_NUM];
            mem::SpinLock* spinlocks_[MAX_WORKER_NUM];
            mem::AlignedBool* running_[MAX_WORKER_NUM];
            mem::Epoch* global_epoch_;
            moodycamel::ConcurrentQueue<mem::GCRequest> gc_requests_;
            // mem::AlignedBool* gc_running_;
            mem::AlignedBool* global_running_;
            base::WaitGroup wg_;
            friend void _head_global_garbage_collection(SpanHead* h);

            /******************** Memory to disk migration ********************/
            std::atomic<uint64_t> mem_to_disk_migration_threshold_;
            bool migration_enabled_;
            mem::AlignedBool* migration_running_;
            friend void _head_mem_to_disk_migration(SpanHead* h);

        public:

            SpanHead(const std::string& dir_sep, const std::string& dir, const std::string& log_path, const std::string& snapshot_dir = "",
                       slab::TreeSeries* tree_series = nullptr, bool sync = false);

            slab::TreeSeries* get_tree_series() {return tree_series_;}
            leveldb::DB* get_first_db() { return db_sep_; }
            leveldb::DB* get_second_db() { return db_; }

            uint64_t migrate_inverted_index (double percent) { return inverted_index_.migrate(percent); }

            uint64_t  get_last_source_group_id() {return last_source_group_id_;}
            uint16_t  get_last_metric_id() {return last_metric_id_;}
            std::pair<TreeMemSeries*, bool> get_or_create_by_thmap(const label::Labels& lset,uint64_t epoch=0);
            std::pair<TreeMemSeries*, bool> get_or_create_with_id(uint64_t source_id, uint16_t mid, const label::Labels & lset);

            uint64_t get_or_create_source_id(const label::Labels& lset, bool &is_exist, uint64_t epoch=0);
            uint16_t get_or_create_metric_id(const label::Labels& lset, bool &is_exist, uint64_t epoch=0);

            TreeMemSeries* get_from_forward_index(uint64_t source_id, uint16_t metric_id);
            void set_to_forward_index(uint64_t source_id, uint16_t metric_id, TreeMemSeries* ms);

            void alloc_flat_forward_index(uint16_t mid, uint64_t sgid) {
                flat_forward_index_ = static_cast<uint64_t *>(malloc(mid * sgid * sizeof(uint64_t)));
            }

            void destroy_flat_forward_index() {
                if (flat_forward_index_ != nullptr) {
                    free(flat_forward_index_);
                    migrate_mid_ = 0;
                    migrate_sgid_ = 0;
                    flat_forward_index_ = nullptr;
                }
            }

            uint64_t* get_flat_forward_index() {
                return flat_forward_index_;
            }

            uint64_t get_flat_version() { return flat_version_.load(); }
            void incr_flat_version() { flat_version_.fetch_add(1); }

            uint16_t get_migrate_mid() {
                return migrate_mid_;
            }

            uint64_t get_migrate_sgid() {
                return migrate_sgid_;
            }

            bool migrate_to_flat_forward_index(uint64_t sgid, uint16_t mid);

            TreeMemSeries* read_flat_forward_index(uint64_t sgid, uint16_t mid);

            void update_min_max_time(int64_t mint, int64_t maxt);


            std::unique_ptr<db::AppenderInterface> head_appender();

            std::unique_ptr<db::AppenderInterface> appender();

            int64_t MinTime() const {
                return const_cast<base::AtomicInt64*>(&min_time)->get();
            }
            int64_t MaxTime() const {
                return const_cast<base::AtomicInt64*>(&max_time)->get();
            }

            // init_time initializes a head with the first timestamp. This only needs to
            // be called for a completely fresh head with an empty WAL. Returns true if
            // the initialization took an effect.
            bool init_time(int64_t t);

            bool overlap_closed(int64_t mint, int64_t maxt) const {
                // The block itself is a half-open interval
                // [pb.meta.MinTime, pb.meta.MaxTime).
                return MinTime() <= maxt && mint < MaxTime();
            }

            error::Error error() const { return err_; }

            leveldb::Status flush_db();

            void flush_wal() { pwal_m_.FlushAll();}

            void reset_flush_txn();
            leveldb::Status recover_parallel();
            leveldb::Status recover_serial();
            leveldb::Status recover_index_from_log();
            leveldb::Status recover_samples_from_log(int seq);

            leveldb::Status clean_sample_log(int seq);
            leveldb::Status clean_samples_logs();
            void bg_clean_samples_logs();
            inline void bg_start_clean_samples_logs() { bg_clean_samples_logs_.store(true); }
            inline void bg_stop_clean_samples_logs() { bg_clean_samples_logs_.store(false); }

            void bg_flush_data(){
                bg_flush_.store(true);
                std::thread bg_flush_tree_series([this]() {
                    sleep(1);
                    MasstreeWrapper<slab::SlabInfo>::ti=threadinfo::make(threadinfo::TI_PROCESS, 1000);
                    while (bg_flush_) {
                        sleep(0.5);
                        while(bg_flush_&&(tree_series_->GetFullMemSlabNum() > tree_series_ -> GetBatchWriteNum())){
                            tree_series_->OptBatchWriteLazyFlush();
                            //tree_series_->OptBatchWrite();
                            flush_cnt_++;
//                            std::cout<<"+++++++++++++++++++++++++++++++++++++++++[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["<<std::endl;
//                            std::cout<<tree_series_->GetDB()->mem()->ApproximateMemoryUsage()<<std::endl;
//                            auto iter = tree_series_->GetDB()->mem()->NewIterator();
//                            iter->SeekToFirst();
//                            uint64_t cnt = 0;
//                            while (iter->Valid()) {
//                                std::cout<<iter->key().data()<<" "<<iter->value().data()<<std::endl;
//                                cnt++;
//                                iter->Next();
//                            }
//                            std::cout<<"memtable kv cnt: "<<cnt<<std::endl;
//                  if (flush_cnt_ % 5 == 0 && (last_metric_id_-migrate_mid_)*(last_source_group_id_-migrate_sgid_) > 10000) {
//                      migrate_to_flat_forward_index(last_source_group_id_, last_metric_id_);
//                  }
                        }
                    }
                });
                std::thread bg_flush_db([this]() {
                    sleep(1);
                    MasstreeWrapper<slab::SlabInfo>::ti=threadinfo::make(threadinfo::TI_PROCESS, 1000);
                    while (bg_flush_) {
                        sleep(0.5);
                        while (bg_flush_&&(tree_series_->GetUsedDiskSlabNum() > tree_series_ -> GetMigrateNum())){
                            flush_db();
                        }
                    }
                });
                bg_flush_tree_series.detach();
                bg_flush_db.detach();
            }

            void stop_bg_flush_data(){
                bg_flush_.store(false);
            }

            void stop_bg_wal() { pwal_m_.Stop(); }

            void print_forward_index();
            leveldb::Status snapshot_index();

            int memseries_gc(
                    int percentage, uint64_t epoch = 0,
                    moodycamel::ConcurrentQueue<mem::GCRequest>* gc_queue = nullptr);
            int memseries_gc_preflush(int percentage);

            void full_migrate();

            void set_inverted_index_gc_threshold(uint64_t mem_threshold) {
                posting_list->set_mem_threshold(mem_threshold);
            }
            int inverted_index_gc() { return posting_list->try_migrate(); }
            mem::InvertedIndex* inverted_index() { return posting_list.get(); }

            void set_mergeset_manager(const std::string& dir, leveldb::Options opts) {
                posting_list->set_mergeset_manager(dir, opts);
            }

            ~SpanHead();

            /******************** index reader ********************/
            std::set<std::string> symbols();
            std::vector<std::string> label_values(const std::string& name);
            std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
                    const std::string& name, const std::string& value);

            auto DecodeChunk(uint8_t *chunk) -> std::pair<std::vector<int64_t>, std::vector<double>>;
            bool series(uint64_t source_id, uint16_t metric_id, label::Labels& lset, std::string* chunk_contents);
            std::vector<std::string> label_names() const;
            std::unique_ptr<index::PostingsInterface> sorted_postings(
                    std::unique_ptr<index::PostingsInterface>&& p);

            std::unique_ptr<index::PostingsInterface> select(
                    const std::vector<std::shared_ptr<::tsdb::label::MatcherInterface>>& l);

            /******************** Concurrency ********************/
            void enable_concurrency();
            leveldb::Status register_thread(int* idx);
            void deregister_thread(int idx);
            void update_local_epoch(int idx);
            uint64_t get_epoch(int idx) { return local_epochs_[idx]->load(); }

            /******************** Memory to disk migration ********************/
            void set_mem_to_disk_migration_threshold(size_t s) {
                mem_to_disk_migration_threshold_.store(s);
                posting_list->set_mem_threshold(s);
            }
            void enable_migration();
            void disable_migration();
        };

    }
}

#endif //TEMPUSCSTORE_SPANHEAD_H
