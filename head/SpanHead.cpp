#include "SpanHead.h"
#include <sys/wait.h>

#include <algorithm>
#include <boost/bind.hpp>
#include <future>
#include <iostream>
#include <limits>
#include <thread>

#include "TreeHead.h"
#include "Head.hpp"
#include "SpanHead.h"
#include "base/Logging.hpp"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "head/SpanHeadAppender.h"
#include "index/IntersectPostings.hpp"
#include "index/VectorPostings.hpp"
#include "querier/ChunkSeriesIterator.hpp"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/RecordDecoder.hpp"
#include "tsdbutil/RecordEncoder.hpp"
#include "util/mutexlock.h"
#include "wal/checkpoint.hpp"
#include "leveldb/db/log_writer.h"
#include "index/EmptyPostings.hpp"
#include "mem/mem_postings.h"

namespace tsdb{
    namespace head{

        SpanHead::SpanHead(const std::string& dir_sep, const std::string& dir, const std::string& log_path, const std::string& snapshot_dir,
                               slab::ValueLog* value_log, bool sync)
                           : pwal_m_(this, wal_num, log_path),log_path_(log_path),last_source_group_id_(0),last_metric_id_(0),
                             hash_shards_(STRIPE_SIZE),inverted_index_(log_path_),dir_sep_(dir_sep),dir_(dir),value_log_(value_log),
                             sync_api_(sync),concurrency_enabled_(false),flush_cnt_(0),flat_version_(0),
                             mem_to_disk_migration_threshold_(MEM_TO_DISK_MIGRATION_THRESHOLD),migration_enabled_(false),no_log_(false) {

            min_time.getAndSet(std::numeric_limits<int64_t>::max());
            max_time.getAndSet(std::numeric_limits<int64_t>::min());
            metric_thmap_ = thmap_create(0, nullptr, 0);
            source_thmap_ = thmap_create(0, nullptr, 0);

            source_locks_ = (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock) * source_lock_num_);
            metric_locks_ = (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock) * metric_lock_num_);

            for (int i = 0; i < metric_lock_num_; i++) {
                metric_locks_[i].reset();
            }

            for (int i = 0; i < source_lock_num_; i++) {
                source_locks_[i].reset();
            }

            forward_index_.resize(MAX_METRIC_NUM);
            for (int i = 0; i < MAX_METRIC_NUM; i++) {
                forward_index_[i] = thmap_create(0, nullptr, 0);
            }

            slab_buffer_ = static_cast<uint8_t *>(tsmmap(4*KB));

            flat_forward_index_ = nullptr;
            migrate_mid_ = 0;
            migrate_sgid_ = 0;

            hash_locks_ =
                    (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock) * STRIPE_SIZE);
            for (int i = 0; i < STRIPE_SIZE; i++) hash_locks_[i].reset();

            mem::art_tree_init(&symbols_);

            posting_list = std::unique_ptr<mem::InvertedIndex>(
                    new mem::InvertedIndex(dir, snapshot_dir));
            if (!sync_api_) posting_list->start_all_stages();

            leveldb::Options options;
            options.create_if_missing = true;
            options.max_imm_num = 5;
            options.write_buffer_size = 256 * 1024 * 1024;
            options.max_file_size = 256 * 1024 * 1024;
            options.use_log = false;
            leveldb::Status st = leveldb::DB::Open(options, dir_sep, &db_sep_);
            db_sep_->SetSpanHead(this);

            value_log_->SetDB(db_sep_);

            options.max_imm_num = 3;
            options.write_buffer_size = 256 * 1024 * 1024;
            options.max_file_size = 256 * 1024 * 1024;
            st = leveldb::DB::Open(options, dir, &db_);
            db_->SetSpanHead(this);

            bg_start_clean_samples_logs();
            bg_clean_samples_logs();
        }

        SpanHead::~SpanHead() {
            if (!sync_api_) posting_list->stop_all_stages();
            auto clean_mem_series = [](uint64_t v) {
                if ((v >> 63) == 0) delete (MemSeries*)(v);
            };
            for(int i=0;i<indirection_manager_.size();i++){
                indirection_manager_[i]->iter(clean_mem_series);
            }

            bg_clean_samples_logs_.store(false);

            thmap_destroy(source_thmap_);
            thmap_destroy(metric_thmap_);

            for (int i = 0; i < MAX_METRIC_NUM; i++) {
                thmap_destroy(forward_index_[i]);
            }
            forward_index_.clear();
            tsmunmap(slab_buffer_,4*KB);
            destroy_flat_forward_index();
            delete flat_forward_index_;

            free(source_locks_);
            free(metric_locks_);
            free(hash_locks_);
            mem::art_tree_destroy(&symbols_);

            if (concurrency_enabled_) {
                global_running_->set(false);
            }
            if (migration_enabled_) {
                migration_running_->set(false);
            }
            wg_.wait();
            if (concurrency_enabled_) {
                for (int i = 0; i < MAX_WORKER_NUM; i++) {
                    free(local_epochs_[i]);
                    free(spinlocks_[i]);
                    free(running_[i]);
                }
                free(global_epoch_);
                free(global_running_);
            }
            if (migration_enabled_) free(migration_running_);
        }

        void SpanHead::reset_flush_txn() {
            for (uint64_t i = 1; i <= last_source_group_id_; i++) {
                for (uint16_t j = 1; j <=last_metric_id_; j++) {
//            auto tms = get_from_forward_index(i, j);
                    auto tms = read_flat_forward_index(i, j);
                    if (tms == nullptr) continue;
                    tms->flushed_txn_.store(tms->sample_txn_);
                }
            }
        }

        leveldb::Status SpanHead::recover_parallel() {
            std::vector<std::thread> threads;
            threads.reserve(wal_num);
            recover_index_from_log();
            for (int i = 0; i < wal_num; i++) {
                threads.emplace_back([this, i]() {
                    recover_samples_from_log(i);
                });
            }
            for (auto& thread : threads) {
                thread.join();
            }
            reset_flush_txn();
            return leveldb::Status::OK();
        }

        leveldb::Status SpanHead::recover_serial() {
            recover_index_from_log();
            for (int i = 0; i < wal_num; i++) {
                recover_samples_from_log(i);
            }
            reset_flush_txn();
            return leveldb::Status::OK();
        }

        leveldb::Status SpanHead::recover_index_from_log() {
            MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16);
            leveldb::Env* env = leveldb::Env::Default();
            leveldb::Status s;

            std::vector<std::string> existing_logs;
            boost::filesystem::path p(log_path_ + "/metric" + std::to_string(0));
            boost::filesystem::directory_iterator end_itr;

            if (!boost::filesystem::exists(p) || p.empty()) return leveldb::Status::OK();

            for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
                if (boost::filesystem::is_regular_file(itr->path())) {
                    std::string current_file = itr->path().filename().string();
                    if (current_file.size() > HEAD_INDEX_LOG_NAME.size() && memcmp(current_file.c_str(), HEAD_INDEX_LOG_NAME.c_str(), HEAD_INDEX_LOG_NAME.size()) == 0) {
                        existing_logs.push_back(current_file);
                    }
                }
            }
            std::sort(existing_logs.begin(), existing_logs.end(), [&](const std::string& l, const std::string& r) {
                return std::stoi(l.substr(HEAD_INDEX_LOG_NAME.size())) < std::stoi(r.substr(HEAD_INDEX_LOG_NAME.size()));
            });

            for (size_t i = 0; i < existing_logs.size(); i++) {
                leveldb::SequentialFile* sf;
                s = env->NewSequentialFile(log_path_ + "/metric" + std::to_string(0) + "/" + existing_logs[i], &sf);
                if (!s.ok()) {
                    delete sf;
                    return s;
                }

                leveldb::log::Reader r(sf, nullptr, false, 0);
                leveldb::Slice record;
                std::string scratch;
                while (r.ReadRecord(&record, &scratch)) {
                    if (record.data()[0] == leveldb::log::kSeries) {
                        std::vector<tsdb::tsdbutil::TreeRefSeries> rs;
                        bool success = leveldb::log::valueLog(record, &rs);
                        if (!success) {
                            return leveldb::Status::Corruption("series recover_index_from_log");
                        }
                        for (auto & r : rs) {
                            auto p = get_or_create_with_id(r.sgid, r.mid, r.lset);
                            if (p.second) {
                                if (r.sgid > last_source_group_id_) last_source_group_id_ = r.sgid;
                                if (r.mid > last_metric_id_) last_metric_id_ = r.mid;
                            }
                        }
                    }
                }
                delete sf;
            }

            if (existing_logs.empty()) {
                pwal_m_.SetTagLogSeq(0);
            } else {
                pwal_m_.SetTagLogSeq(std::stoi(existing_logs.back().substr(HEAD_INDEX_LOG_NAME.size())) + 1);
            }

            leveldb::WritableFile* f;
            s = env->NewAppendableFile(log_path_ + "/metric" + std::to_string(0) + "/" + HEAD_INDEX_LOG_NAME + std::to_string(pwal_m_.GetTagLogSeq()), &f);
            if (!s.ok()) {
                delete f;
                return s;
            }
            pwal_m_.SetTagLogFile(f);
            pwal_m_.SetTagLogWriter(new leveldb::log::Writer(f));

            return leveldb::Status::OK();
        }

        leveldb::Status SpanHead::recover_samples_from_log(int seq) {
            MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16);
            leveldb::Env* env = leveldb::Env::Default();
            leveldb::Status s;

            std::vector<std::string> existing_logs;
            boost::filesystem::path p(log_path_ + "/sample" + std::to_string(seq));
            boost::filesystem::directory_iterator end_itr;

            if (!boost::filesystem::exists(p) || p.empty()) return leveldb::Status::OK();

            for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
                if (boost::filesystem::is_regular_file(itr->path())) {
                    std::string current_file = itr->path().filename().string();
                    if (current_file.size() > HEAD_SAMPLES_LOG_NAME.size() && memcmp(current_file.c_str(), HEAD_SAMPLES_LOG_NAME.c_str(), HEAD_SAMPLES_LOG_NAME.size()) == 0) {
                        existing_logs.push_back(current_file);
                    }
                }
            }
            std::sort(existing_logs.begin(), existing_logs.end(), [&](const std::string& l, const std::string& r) {
                return std::stoi(l.substr(HEAD_SAMPLES_LOG_NAME.size())) < std::stoi(r.substr(HEAD_SAMPLES_LOG_NAME.size()));
            });

            for (const auto & existing_log : existing_logs) {
                leveldb::SequentialFile* sf;
                s = env->NewSequentialFile(log_path_ + "/sample" + std::to_string(seq) + "/" + existing_log, &sf);
                if (!s.ok()) {
                    delete sf;
                    return s;
                }
                leveldb::log::Reader r(sf, nullptr, false, 0);
                leveldb::Slice record;
                std::string scratch;
                std::vector<tsdb::tsdbutil::TreeRefSample> rs;
                while (r.ReadRecord(&record, &scratch)) {
                    if (record.data()[0] == leveldb::log::kSample) {
                        rs.clear();
                        leveldb::log::treeSamples(record, &rs);

                        auto app = appender();
                        for (size_t j = 0; j < rs.size(); j++) {
                            app->add_fast(rs[j].sgid, rs[j].mid, rs[j].t, rs[j].v);
                        }
                        app->recover_commit();
                    }
                }
            }

            if (existing_logs.empty()) {
                pwal_m_.SetSampleLogSeq(seq, 0);
            } else {
                pwal_m_.SetSampleLogSeq(seq, std::stoi(existing_logs.back().substr(HEAD_SAMPLES_LOG_NAME.size())) + 1);
            }
            leveldb::WritableFile* f;
            s = env->NewAppendableFile(log_path_ + "/sample" + std::to_string(seq) + "/" + HEAD_SAMPLES_LOG_NAME + std::to_string(pwal_m_.GetSampleLogSeq(seq)), &f);
            if (!s.ok()) {
                delete f;
                return s;
            }

            pwal_m_.SetSampleLogFile(seq, f);
            pwal_m_.SetSampleLogWriter(seq, new leveldb::log::Writer(f));
            pwal_m_.SetSampleActiveLogNum(seq, existing_logs.size());

            return s;
        }

        leveldb::Status SpanHead::clean_sample_log(int seq) {
            std::vector<std::vector<tsdb::tsdbutil::TreeRefSample>> refSamples;
            leveldb::Status s;
            leveldb::Env* env = leveldb::Env::Default();
            TreeMemSeries* ms;

            int end = pwal_m_.GetSampleLogSeq(seq)-1;
            if (end == -1) end = 0;
            for (int i = 0; i <= end; i++) {
                std::string fname = log_path_ + "/sample" + std::to_string(seq) + "/" + tsdb::head::HEAD_SAMPLES_LOG_NAME + std::to_string(i);
                if (!env->FileExists(fname)) {
                    continue;
                }
                leveldb::SequentialFile* sf;
                s = env->NewSequentialFile(fname, &sf);
                if (!s.ok()) {
                    std::cout << "NewSequentialFile " << s.ToString() << std::endl;
                    return s;
                }
                leveldb::log::Reader r(sf, nullptr, false, 0);
                leveldb::Slice record;
                std::string scratch;
                while (r.ReadRecord(&record, &scratch)) {
                    if (record.data()[0] == leveldb::log::kSample) {
                        std::vector<tsdb::tsdbutil::TreeRefSample> rs;
                        std::vector<tsdb::tsdbutil::TreeRefSample> reserve;
                        leveldb::log::treeSamples(record, &rs);
                        for (auto & r : rs) {
                            ms = read_flat_forward_index(r.sgid, r.mid);
                            uint8_t idx;
                            while (ms->sid_ == std::numeric_limits<uint32_t>::max()){}
                            slab::SlabInfo* slabInfo = value_log_->GetMemSlabInfo(ms->source_id_,ms->metric_id_,ms->sid_,idx);
                            if (r.txn > slabInfo->txn_[idx]) {
                                reserve.emplace_back(r);
                            }
                        }
                        if (!reserve.empty()) {
                            refSamples.emplace_back(reserve);
                        }
                    }
                }
                delete sf;

                s = env->DeleteFile(fname);
                if (!s.ok()) {
                    return s;
                }
                if (!refSamples.empty()) {
                    leveldb::WritableFile* f;
                    s = env->NewAppendableFile(fname, &f);
                    if (!s.ok()) {
                        std::cout << "NewAppendableFile " << s.ToString() << std::endl;
                        return s;
                    }
                    leveldb::log::Writer* w = new leveldb::log::Writer(f);

                    for (const auto & refSample : refSamples) {
                        std::string record = leveldb::log::treeSamples(refSample);
                        s = w->AddRecord(record);
                        if (!s.ok()) {
                            return s;
                        }
                    }
                    refSamples.clear();
                    delete f;
                    delete w;
                } else {
                    pwal_m_.DecrSampleActiveLog(seq);
                }
            }
            return s;
        }

        leveldb::Status SpanHead::clean_samples_logs() {
            std::vector<std::thread> threads;
            threads.reserve(wal_num);
            for (int i = 0; i < wal_num; i++) {
                if (pwal_m_.ActiveSampleLogNum(i) > HEAD_SAMPLES_LOG_CLEANING_THRES) {
                    threads.emplace_back([this, i] {
                        clean_sample_log(i);
                    });
                }
            }
            for (auto& thread : threads) {
                thread.join();
            }
            return leveldb::Status::OK();
        }

        void SpanHead::bg_clean_samples_logs() {
            std::thread bg_log_cleaning([this] {
                while (bg_clean_samples_logs_.load()) {
                    bool free = false;
                    clean_samples_logs();
                    sleep(1);
                }
                printf("stop bg_clean_samples_logs \n");
            });
            bg_log_cleaning.detach();
        }

        bool SpanHead::migrate_to_flat_forward_index(uint64_t sgid, uint16_t mid) {
            if (flat_forward_index_ != nullptr && mid <= migrate_mid_ && sgid <= migrate_sgid_) {
                return true;
            }
            if (mid >= forward_index_.size()) {
                return false;
            }
            if (flat_forward_index_ == nullptr) {
                alloc_flat_forward_index(mid, sgid);
                for (uint32_t i = 1; i <= mid; i++) {
                    for (uint64_t j = 1; j <= sgid; j++) {
                        TreeMemSeries* ms = static_cast<TreeMemSeries *>(thmap_get(forward_index_[i], &(j), sizeof(uint64_t)));
                        flat_forward_index_[(i-1)*sgid + (j-1)] = reinterpret_cast<uint64_t>(ms);
                    }
                }
                migrate_mid_ = mid;
                migrate_sgid_ = sgid;

                for (uint32_t i = 1; i <= mid; i++) {
                    for (uint64_t j = 1; j <= sgid; j++) {
                        thmap_del(forward_index_[i], &(j), sizeof(uint64_t));
                    }
                }

                return true;
            } else {
                if (mid < migrate_mid_ || sgid < migrate_sgid_) {
                    return false;
                }
                uint64_t* new_flat_forward_index = static_cast<uint64_t *>(malloc(mid * sgid * sizeof(uint64_t)));
                for (uint32_t i = 1; i <= migrate_mid_; i++) {
                    for (uint64_t j = 1; j <= migrate_sgid_; j++) {
                        new_flat_forward_index[(i-1)*sgid + (j-1)] = flat_forward_index_[(i-1)*migrate_sgid_ + (j-1)];
                    }
                }

                if (sgid > migrate_sgid_) {
                    for (uint32_t i = 1; i <= migrate_mid_; i++) {
                        for (uint64_t j = migrate_sgid_+1; j <= sgid; j++) {
                            TreeMemSeries* ms = static_cast<TreeMemSeries *>(thmap_get(forward_index_[i], &(j), sizeof(uint64_t)));
                            new_flat_forward_index[(i-1)*sgid + (j-1)] = reinterpret_cast<uint64_t>(ms);
                        }
                    }
                }
                if (mid > migrate_mid_) {
                    for (uint32_t i = migrate_mid_+1; i <= mid; i++) {
                        for (uint64_t j = 1; j <= sgid; j++) {
                            TreeMemSeries* ms = static_cast<TreeMemSeries *>(thmap_get(forward_index_[i], &(j), sizeof(uint64_t)));
                            new_flat_forward_index[(i-1)*sgid + (j-1)] = reinterpret_cast<uint64_t>(ms);
                        }
                    }
                }
                auto tmp = flat_forward_index_;
                flat_forward_index_ = new_flat_forward_index;
                migrate_mid_ = mid;
                migrate_sgid_ = sgid;
                incr_flat_version();

                for (uint32_t i = 1; i <= mid; i++) {
                    for (uint64_t j = 1; j <= sgid; j++) {
                        thmap_del(forward_index_[i], &(j), sizeof(uint64_t));
                    }
                }

                free(tmp);
                return true;
            }
        }

        TreeMemSeries* SpanHead::read_flat_forward_index(uint64_t sgid, uint16_t mid) {
            AGAIN:
            uint64_t version = get_flat_version();
            TreeMemSeries* tms = nullptr;
            if (flat_forward_index_ != nullptr && mid <= migrate_mid_ && sgid <= migrate_sgid_) {
                tms = (TreeMemSeries*)(flat_forward_index_[(mid-1)*migrate_sgid_ + (sgid-1)]);
            } else {
                tms = get_from_forward_index(sgid, mid);
            }
            if (get_flat_version() != version) goto AGAIN;

            return tms;
        }

        void SpanHead::update_min_max_time(int64_t mint, int64_t maxt) {
            while (true) {
                int64_t lt = min_time.get();
                if (mint >= lt || valid_time.get() >= mint) break;
                if (min_time.cas(lt, mint)) break;
            }
            while (true) {
                int64_t ht = max_time.get();
                if (maxt <= ht) break;
                if (max_time.cas(ht, maxt)) break;
            }
        }

        std::unique_ptr<db::AppenderInterface> SpanHead::head_appender() {
            return std::unique_ptr<db::AppenderInterface>(
                    new SpanHeadAppender(const_cast<SpanHead*>(this), value_log_, db_));
        }

        std::unique_ptr<db::AppenderInterface> SpanHead::appender() {
            return head_appender();
        }

// init_time initializes a head with the first timestamp. This only needs to be
// called for a completely fresh head with an empty WAL. Returns true if the
// initialization took an effect.
        bool SpanHead::init_time(int64_t t) {
            if (!min_time.cas(std::numeric_limits<int64_t>::max(), t)) return false;
            // Ensure that max time is initialized to at least the min time we just set.
            // Concurrent appenders may already have set it to a higher value.
            max_time.cas(std::numeric_limits<int64_t>::min(), t);
            return true;
        }

        std::set<std::string> SpanHead::symbols() {
            std::set<std::string> r;
            auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                         void* value) -> bool {
                ((std::set<std::string>*)(data))
                        ->insert(std::string(reinterpret_cast<const char*>(key), key_len));
                return false;
            };

            unsigned char left[1] = {0};
            unsigned char right[1] = {255};
            mem::art_range(&symbols_, left, 1, right, 1, true, true, cb, &r);
            return r;
        }
        std::vector<std::string> SpanHead::label_names() const {
            std::vector<std::string> r;
            auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                         void* value) -> bool {
                ((std::vector<std::string>*)(data))
                        ->push_back(std::string(reinterpret_cast<const char*>(key), key_len));
                return false;
            };

            unsigned char left[1] = {0};
            unsigned char right[1] = {255};
            mem::art_range(const_cast<mem::art_tree*>(posting_list->get_tag_keys_trie()),
                           left, 1, right, 1, true, true, cb, &r);
            return r;
        }

        std::vector<std::string> SpanHead::label_values(const std::string& name) {
            std::vector<std::string> vec;
            return vec;
        }

        std::pair<std::unique_ptr<index::PostingsInterface>, bool> SpanHead::postings(const std::string& name, const std::string& value) {
            std::vector<uint64_t> pls;
            inverted_index_.get_and_read(label::Label(name, value), pls);
            if (!pls.empty()) {
                return std::make_pair(std::unique_ptr<index::PostingsInterface>(new index::VectorPostings(std::move(pls))),true);
            }
            else {
                return std::make_pair(nullptr, false);
            }
        }

        std::unique_ptr<index::PostingsInterface> SpanHead::select(const std::vector<std::shared_ptr<::tsdb::label::MatcherInterface>>& l) {
            if (l.empty()) {
                return std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings());
            } else {
                std::vector<std::unique_ptr<index::PostingsInterface>> lists;
                for (std::shared_ptr<label::MatcherInterface> m : l) {
                    if (m->name() == "__name__") continue;
                    auto pl = inverted_index_.get(label::Label(m->name(), m->value()));
                    if (pl == nullptr) {
                        return std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings());
                    }
                    pl->posting_list_.reset_cursor();
                    lists.push_back(std::unique_ptr<index::PostingsInterface>(new index::PrefixPostingsV2(pl->posting_list_)));
                }
                std::vector<uint64_t> v;
                if (!lists.empty()) {
                    mem::TreeIntersectPostingsLists p(std::move(lists));
                    while (p.next()) v.push_back(p.at());
                }

                return std::unique_ptr<index::PostingsInterface>(new index::VectorPostings(std::move(v)));
            }
        }

        bool SpanHead::series(uint64_t source_id, uint16_t metric_id, label::Labels& lset, std::string* chunk_contents){
//    TreeMemSeries* tms = (TreeMemSeries*)(thmap_get(forward_index_[metric_id],&source_id, sizeof(uint64_t)));
            auto tms = read_flat_forward_index(source_id, metric_id);
            // if (tms == nullptr || tms->sid_ == std::numeric_limits<uint32_t>::max()) return false;
            if (tms == nullptr) return false;
            lset.insert(lset.end(), tms->labels.begin(), tms->labels.end());
            chunk_contents->append(reinterpret_cast<const char*>(tms->chunk_.all_bytes()), tms->chunk_.all_size());
            return true;
        }

        auto SpanHead::DecodeChunk(uint8_t *chunk) -> std::pair<std::vector<int64_t>, std::vector<double>> {
            if (chunk == nullptr) return std::make_pair(std::vector<int64_t>(), std::vector<double>());

            std::vector<int64_t> t;
            std::vector<double> v;

            auto s = leveldb::Slice(reinterpret_cast<const char *>(chunk), slab::CHUNK_SIZE);
            leveldb::Slice tmp_value;
            uint16_t mid = leveldb::DecodeFixed16(s.data() + 1);
            uint64_t sgid = leveldb::DecodeFixed64BE(s.data() + 3);
            tmp_value = leveldb::Slice(s.data() + 11, s.size() - 11);

            tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()), tmp_value.size());
            auto iter = c.xor_iterator();
            while (iter->next()) {
                t.push_back(iter->at().first);
                v.push_back((iter->at().second));
            }
            return std::make_pair(t, v);
        }

        void SpanHead::print_forward_index() {
            for (uint64_t i = 1; i < last_metric_id_; i++) {
                for (uint64_t j = 1; j < last_source_group_id_; j++) {
                    auto tms = read_flat_forward_index(j, i);
                    if (tms == nullptr) {
//                std::cout<<"mid: "<<i<<" sgid: "<<j<<" lset: "<<"NULL"<<std::endl;
                    } else {
                        std::cout<<"mid: "<<i<<" sgid: "<<j<<" max_time: "<<tms->max_time_<<" lset: "<<label::lbs_string(tms->labels)<<std::endl;
                    }
                }
            }
        }

        std::unique_ptr<index::PostingsInterface> SpanHead::sorted_postings(std::unique_ptr<index::PostingsInterface>&& p) {
//    struct SortPair {
//    label::Labels lset;
//    uint64_t id;
//    SortPair() = default;
//    SortPair(const label::Labels& ls, uint64_t v) : lset(ls), id(v) {}
//    };
//
//    std::vector<SortPair> pairs;
//    while (p->next()) {
//    uint64_t series_pointer = indirection_manager_.read_slot(p->at());
//    if ((series_pointer >> 63) == 0)
//      pairs.emplace_back(((MemSeries*)(series_pointer))->labels, p->at());
//    else {
//      pairs.emplace_back();
//      load_mem_series(series_pointer & 0x7fffffffffffffff, nullptr,
//                      &pairs.back().lset, nullptr, nullptr);
//      pairs.back().id = p->at();
//    }
//    }
//
//    std::sort(pairs.begin(), pairs.end(),
//            [](const SortPair& l, const SortPair& r) {
//              return label::lbs_compare(l.lset, r.lset) < 0;
//            });
//
//    index::VectorPostings* vp = new index::VectorPostings(pairs.size());
//    for (auto const& s : pairs) vp->push_back(s.id);
//    return std::unique_ptr<index::PostingsInterface>(vp);
//    return nullptr;
        }

        leveldb::Status SpanHead::snapshot_index() {
            return leveldb::Status::OK();
//  pid_t child_pid = fork();
//  if (child_pid == 0) {
//    // Snapshot indirection manager.
//    // We cannot use async call here because threads are not copied.
//    auto p = posting_list->get(label::ALL_POSTINGS_KEYS.label,
//                               label::ALL_POSTINGS_KEYS.value);
//    if (p == nullptr) {
//      LOG_ERROR << "no all postings";
//      abort();
//    }
//    while (p->next()) {
//      uint64_t series_pointer = indirection_manager_.read_slot(p->at());
//      if ((series_pointer >> 63) == 0)
//        indirection_manager_.set_slot(
//            p->at(),
//            ((MemSeries*)(series_pointer))->log_pos | 0x8000000000000000);
//    }
//
//    // Snapshot index.
//    leveldb::Status st = posting_list->snapshot_index(dir_ + "/snapshot");
//    LOG_DEBUG << "InvertedIndex::snapshot_index " << st.ToString();
//
//    st = indirection_manager_.snapshot(dir_ + "/snapshot");
//    LOG_DEBUG << "IndirectionManager::snapshot " << st.ToString();
//    exit(0);
//  } else if (child_pid < 0) {
//    return leveldb::Status::IOError("snapshot_index() fork fails!");
//  } else {
//    int rstatus;
//    waitpid(child_pid, &rstatus, 0);
//    if (rstatus == 0)
//      return leveldb::Status::OK();
//    else if (rstatus == -1)
//      return leveldb::Status::IOError(
//          "snapshot_index() child terminates with error!");
//    else
//      return leveldb::Status::OK();
//  }
        }

        int SpanHead::memseries_gc(int percentage, uint64_t epoch,
                                   moodycamel::ConcurrentQueue<mem::GCRequest>* gc_queue) {
            return 0;
//  std::vector<uint64_t> pls;
//  if (!sync_api_) {
//    std::promise<bool> p;
//    std::future<bool> f = p.get_future();
//    posting_list->async_get_single(
//        &label::ALL_POSTINGS_KEYS.label, &label::ALL_POSTINGS_KEYS.value, &pls,
//        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
//    f.wait();
//  } else
//    posting_list->get(label::ALL_POSTINGS_KEYS.label,
//                      label::ALL_POSTINGS_KEYS.value, &pls);
//
//  std::vector<uint64_t> mem_ids;
//  mem_ids.reserve(pls.size());
//  for (size_t i = 0; i < pls.size(); i++) {
//    if ((indirection_manager_.read_slot(pls[i]) >> 63) == 0)
//      mem_ids.push_back(pls[i]);
//  }
//  std::sort(mem_ids.begin(), mem_ids.end(), [&](uint64_t lhs, uint64_t rhs) {
//    uint64_t lp = indirection_manager_.read_slot(lhs);
//    uint64_t rp = indirection_manager_.read_slot(rhs);
//    return ((MemSeries*)(lp))->access_epoch.load() >
//           ((MemSeries*)(rp))->access_epoch.load();
//  });
//
//  // Preflush
//  for (int i = 0; i < mem_ids.size() * percentage / 100; i++) {
//    uint64_t p = indirection_manager_.read_slot(mem_ids[i]);
//    MemSeries* ms = (MemSeries*)(p);
//    mem::write_lock_or_restart(ms);
//    ms->lock();
//    // We need to clean the data samples.
//    if (ms->num_samples_ > 0) ms->_flush(db_, ms->flushed_txn_.load());
//    ms->unlock();
//    mem::write_unlock(ms);
//  }
//
//  // Wait for imm being compacted.
//  reinterpret_cast<leveldb::DBImpl*>(db_)->TEST_CompactMemTable();
//
//  // Update indirection manager.
//
//  std::string buf;
//  std::set<int> need_flush;
//  for (int i = 0; i < mem_ids.size() * percentage / 100; i++) {
//    uint64_t p = indirection_manager_.read_slot(mem_ids[i]);
//    MemSeries* ms = (MemSeries*)(p);
//
//    // if (ms->labels.empty()) {
//    //   leveldb::Status st =
//    //       load_mem_series(ms->log_pos, nullptr, &(ms->labels), nullptr,
//    //       nullptr);
//    //   if (!st.ok()) {
//    //     LOG_ERROR << st.ToString();
//    //     abort();
//    //   }
//    // }
//
//    // Update log clean txn.
//    uint64_t idx1 = ms->log_pos >> 32;
//    uint64_t idx2 = ms->log_pos & 0xffffffff;
//    buf.clear();
//    tags_log_lock_.lock();
//    ::leveldb::log::series_without_labels(ms->ref, ms->flushed_txn_.load(),
//                                          ms->log_clean_txn_, &buf);
//    if (idx1 == tags_log_writers_.size() - 1) tags_log_file_->Flush();
//    tags_log_writers_[idx1]->AddRecord(idx2, buf, false);
//    need_flush.insert(idx1);
//    tags_log_lock_.unlock();
//
//    ++_ms_gc_counter;
//
//    indirection_manager_.set_slot(mem_ids[i], 0x8000000000000000 | ms->log_pos);
//
//    if (gc_queue)
//      gc_queue->enqueue(mem::GCRequest(epoch, (void*)(ms), mem::MEMSERIES));
//  }
//
//  for (int i : need_flush) tags_log_writers_[i]->flush();
//
//  return mem_ids.size() * percentage / 100;
        }

        int SpanHead::memseries_gc_preflush(int percentage) {
//  std::vector<uint64_t> pls;
//  if (!sync_api_) {
//    std::promise<bool> p;
//    std::future<bool> f = p.get_future();
//    posting_list->async_get_single(
//        &label::ALL_POSTINGS_KEYS.label, &label::ALL_POSTINGS_KEYS.value, &pls,
//        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
//    f.wait();
//  } else
//    posting_list->get(label::ALL_POSTINGS_KEYS.label,
//                      label::ALL_POSTINGS_KEYS.value, &pls);
//
//  std::vector<uint64_t> mem_ids;
//  mem_ids.reserve(pls.size());
//  for (size_t i = 0; i < pls.size(); i++) {
//    if ((indirection_manager_.read_slot(pls[i]) >> 63) == 0)
//      mem_ids.push_back(pls[i]);
//  }
//  std::sort(mem_ids.begin(), mem_ids.end(), [&](uint64_t lhs, uint64_t rhs) {
//    uint64_t lp = indirection_manager_.read_slot(lhs);
//    uint64_t rp = indirection_manager_.read_slot(rhs);
//    return ((MemSeries*)(lp))->access_epoch.load() >
//           ((MemSeries*)(rp))->access_epoch.load();
//  });
//
//  for (int i = 0; i < mem_ids.size() * percentage / 100; i++) {
//    uint64_t p = indirection_manager_.read_slot(mem_ids[i]);
//    MemSeries* ms = (MemSeries*)(p);
//    mem::write_lock_or_restart(ms);
//    ms->lock();
//    // We need to clean the data samples.
//    if (ms->num_samples_ > 0) ms->_flush(db_, ms->flushed_txn_.load());
//    ms->unlock();
//    mem::write_unlock(ms);
//  }

//  return mem_ids.size() * percentage / 100;
        }

/******************** Concurrency ********************/

        void SpanHead::enable_migration() {
//  migration_enabled_ = true;
//  migration_running_ =
//      (mem::AlignedBool*)aligned_alloc(64, sizeof(mem::AlignedBool));
//  migration_running_->reset();
//
//  migration_running_->set(true);
//  std::thread migration(_head_mem_to_disk_migration, this);
//  migration.detach();
        }

        void SpanHead::disable_migration() { migration_running_->set(false); }

        void SpanHead::full_migrate() {
            uint64_t current_epoch = global_epoch_->load();
            posting_list->full_migrate(current_epoch, &gc_requests_);
            memseries_gc(100, current_epoch, &gc_requests_);
            posting_list->art_gc(100, current_epoch, nullptr, &gc_requests_);
        }

        void _head_mem_to_disk_migration(SpanHead* h) {
            uint64_t current_epoch;
            double vm, rss;
            h->wg_.add(1);
            while (h->migration_running_->get()) {
                mem_usage(vm, rss);
                if (rss * 1024 > h->mem_to_disk_migration_threshold_.load()) {
                    current_epoch = h->global_epoch_->load();
                    h->posting_list->try_migrate(current_epoch, &h->gc_requests_);
                    // h->memseries_gc_preflush(10);
                    // // Wait till no imm.
                    // while (true) {
                    //   leveldb::MutexLock l(h->db_->mutex());
                    //   if (h->db_->imms()->empty())
                    //     break;
                    //   sleep(1);
                    // }
                    h->memseries_gc(10, current_epoch, &h->gc_requests_);

                    h->posting_list->art_gc(10, current_epoch, nullptr, &h->gc_requests_);
                }

                usleep(GLOBAL_EPOCH_INCR_INTERVAL * 20);
            }
            printf("_head_mem_to_disk_migration exits\n");
            h->wg_.done();
        }

/******************** Concurrency ********************/
        void SpanHead::enable_concurrency() {
            concurrency_enabled_ = true;
            for (int i = 0; i < MAX_WORKER_NUM; i++) {
                local_epochs_[i] = (mem::Epoch*)aligned_alloc(64, sizeof(mem::Epoch));
                local_epochs_[i]->reset();
                spinlocks_[i] = (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock));
                spinlocks_[i]->reset();
                running_[i] =
                        (mem::AlignedBool*)aligned_alloc(64, sizeof(mem::AlignedBool));
                running_[i]->reset();
            }
            global_epoch_ = (mem::Epoch*)aligned_alloc(64, sizeof(mem::Epoch));
            global_epoch_->reset();
            // gc_running_ = (mem::AlignedBool*)aligned_alloc(64,
            // sizeof(mem::AlignedBool)); gc_running_->reset();
            global_running_ =
                    (mem::AlignedBool*)aligned_alloc(64, sizeof(mem::AlignedBool));
            global_running_->reset();

            global_running_->set(true);
//  std::thread global_gc(_head_global_garbage_collection, this);
//  global_gc.detach();
        }

        leveldb::Status SpanHead::register_thread(int* idx) {
            for (int i = 0; i < MAX_WORKER_NUM; i++) {
                if (running_[i]->cas(false, true)) {
                    *idx = i;
                    return leveldb::Status::OK();
                }
            }
            return leveldb::Status::NotFound("no available slot");
        }

        void SpanHead::deregister_thread(int idx) {
            running_[idx]->set(false);
            local_epochs_[idx]->store(0x8000000000000000);
        }

        void SpanHead::update_local_epoch(int idx) {
            local_epochs_[idx]->store(global_epoch_->load());
        }

        void _head_global_garbage_collection(SpanHead* h) {
            h->wg_.add(1);

            mem::GCRequest garbage;
            uint64_t min_local_epoch;
            while (h->global_running_->get()) {
                if (h->global_epoch_->load() == 0xFFFFFFFFFFFFFFFFul) {
                    // Need to wrap around.
                    h->global_epoch_->store(0);
                    bool ready = false;
                    while (!ready) {
                        ready = true;
                        for (int i = 0; i < MAX_WORKER_NUM; i++) {
                            if (h->running_[i]->get() && h->local_epochs_[i]->load() != 0) {
                                ready = false;
                                break;
                            }
                        }
                        usleep(GLOBAL_EPOCH_INCR_INTERVAL);
                    }

                    // Lock all running threads.
                    for (int i = 0; i < MAX_WORKER_NUM; i++) {
                        if (h->running_[i]->get()) h->spinlocks_[i]->lock();
                    }

                    // Clean the garbage from the last term.
                    void* g = nullptr;
                    while (h->gc_requests_.try_dequeue(garbage)) {
                        if (g == garbage.garbage_) {
                            h->gc_requests_.enqueue(garbage);
                            break;
                        }
                        if (garbage.epoch_ != 0) {
                            delete garbage.garbage_;
                            ++_garbage_counter;
                        } else {
                            if (g == nullptr) g = garbage.garbage_;
                            h->gc_requests_.enqueue(garbage);
                        }
                    }

                    for (int i = 0; i < MAX_WORKER_NUM; i++) {
                        if (h->running_[i]->get()) h->spinlocks_[i]->unlock();
                    }
                } else {
                    min_local_epoch = h->global_epoch_->load();
                    h->global_epoch_->increment();

                    for (int i = 0; i < MAX_WORKER_NUM; i++) {
                        if (h->running_[i]->get() &&
                            h->local_epochs_[i]->load() < min_local_epoch)
                            min_local_epoch = h->local_epochs_[i]->load();
                    }

                    // LOG_DEBUG << "min_local_epoch:" << min_local_epoch << " global_epoch_:"
                    // << h->global_epoch_->load();

                    void* g = nullptr;
                    while (h->gc_requests_.try_dequeue(garbage)) {
                        if (g == garbage.garbage_) {
                            h->gc_requests_.enqueue(garbage);
                            break;
                        }
                        // LOG_DEBUG << "type:" << garbage.type_ << " epoch:" << garbage.epoch_
                        // << " min_local_epoch:" << min_local_epoch << " ptr:" <<
                        // (uint64_t)((uintptr_t)(garbage.garbage_));
                        if (garbage.epoch_ < min_local_epoch) {
                            delete garbage.garbage_;  // TODO: garbage handling.
                            ++_garbage_counter;
                        } else {
                            if (g == nullptr) g = garbage.garbage_;
                            h->gc_requests_.enqueue(garbage);
                        }
                    }
                }
                usleep(GLOBAL_EPOCH_INCR_INTERVAL);
            }
            h->wg_.done();
        }

        leveldb::Status SpanHead::flush_db() {
            std::vector<std::pair<slab::SlabInfo*, slab::Slab*>> migrate_array;
            bool is_seq = false;
            auto status = value_log_->MigrateReadLazyFlush(migrate_array, is_seq);
            if(!status){
                return leveldb::Status::OK();
            }
            leveldb::Status s;
            for(auto &it:migrate_array){
                for(uint8_t i=0;i<it.first->idx_;i++){
                    key_.clear();
                    uint64_t sgid = value_log_->GetSinfoSourceID(it.first,i);
                    uint16_t mid = value_log_->GetSinfoMetricID(it.first,i);
                    slab::encode_l1key(&key_, sgid, mid,it.first->end_time_[i]);
                    uint8_t sub_idx = 0;
                    for(uint8_t j=0;j<it.first->nalloc_;j++){
                        uint64_t temp_sgid;
                        uint16_t temp_mid;
                        auto item = (slab::Item*)(it.second+j*slab::SLAB_ITEM_SIZE);
                        DecodeItemChunkHeader(item,temp_mid,temp_sgid);
                        if(temp_sgid == sgid && temp_mid == mid){
                            memcpy(slab_buffer_+sub_idx*slab::SLAB_ITEM_SIZE,item,slab::SLAB_ITEM_SIZE);
                            sub_idx++;
                        }
                    }
                    if(sub_idx == 0){
                        continue;
                    }
                    s = db_->Put(leveldb::WriteOptions(), key_,
                                 leveldb::Slice(reinterpret_cast<char*>(slab_buffer_),
                                                slab::SLAB_ITEM_SIZE*sub_idx));
                    auto tms = read_flat_forward_index(sgid,mid);
                    if (tms == nullptr) {
                        tms = read_flat_forward_index(sgid,mid);
                    }
                    tms->level_flush_time_ = it.first->end_time_[i];
                }
                if (!is_seq) {
                    value_log_->FreeBufSlab(it.second);
                }
            }
            return s;
        }

        uint64_t SpanHead::get_or_create_source_id(const label::Labels& lset, bool& is_exist,uint64_t epoch) {
            if(lset.size()==0){
                return 0;
            }
            std::string source_group_str = label::label_source_group(lset);
            void* source_group = thmap_get(source_thmap_, source_group_str.c_str(), source_group_str.size());
            if (source_group == NULL) {
                auto hash1 = label::lbs_source_hash(lset);
                source_locks_[hash1%source_lock_num_].lock();
                source_group = thmap_get(source_thmap_, source_group_str.c_str(), source_group_str.size());
                if(source_group){
                    source_locks_[hash1%source_lock_num_].unlock();
                    return *(uint64_t *)source_group;
                }
                is_exist = false;
                uint64_t sgid = last_source_group_id_.fetch_add(1) + 1;
                auto source_group_id = new uint64_t(sgid);
                thmap_put(source_thmap_, source_group_str.c_str(), source_group_str.size(), source_group_id);
                source_locks_[hash1%source_lock_num_].unlock();
                return sgid;
            }
            return *(uint64_t *)source_group;
        }

        uint16_t SpanHead::get_or_create_metric_id(const label::Labels& lset, bool& is_exist, uint64_t epoch) {
            if(lset.size()==0){
                LOG_DEBUG<<"lset size = 0\n";
                return 0;
            }
            std::string metric_str = label::label_metric(lset);
            void* metric = thmap_get(metric_thmap_, metric_str.c_str(), metric_str.size());
            if (metric == NULL) {
                auto hash1 = label::lbs_metric_hash(lset);
                metric_locks_[hash1%metric_lock_num_].lock();
                metric = thmap_get(metric_thmap_, metric_str.c_str(), metric_str.size());
                if(metric){
                    metric_locks_[hash1%metric_lock_num_].unlock();
                    return *(uint16_t *)metric;
                }
                is_exist = false;
                uint16_t mid = last_metric_id_.fetch_add(1) + 1;
                auto metric_id = new uint16_t(mid);
                thmap_put(metric_thmap_, metric_str.c_str(), metric_str.size(), metric_id);
                metric_locks_[hash1%metric_lock_num_].unlock();
                return mid;
            }
            return *(uint16_t *)metric;
        }

        std::pair<TreeMemSeries*, bool> SpanHead::get_or_create_by_thmap(const label::Labels& lset, uint64_t epoch) {
            bool is_exist = true;
            uint64_t sgid = get_or_create_source_id(lset,is_exist,epoch);
            uint16_t mid = get_or_create_metric_id(lset,is_exist,epoch);

            if(!is_exist){
                inverted_index_.add(sgid,lset);
            }

            auto series_ptr = read_flat_forward_index(sgid, mid);
            if (series_ptr != nullptr) {
                return std::make_pair((TreeMemSeries*)series_ptr, false);
            }
            uint32_t slab_id;
//  while(!value_log_->GetMemSlabID(slab_id,sgid,mid)){
//  }
            slab_id = std::numeric_limits<uint32_t>::max();
            TreeMemSeries* ms = new TreeMemSeries(sgid,mid,lset,slab_id);
            set_to_forward_index(sgid,mid,ms);
            return std::make_pair(ms, true);

        }

        std::pair<TreeMemSeries*, bool> SpanHead::get_or_create_with_id(uint64_t sgid, uint16_t mid, const label::Labels & lset) {
            auto series_ptr = read_flat_forward_index(sgid, mid);
            if (series_ptr != nullptr) {
                return std::make_pair((TreeMemSeries*)series_ptr, false);
            }
            uint32_t slab_id;
//    while(!value_log_->GetMemSlabID(slab_id,sgid,mid)){
//    }
            slab_id = std::numeric_limits<uint32_t>::max();
            TreeMemSeries* ms = new TreeMemSeries(sgid, mid, lset, slab_id);
            ms->flushed_txn_ = std::numeric_limits<int64_t>::max();
            set_to_forward_index(sgid, mid, ms);
            inverted_index_.add(sgid,lset);
            return std::make_pair(ms, true);
        }

        TreeMemSeries* SpanHead::get_from_forward_index(uint64_t source_id, uint16_t metric_id) {
            if (metric_id >= forward_index_.size() || forward_index_[metric_id] == nullptr) {
                return nullptr;
            }
            void* ms = thmap_get(forward_index_[metric_id], &source_id, sizeof(uint64_t));
            if (ms == NULL) {
                return nullptr;
            }
            return (TreeMemSeries*)ms;
        }

        void SpanHead::set_to_forward_index(uint64_t source_id, uint16_t metric_id, tsdb::head::TreeMemSeries* ms) {
            if (metric_id >= forward_index_.size()) {
                for (uint32_t i = forward_index_.size(); i <= metric_id; i++) {
                    forward_index_.emplace_back(thmap_create(0, nullptr, 0));
                }
            }
            if (forward_index_[metric_id] == nullptr) {
                forward_index_[metric_id] = thmap_create(0, nullptr, 0);
            }
            thmap_put(forward_index_[metric_id], &source_id, sizeof(uint64_t), ms);
            return;
        }

    }

}