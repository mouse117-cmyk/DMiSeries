#include "parallel_wal.h"
#include "head/Head.hpp"
#include "head/TreeHead.h"
#include <memory>
#include <boost/filesystem/path.hpp>
#include <utility>


namespace tsdb::parallel_wal {
    PWalManagement::Wal::Wal(tsdb::head::TreeHead* head, const std::string& dir, int seq, bool tag_log) :
    head_(head),
    dir_(dir),
    seq_(seq),
    cur_log_seq_(0),
    tag_log_(tag_log),
    running_(true),
    should_stop_(false),
    cur_item_num_(0),
//    sample_queue_(),
//    series_queue_(),
    bg_writing_(false),
    write_t_(&Wal::WriteToDiskBG, this)
    {
        boost::filesystem::path p(dir_);

//        if (boost::filesystem::exists(p) && !p.empty()) return;
//        if (boost::filesystem::exists(p) && p.empty()) boost::filesystem::remove_all(dir_);

        leveldb::Env* env = leveldb::Env::Default();
        if (tag_log) {
            boost::filesystem::path tp(dir_ + "/metric" + std::to_string(seq));
            if (boost::filesystem::exists(tp) && !p.empty()) return;

            leveldb::WritableFile* f;
            leveldb::Status s;
            env->CreateDir(dir);
            auto status = env->CreateDir(dir_ + "/metric" + std::to_string(seq));
            if(!status.ok()){
                std::cout<<status.ToString()<<"=========================================\n";
            }
            s = env->NewAppendableFile(dir_ + "/metric" + std::to_string(seq) + "/" + tsdb::head::HEAD_INDEX_LOG_NAME + std::to_string(cur_log_seq_), &f);
            if (!s.ok()) {
                delete f;
                std::cout<<s.ToString()<<std::endl;
            }
            log_file_ = f;
            log_writer_ = new leveldb::log::Writer(f);
        } else {
            boost::filesystem::path tp(dir_ + "/sample" + std::to_string(seq));
            if (boost::filesystem::exists(tp) && !p.empty()) return;

            leveldb::WritableFile* f;
            leveldb::Status s;
            env->CreateDir(dir_);
            auto status = env->CreateDir(dir_ + "/sample" + std::to_string(seq));
            if(!status.ok()){
                std::cout<<status.ToString()<<"=========================================\n";
            }
            s = env->NewAppendableFile(dir_ + "/sample" + std::to_string(seq) + "/" + tsdb::head::HEAD_SAMPLES_LOG_NAME + std::to_string(cur_log_seq_), &f);
            if (!s.ok()) {
                delete f;
                std::cout<<s.ToString()<<std::endl;
            }
            log_file_ = f;
            log_writer_ = new leveldb::log::Writer(f);
        }
    }

    PWalManagement::Wal::~Wal() {
        should_stop_ = true;
        {
            std::unique_lock<std::mutex> lock(mu_);
            bg_writing_ = true;
        }
        cv_.notify_all();
        if (write_t_.joinable()) {
            write_t_.join();
        }
        Flush();
        running_ = false;
        rec_buf_.clear();
    }

    void PWalManagement::Wal::StartBGWork() {
        write_t_.detach();
    }

    void PWalManagement::Wal::AppendSample(tsdbutil::TreeRefSample sample) {
//        sample_queue_.enqueue(sample);
        sample_queue_.push(sample);
//        if (!bg_writing_ && sample_queue_.size_approx() >> 8 >= 1) {
//            {
//                std::unique_lock<std::mutex> lock(mu_);
//                bg_writing_ = true;
//            }
//            cv_.notify_one();
//        }
    }

    void PWalManagement::Wal::AppendSeries(tsdbutil::TreeRefSeries series) {
//        series_queue_.enqueue(series);
        series_queue_.push(series);
//        if (!bg_writing_ && series_queue_.size_approx() >> 8 >= 1) {
//            {
//                std::unique_lock<std::mutex> lock(mu_);
//                bg_writing_ = true;
//            }
//            cv_.notify_one();
//        }
    }

    void PWalManagement::Wal::TryExtendLog() {
        if (log_writer_ && log_writer_->write_off() > MAX_LOG_SIZE) {
            cur_log_seq_++;
            leveldb::WritableFile* f;
            leveldb::Env* env = leveldb::Env::Default();
            if (tag_log_) {
                env->NewAppendableFile(dir_ + "/metric" + std::to_string(seq_) + "/" + tsdb::head::HEAD_INDEX_LOG_NAME + std::to_string(cur_log_seq_), &f);
            } else {
                env->NewAppendableFile(dir_ + "/sample" + std::to_string(seq_) + "/" + tsdb::head::HEAD_SAMPLES_LOG_NAME + std::to_string(cur_log_seq_), &f);
                active_samples_logs_++;
            }
            log_file_ = f;
            log_writer_ = new leveldb::log::Writer(f);
        }
    }

    void PWalManagement::Wal::WriteToDiskBG() {
        MasstreeWrapper<slab::SlabInfo>::ti=threadinfo::make(threadinfo::TI_PROCESS, 1000);
        while (!ShouldStop()) {
            sleep(0.5);
            while (!ShouldStop() && (sample_queue_.was_size() > 0 || series_queue_.was_size() > 0)) {
    //            std::unique_lock<std::mutex> lock(mu_);
    //            cv_.wait(lock, [this]{return bg_writing_;});
    //            sleep(0.5);
    //            std::cout<<"start writing"<<std::endl;

                if (tag_log_) {
//                    auto cnt = series_queue_.try_dequeue_bulk(series_, ITEM_NUM);
//                    if (cnt == 0) {
//                        continue;
//                    }
//                    rec_buf_ = leveldb::log::treeSeries(series_,cnt);
//                    log_writer_->AddRecord(rec_buf_);
//                    TryExtendLog();

//                    while (series_queue_.try_pop(series_[cur_item_num_])) {
//                        cur_item_num_++;
//                        if (cur_item_num_ == ITEM_NUM) {
//                            rec_buf_ = leveldb::log::treeSeries(series_,ITEM_NUM);
//                            log_writer_->AddRecord(rec_buf_);
//                            TryExtendLog();
//                            cur_item_num_ = 0;
//                            break;
//                        }
//                    }

                    while (series_queue_.try_pop(series_[cur_item_num_])) {
                        cur_item_num_++;
                        if (cur_item_num_ >= ITEM_NUM || series_queue_.was_size() == 0) {
                            rec_buf_ = leveldb::log::treeSeries(series_, cur_item_num_);
                            log_writer_->AddRecord(rec_buf_);
                            TryExtendLog();
                            cur_item_num_ = 0;
                        }
                    }
                } else {
//                    auto cnt = sample_queue_.try_dequeue_bulk(samples_, ITEM_NUM);
//                    if (cnt == 0) {
//                        continue;
//                    }
//                    rec_buf_ = leveldb::log::treeSamples(samples_, cnt);
//                    log_writer_->AddRecord(rec_buf_);
//
//                    for (int i = 0; i < cnt; i++) {
//                        auto ms = head_->get_from_forward_index(samples_[i].sgid, samples_[i].mid);
//                        samples_[i].txn = ++ms->flushed_txn_;
//                    }
//
//                    TryExtendLog();

//                    while (sample_queue_.try_pop(samples_[cur_item_num_])) {
//                        cur_item_num_++;
//                        if (cur_item_num_ == ITEM_NUM) {
//                            rec_buf_ = leveldb::log::treeSamples(samples_, ITEM_NUM);
//                            log_writer_->AddRecord(rec_buf_);
//                            TryExtendLog();
//                            cur_item_num_ = 0;
//                            break;
//                        }
//                    }

                    while (sample_queue_.try_pop(samples_[cur_item_num_])) {
                        cur_item_num_++;
                        if (cur_item_num_ >= ITEM_NUM || sample_queue_.was_size() == 0) {
                            rec_buf_ = leveldb::log::treeSamples(samples_, cur_item_num_);
                            log_writer_->AddRecord(rec_buf_);
                            TryExtendLog();
                            for (int i = 0; i < cur_item_num_; i++) {
//                                auto ms = head_->get_from_forward_index(samples_[i].sgid, samples_[i].mid);
                                auto ms = head_->read_flat_forward_index(samples_[i].sgid, samples_[i].mid);
                                if(ms == nullptr){
                                    ms = head_->read_flat_forward_index(samples_[i].sgid, samples_[i].mid);
                                    if (ms == nullptr) {
                                      LOG_DEBUG<<"ms == null";
                                      continue;
                                    }
                                }
                                samples_[i].txn = ++ms->flushed_txn_;
                            }
                            cur_item_num_ = 0;
                        }
                    }

                }
                bg_writing_ = false;
            }
        }

    }
    
    void PWalManagement::Wal::Flush() {
        if (tag_log_) {
//            auto cnt = series_queue_.try_dequeue_bulk(series_, ITEM_NUM);
//            if (cnt == 0) {
//                return;
//            }
//            rec_buf_ = leveldb::log::treeSeries(series_,cnt);
//            log_writer_->AddRecord(rec_buf_);
//            TryExtendLog();

            while (series_queue_.try_pop(series_[cur_item_num_])) {
                cur_item_num_++;
                if (cur_item_num_ >= ITEM_NUM || series_queue_.was_size() == 0) {
                    rec_buf_ = leveldb::log::treeSeries(series_, cur_item_num_);
                    log_writer_->AddRecord(rec_buf_);
                    TryExtendLog();
                    cur_item_num_ = 0;
                }
            }
        } else {
//            auto cnt = sample_queue_.try_dequeue_bulk(samples_, ITEM_NUM);
//            if (cnt == 0) {
//                return;
//            }
//            rec_buf_ = leveldb::log::treeSamples(samples_, cnt);
//            log_writer_->AddRecord(rec_buf_);
//            TryExtendLog();

            while (sample_queue_.try_pop(samples_[cur_item_num_])) {
                cur_item_num_++;
                if (cur_item_num_ >= ITEM_NUM || sample_queue_.was_size() == 0) {
                    rec_buf_ = leveldb::log::treeSamples(samples_, cur_item_num_);
                    log_writer_->AddRecord(rec_buf_);
                    TryExtendLog();
                    cur_item_num_ = 0;
                }
            }
        }

        if (tag_log_) {
            if (cur_item_num_ > 0) {
               rec_buf_ = leveldb::log::treeSeries(series_, cur_item_num_);
               log_writer_->AddRecord(rec_buf_);
               TryExtendLog();
               cur_item_num_ = 0;
            }
        } else {
            if (cur_item_num_ > 0) {
                rec_buf_ = leveldb::log::treeSamples(samples_, cur_item_num_);
                log_writer_->AddRecord(rec_buf_);
                TryExtendLog();
                cur_item_num_ = 0;
            }
        }
        if (log_writer_->write_off() > 0) {
            log_writer_->flush();
        }
    }

    PWalManagement::PWalManagement(tsdb::head::TreeHead* head, const int &wal_num, const std::string &dir) : head_(head), wal_sz_(wal_num) {
        sample_wal_.reserve(wal_num);
        for (int i = 0; i < wal_num; i++) {
            sample_wal_.emplace_back(new Wal(head, dir, i, false));
        }
        tag_wal_ = new Wal(head, dir, 0, true);

        for (int i = 0; i < wal_num; i++) {
            sample_wal_[i]->StartBGWork();
        }
        tag_wal_->StartBGWork();
    }

    void PWalManagement::AppendSample(tsdbutil::TreeRefSample sample) {
        sample_wal_[sample.mid % wal_sz_]->AppendSample(sample);
    }

    void PWalManagement::AppendSeries(tsdbutil::TreeRefSeries series) {
        tag_wal_->AppendSeries(series);
    }

    void PWalManagement::FlushTagLog() {
        tag_wal_->Flush();
    }

    void PWalManagement::FlushSampleLog(int seq) {
        sample_wal_[seq]->Flush();
    }

    void PWalManagement::FlushAll() {
        tag_wal_->Flush();
        for (int i = 0; i < wal_sz_; i++) {
            sample_wal_[i]->Flush();
        }
    }
}



