#ifndef PARALLEL_WAL_H
#define PARALLEL_WAL_H

#include "third_party/atomic_queue/include/atomic_queue/atomic_queue.h"
#include <leveldb/include/leveldb/env.h>
#include "leveldb/port/port_stdcxx.h"
#include "leveldb/util/mutexlock.h"
#include "tsdbutil/tsdbutils.hpp"
#include "leveldb/db/log_writer.h"
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
#include <semaphore.h>
#include "base/Logging.hpp"

namespace tsdb::head {
class TreeHead;
}

#define WriteDiskBound (4 * 1024)
#define ITEM_NUM (1 << 8) // 256
#define MAX_LOG_SIZE (64 * 1024 * 1024)
#define QUEUE_SIZE (1 << 10)
namespace tsdb {
    namespace parallel_wal {

        class PWalManagement {

            class Wal {
            public:
                Wal(tsdb::head::TreeHead* head, const std::string& dir = "", int seq = 0, bool tag_log = false);
                ~Wal();

                inline bool ShouldStop() {return should_stop_;}
                inline bool IsRunning() {return running_;}
                inline int GetActiveSampleLogNum() {return active_samples_logs_;}
                inline void DecrActiveSampleLogNum() {active_samples_logs_--;}
                inline void SetActiveSampleLogNum(int num) {active_samples_logs_ = num;}
                inline int CurLogSeq() {return cur_log_seq_;}
                inline void SetLogSeq(int seq) {cur_log_seq_ = seq;}
                inline void SetLogFile(leveldb::WritableFile* f) {log_file_ = f;}
                inline void SetLogWriter(leveldb::log::Writer* w) {log_writer_ = w;}

                void AppendSample(tsdb::tsdbutil::TreeRefSample sample);
                void AppendSeries(tsdb::tsdbutil::TreeRefSeries series);

                void TryExtendLog();
                void StartBGWork();
                void WriteToDiskBG();
                void Flush();

            private:
                std::string dir_;
                int seq_;
                bool tag_log_ = false;

                tsdb::head::TreeHead* head_;

                std::atomic<int> cur_log_seq_;
                leveldb::WritableFile* log_file_;
                leveldb::log::Writer* log_writer_;
                int active_samples_logs_;

                std::string rec_buf_;

                std::atomic<bool> running_;
                std::atomic<bool> should_stop_;


                uint32_t cur_item_num_;
                tsdb::tsdbutil::TreeRefSample samples_[ITEM_NUM];
                tsdb::tsdbutil::TreeRefSeries series_[ITEM_NUM];
                atomic_queue::AtomicQueueB2<tsdb::tsdbutil::TreeRefSample> sample_queue_{QUEUE_SIZE};
                atomic_queue::AtomicQueueB2<tsdb::tsdbutil::TreeRefSeries> series_queue_{QUEUE_SIZE};

                std::thread write_t_;

                bool bg_writing_;
                std::mutex mu_;
                std::condition_variable cv_;
            };

        public:
            PWalManagement(tsdb::head::TreeHead* head, const int &wal_num, const std::string &dir);

            inline int GetTagLogSeq() {return tag_wal_->CurLogSeq();}
            inline void SetTagLogSeq(int idx) {tag_wal_->SetLogSeq(idx);}
            inline int GetSampleLogSeq(int seq) {return sample_wal_[seq]->CurLogSeq();}
            inline void SetSampleLogSeq(int seq, int idx) {sample_wal_[seq]->SetLogSeq(idx);}
            inline int GetSampleActiveLogNum(int seq) { return sample_wal_[seq]->GetActiveSampleLogNum();}
            inline void SetSampleActiveLogNum(int seq, int num) {sample_wal_[seq]->SetActiveSampleLogNum(num);}
            inline void DecrSampleActiveLog(int seq) {sample_wal_[seq]->DecrActiveSampleLogNum();}
            inline int ActiveSampleLogNum(int seq) { return sample_wal_[seq]->GetActiveSampleLogNum();}
            inline void SetTagLogFile(leveldb::WritableFile* f) {tag_wal_->SetLogFile(f);}
            inline void SetTagLogWriter(leveldb::log::Writer* w) {tag_wal_->SetLogWriter(w);}
            inline void SetSampleLogFile(int seq, leveldb::WritableFile* f) {sample_wal_[seq]->SetLogFile(f);}
            inline void SetSampleLogWriter(int seq, leveldb::log::Writer* w) {sample_wal_[seq]->SetLogWriter(w);}

            void AppendSample(tsdb::tsdbutil::TreeRefSample sample);
            void AppendSeries(tsdb::tsdbutil::TreeRefSeries series);

            void FlushTagLog();
            void FlushSampleLog(int seq);
            void FlushAll();

            void Stop() {
                for (int i = 0; i < sample_wal_.size(); i++) {
                    sample_wal_[i]->ShouldStop();
                }
                tag_wal_->ShouldStop();
            }

        private:
            tsdb::head::TreeHead* head_;
            Wal* tag_wal_;
            std::vector<Wal*> sample_wal_;
            const int wal_sz_;
        };
    }
}

#endif
