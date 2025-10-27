#pragma once

#include <iostream>
#include <vector>

#include "TreeHead.h"
#include "base/Logging.hpp"
#include "base/TimeStamp.hpp"
#include "db/AppenderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadUtils.hpp"
#include "leveldb/db/log_format.h"
#include "leveldb/db/log_writer.h"
#include "mem/go_art.h"
#include "parallel_wal/free_lock_wal.h"
#include "tsdbutil/RecordEncoder.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "TreeMemSeries.h"
#include "TreeSeries/TreeSeries.h"

namespace tsdb {
    namespace head {

        class TreeHeadAppender : public db::AppenderInterface {
        private:
            TreeHead* ts_head_;
            slab::TreeSeries* tree_series_;
            leveldb::DB* db_;
            int64_t min_time_;
            int64_t max_time_;
            int thread_id_;

            std::vector<tsdbutil::TreeRefSeries> series;
            std::vector<tsdbutil::TreeRefSample> samples;
            std::string rec_buf;

        public:
            TreeHeadAppender(TreeHead* head, slab::TreeSeries *tree_series, leveldb::DB* db) : ts_head_(head),
                                                                                               tree_series_(tree_series), db_(db) {}

            //mock
            virtual std::pair<uint64_t, leveldb::Status> add(const label::Labels& lset,
                                                             int64_t t, double v,
                                                             uint64_t epoch = 0) override{
                uint64_t sgid = 0;
                uint16_t mid = 0;
                add(lset, t, v, sgid, mid, 0);
                return std::make_pair(0,leveldb::Status::OK());
//                return std::make_pair(0,leveldb::Status::IOError());
            }
            //mock
            virtual std::pair<uint64_t, leveldb::Status> add(label::Labels&& lset,
                                                             int64_t t, double v,
                                                             uint64_t epoch = 0) override{
                uint64_t sgid = 0;
                uint16_t mid = 0;
                add(lset, t, v, sgid, mid, 0);
                return std::make_pair(0,leveldb::Status::OK());
//                return std::make_pair(0,leveldb::Status::IOError());
            }
            virtual leveldb::Status add(const label::Labels& lset, int64_t t,
                                                             double v, uint64_t &sgid,uint16_t &mid, uint64_t epoch) override{
                if(lset.size()==0){
                    LOG_DEBUG<<"lset size = 0\n";
                }
                std::pair<TreeMemSeries*, bool> s = ts_head_->get_or_create_by_thmap(lset, epoch);
                sgid = s.first->source_id_;
                mid = s.first->metric_id_;
                if (s.second) series.emplace_back(s.first->source_id_,s.first->metric_id_, lset, s.first);

                return add_fast(sgid,mid, t, v);
            }
            virtual leveldb::Status add(label::Labels&& lset, int64_t t,
                                        double v, uint64_t &sgid,uint16_t &mid, uint64_t epoch) override{
                if(lset.size()==0){
                    LOG_DEBUG<<"lset size = 0\n";
                }
                std::pair<TreeMemSeries*, bool> s = ts_head_->get_or_create_by_thmap(lset, epoch);
                sgid = s.first->source_id_;
                mid = s.first->metric_id_;
                if (s.second) series.emplace_back(s.first->source_id_,s.first->metric_id_, lset, s.first);

                return add_fast(sgid,mid, t, v);
            }

            virtual bool add_mock(const label::Labels& lset) override{
                bool is_exist = true;
                ts_head_->get_or_create_metric_id(lset,is_exist);
                ts_head_->get_or_create_source_id(lset,is_exist);
                return true;
            }

            virtual leveldb::Status add_fast(uint64_t sgid,uint16_t mid, int64_t t, double v) override {
                int64_t txn = 0;
                auto ms = ts_head_->read_flat_forward_index(sgid, mid);
                if(ms == nullptr){
                    return leveldb::Status::IOError();
                }
                samples.emplace_back(sgid,mid, t, v, txn);
                return leveldb::Status::OK();
            }
            //mock
            virtual leveldb::Status add_fast(uint64_t logical_id, int64_t t, double v) override {
                return leveldb::Status::OK();
            }

            virtual leveldb::Status commit(bool release_labels = false) override {

                leveldb::Status s = log(release_labels);
                if (!s.ok()) return leveldb::Status::IOError("log");

                uint32_t version;
                for (tsdbutil::TreeRefSample& s : samples) {
                    auto ms = ts_head_->read_flat_forward_index(s.sgid, s.mid);
                    if(ms == nullptr){
                        ms = (TreeMemSeries*)ts_head_->read_flat_forward_index(s.sgid, s.mid);
                        if (ms == nullptr) {
                            LOG_DEBUG<<"ms == null";
                            continue;
                        }
                    }
                    ms->lock();
                    ms->append(tree_series_,s.t,s.v, s.txn);
                    ms->unlock();
                }

                series.clear();
                samples.clear();
                return leveldb::Status::OK();
            }

            virtual leveldb::Status recover_commit() override {
                uint32_t version;
                for (tsdbutil::TreeRefSample& s : samples) {
                    auto ms = ts_head_->read_flat_forward_index(s.sgid, s.mid);
                    if(ms == nullptr){
                        LOG_DEBUG<<"ms == null";
                        continue;
                    }
                    ms->lock();
                    ms->append(tree_series_,s.t,s.v, s.txn);
                    ms->unlock();
                }

                series.clear();
                samples.clear();
                return leveldb::Status::OK();
            }

            virtual leveldb::Status rollback() override {
                // Series are created in the head memory regardless of rollback. Thus we
                // have to log them to the WAL in any case.
                samples.clear();
                return log();
            }

            leveldb::Status log(bool rl = false) {
                if (!series.empty()) {
                    for (auto & s : series) {
                        ts_head_->pwal_m_.AppendSeries(s);
                    }
                }
                if (!samples.empty()) {
                    for (auto & s : samples) {
                        ts_head_->pwal_m_.AppendSample(s);
                    }
                }

                return leveldb::Status::OK();
            }

        };

    }  // namespace head
}  // namespace tsdb
