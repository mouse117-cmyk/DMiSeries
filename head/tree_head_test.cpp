#define GLOBAL_VALUE_DEFINE
#include "head/Head.hpp"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#include <boost/filesystem.hpp>

#include "base/Logging.hpp"
#include "base/ThreadPool.hpp"
#include "db/DBUtils.hpp"
#include "db/db_impl.h"
#include "gtest/gtest.h"
#include "head/HeadSeriesSet.hpp"
#include "label/EqualMatcher.hpp"
#include "querier/ChunkSeriesIterator.hpp"
#include "querier/Querier.hpp"
#include "querier/tsdb_querier.h"
#include "third_party/thread_pool.h"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WAL.hpp"
#include "TreeSeries/TreeSeries.h"
#include "TreeHead.h"

namespace tsdb::head{
    std::vector<std::vector<std::string>> devops(
            {{"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
                     "usage_irq", "usage_softirq", "usage_steal", "usage_guest",
                     "usage_guest_nice"},
             {"reads", "writes", "read_bytes", "write_bytes", "read_time", "write_time",
                     "io_time"},
             {"total", "free", "used", "used_percent", "inodes_total", "inodes_free",
                     "inodes_used"},
             {"boot_time", "interrupts", "context_switches", "processes_forked",
                     "disk_pages_in", "disk_pages_out"},
             {"total", "available", "used", "free", "cached", "buffered",
                     "used_percent", "available_percent", "buffered_percent"},
             {"bytes_sent", "bytes_recv", "packets_sent", "packets_recv", "err_in",
                     "err_out", "drop_in", "drop_out"},
             {"accepts", "active", "handled", "reading", "requests", "waiting",
                     "writing"},
             {"numbackends", "xact_commit", "xact_rollback", "blks_read", "blks_hit",
                     "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
                     "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks",
                     "blk_read_time", "blk_write_time"},
             {"uptime_in_seconds",
                     "total_connections_received",
                                            "expired_keys",
                     "evicted_keys",
                     "keyspace_hits",
                     "keyspace_misses",
                     "instantaneous_ops_per_sec",
                     "instantaneous_input_kbps",
                     "instantaneous_output_kbps",
                     "connected_clients",
                                    "used_memory",
                                                 "used_memory_rss",
                                                               "used_memory_peak",
                                                                             "used_memory_lua",
                     "rdb_changes_since_last_save",
                                      "sync_full",
                     "sync_partial_ok",
                     "sync_partial_err",
                     "pubsub_channels",
                     "pubsub_patterns",
                     "latest_fork_usec",
                     "connected_slaves",
                     "master_repl_offset",
                     "repl_backlog_active",
                     "repl_backlog_size",
                     "repl_backlog_histlen",
                     "mem_fragmentation_ratio",
                     "used_cpu_sys",
                     "used_cpu_user",
                     "used_cpu_sys_children",
                     "used_cpu_user_children"}});
    std::vector<std::string> devops_names({"cpu_", "diskio_", "disk_", "kernel_",
                                           "mem_", "net_", "nginx_", "postgres_",
                                           "redis_"});

    std::unordered_map<std::string, bool> query_types({{"1-1-1", true},
                                                       {"1-1-12", true},
                                                       {"1-8-1", true},
                                                       {"5-1-1", true},
                                                       {"5-1-12", true},
                                                       {"5-8-1", true},
                                                       {"double-groupby-1", false},
                                                       {"high-cpu-1", false},
                                                       {"high-cpu-all", false},
                                                       {"lastpoint", true}});
    class TreeHeadTest : public testing::Test {
    public:
        leveldb::Status setup() {
            //=================TreeSeries==========
            std::string  path = "/home/dell/project/SSD/tree_series_test";
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
            slab::Setting *setting = new slab::Setting();
            setting->ssd_device_ = "/home/dell/project/SSD/tree_series_test";
            std::string info_path = "/home/dell/project/SSD/tree_series_info_test";
            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
            setting->ssd_slab_info_ = "/home/dell/project/SSD/tree_series_info_test";
            tree_series_ = new slab::TreeSeries(*setting);

            //==========LevelDB============
            std::string hd_path = "/tmp/tree_head_test";
//            std::string hd_path = "/mnt/HDD/tree_head_test";
            leveldb::Options options;
            options.create_if_missing = true;
            options.max_imm_num = 3;
            options.write_buffer_size = 256 * 1024 * 1024;
            options.max_file_size = 256 * 1024 * 1024;
            options.use_log = false;
            leveldb::Status st = leveldb::DB::Open(options, hd_path, &db_);
            if (!st.ok())return st;

            boost::filesystem::remove_all(hd_path);
            hd_ = new TreeHead(hd_path,"",db_,tree_series_);
            db_->SetTreeHead(hd_);
            return st;
        }
        ~TreeHeadTest() {
            if (db_) delete db_;
            if(tree_series_) delete tree_series_;
            if(hd_) delete hd_;
        }
        void load_devops_labels1(int num_ts,
                                 std::vector<tsdb::label::Labels>* lsets) {
            char instance[64];
            int current_instance = 0;
            std::ifstream file("../test/devops100000.txt");
            std::string line;
            int num_lines = num_ts / 100;
            int cur_line = 0;
            int ts_counter = 1;

            std::vector<std::string> items, names, values;
            for (size_t round = 0; round < devops_names.size(); round++) {
                while (cur_line < num_lines) {
                    getline(file, line);

                    size_t pos_start = 0, pos_end, delim_len = 1;
                    std::string token;
                    items.clear();
                    while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
                        token = line.substr(pos_start, pos_end - pos_start);
                        pos_start = pos_end + delim_len;
                        items.push_back(token);
                    }
                    items.push_back(line.substr(pos_start));

                    names.clear();
                    values.clear();
                    for (size_t i = 1; i < items.size(); i++) {
                        pos_end = items[i].find("=");
                        names.push_back(items[i].substr(0, pos_end));
                        values.push_back(items[i].substr(pos_end + 1));
                    }

                    for (size_t i = 0; i < devops[round].size(); i++) {
                        tsdb::label::Labels lset;
                        for (size_t j = 0; j < names.size(); j++)
                            lset.emplace_back(names[j], values[j]);
                        lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
                        std::sort(lset.begin(), lset.end());

                        lsets->push_back(std::move(lset));

                        ts_counter++;
                    }
                    cur_line++;
                }
                for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
                cur_line = 0;
            }
        }

        void insert_simple_data(int num_ts, int num_labels, int num_samples,
                                std::vector<std::vector<int64_t>>* times,
                                std::vector<std::vector<double>>* values) {
            auto app = hd_->appender();
            for (int i = 0; i < num_ts; i++) {
                tsdb::label::Labels lset;
                for (int j = 0; j < num_labels; j++)
                    lset.emplace_back("label_" + std::to_string(j),
                                      "value_" + std::to_string(i));
                uint64_t sgid;
                uint16_t mid;
                auto r = app->add(lset, 0, 0,sgid,mid);
                //ASSERT_EQ(r, leveldb::Status::OK());
                std::vector<int64_t> tmp_times;
                std::vector<double> tmp_values;
                tmp_times.push_back(0);
                tmp_values.push_back(0);
                for (int j = 0; j < num_samples; j++) {
                    //int64_t t = (j + 1) * 1000 + rand() % 100;
                    int64_t t = (j+1) * 1000;
                    double v = rand() % 200;
                    //double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
                    tmp_times.push_back(t);
                    tmp_values.push_back(v);
                    leveldb::Status st = app->add_fast(sgid, mid, t, v);
                    ASSERT_TRUE(st.ok());
                }
                times->push_back(tmp_times);
                values->push_back(tmp_values);
                ASSERT_TRUE(app->commit(release_labels).ok());

                if ((i + 1) % (num_ts / 10) == 0)
                    printf("insert_simple_data:%d\n", i + 1);
            }
        }

        void insert_simple_data_parallel(int num_ts, int num_labels, int num_samples,
                                         int num_thread) {
            auto func = [](head::TreeHead* _h, int left, int right, int num_labels,
                           int num_samples, base::WaitGroup* _wg, bool release_labels) {
                auto app = _h->appender();
                for (int i = left; i < right; i++) {
                    MasstreeWrapper<slab::SlabInfo>::ti=threadinfo::make(threadinfo::TI_PROCESS, 16+i);
                    tsdb::label::Labels lset;
                    lset.emplace_back("__name__","name" + std::to_string(left));
                    for (int j = 0; j < num_labels - 1; j++)
                        lset.emplace_back("label_" + std::to_string(j),
                                          "value_" + std::to_string(left));
                    uint64_t sgid;
                    uint16_t mid;
                    auto r = app->add(lset, 0, 0,sgid,mid);
                    for (int j = 0; j < num_samples; j++)
                        app->add_fast(sgid, mid, (j + 1) * 1000, (j + 1) * 1000);
                    app->commit(release_labels);
                }
                _wg->done();
            };

            ThreadPool pool(num_thread);
            base::WaitGroup wg;
            for (int i = 0; i < num_thread; i++) {
                wg.add(1);
                pool.enqueue(std::bind(func, hd_, i * num_ts / num_thread,
                                       (i + 1) * num_ts / num_thread, num_labels,
                                       num_samples, &wg, release_labels));
            }
            wg.wait();
        }


        void insert_simple_data_parallel_2(int num_ts, int num_labels, int num_samples,
                                         int num_thread) {
            std::vector<int64_t> timestamps;
            std::vector<double> values;
            timestamps.reserve(num_samples);
            values.reserve(num_samples);

            for (int i = 0; i < num_samples; i++) {
                timestamps.emplace_back((i+1) );
                values.emplace_back((i+1) );
               // std::cout<<i<<std::endl;
            }
            std::cout<<"generate complete"<<std::endl;

            auto func = [](head::TreeHead* _h, int left, int right, int num_labels,
                           int num_samples, base::WaitGroup* _wg, bool release_labels, std::vector<int64_t>& timestamps, std::vector<double>& values) {
                auto app = _h->appender();
                for (int i = left; i < right; i++) {
                    MasstreeWrapper<slab::SlabInfo>::ti=threadinfo::make(threadinfo::TI_PROCESS, 16+i);
                    tsdb::label::Labels lset;
                    lset.emplace_back("__name__","name" + std::to_string(left));
                    for (int j = 0; j < num_labels - 1; j++)
                        lset.emplace_back("label_" + std::to_string(j),
                                          "value_" + std::to_string(left));
                    uint64_t sgid;
                    uint16_t mid;
                    auto r = app->add(lset, 0, 0,sgid,mid);
                    for (int j = 0; j < num_samples; j++) {
                        app->add_fast(sgid, mid, timestamps[j], values[j]);
                    }

                    app->commit(release_labels);
                }
                _wg->done();
            };

            ThreadPool pool(num_thread);
            base::WaitGroup wg;
            for (int i = 0; i < num_thread; i++) {
                wg.add(1);
                pool.enqueue(std::bind(func, hd_, i * num_ts / num_thread,
                                       (i + 1) * num_ts / num_thread, num_labels,
                                       num_samples, &wg, release_labels, timestamps, values));
            }
            wg.wait();
        }

// private:
        //slab::Setting* setting;
        slab::TreeSeries* tree_series_;
        leveldb::DB* db_;
        TreeHead* hd_;
        bool release_labels;

    };

    TEST_F(TreeHeadTest, Test1){
        release_labels = true;
        ASSERT_TRUE(setup().ok());
        auto app = hd_->appender();
        int num_ts = 10000;
        int num_labels = 10;
        std::set<std::string> syms;
        uint64_t sgid;
        uint16_t mid;
        for (int i = 0; i < num_ts; i++) {
            tsdb::label::Labels lset;
            for (int j = 0; j < num_labels; j++) {
                lset.emplace_back("label_" + std::to_string(j),
                                  "value_" + std::to_string(i));
                syms.insert("label_" + std::to_string(j));
                syms.insert("value_" + std::to_string(i));
            }
            auto r = app->add(std::move(lset), 0, 0,sgid,mid);
            std::cout<<"sgid: "<<sgid<<" mid: "<<mid<<std::endl;
            //ASSERT_EQ(i, r.first);
            ASSERT_TRUE(app->commit(release_labels).ok());
        }
    }

    TEST_F(TreeHeadTest, Test2){
        release_labels = true;
        ASSERT_TRUE(setup().ok());

        int num_ts = 10000;
        int num_labels = 10;
        int num_samples = 32000;
        std::vector<std::vector<int64_t>> times;
        std::vector<std::vector<double>> values;
        hd_->bg_flush_data();
        insert_simple_data(num_ts, num_labels, num_samples, &times, &values);
        sleep(30);
        hd_->stop_bg_flush_data();
        std::cout<<tree_series_->GetFreeMemSlabNum()<<" "<<tree_series_->GetUsedMemSlabNum()<<" "<<tree_series_->GetFullMemSlabNum()<<std::endl;
        std::cout<<tree_series_->GetFreeDiskSlabNum()<<" "<<tree_series_->GetUsedDiskSlabNum();
    }
    // Parallel insertion.
    TEST_F(TreeHeadTest, Test3) {
        release_labels = true;
     //   MAX_HEAD_SAMPLES_LOG_SIZE = 1024 * 1024;
        ASSERT_TRUE(setup().ok());

        int num_ts = 10000;
        int num_labels = 10;
        int num_samples = 32000*10*5;
        std::vector<std::vector<int64_t>> times;
        std::vector<std::vector<double>> values;
        hd_->bg_flush_data();
        insert_simple_data_parallel_2(num_ts, num_labels, num_samples, 16);
        sleep(30);
        hd_->stop_bg_flush_data();
        std::cout<<tree_series_->GetFreeMemSlabNum()<<" "<<tree_series_->GetUsedMemSlabNum()<<" "<<tree_series_->GetFullMemSlabNum()<<std::endl;
        std::cout<<tree_series_->GetFreeDiskSlabNum()<<" "<<tree_series_->GetUsedDiskSlabNum();
    }

    // flat_forward_index
    TEST_F(TreeHeadTest, Test4) {
        release_labels = true;

        ASSERT_TRUE(setup().ok());

        uint16_t first_mid = 100;
        uint64_t first_sgid = 5000;
        for (uint32_t mid = 1; mid <= first_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= first_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                label::Labels lbs;
                lbs.emplace_back(lb);
                hd_->set_to_forward_index(sgid, mid, new TreeMemSeries(sgid, mid, lbs, 0));
            }
        }

        ASSERT_EQ(hd_->get_flat_forward_index(), nullptr);
        bool status = hd_->migrate_to_flat_forward_index(first_sgid, first_mid);
        ASSERT_EQ(status, true);
        ASSERT_EQ(hd_->get_migrate_mid(), first_mid);
        ASSERT_EQ(hd_->get_migrate_sgid(), first_sgid);

        for (uint16_t mid = 1; mid <= first_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= first_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                TreeMemSeries* ms = hd_->read_flat_forward_index(sgid, mid);
//                std::cout<<ms->metric_id_<<" "<<ms->source_id_<<" "<<ms->labels[0].label<<" "<<ms->labels[0].value<<std::endl;
                ASSERT_EQ(ms->metric_id_, mid);
                ASSERT_EQ(ms->source_id_, sgid);
                ASSERT_EQ(ms->labels[0].label, lb.label);
                ASSERT_EQ(ms->labels[0].value, lb.value);
            }
        }

        uint16_t second_mid = 100;
        uint64_t second_sgid = 7000;
        for (uint32_t mid = 1; mid <= second_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= second_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                label::Labels lbs;
                lbs.emplace_back(lb);
                hd_->set_to_forward_index(sgid, mid, new TreeMemSeries(sgid, mid, lbs, 0));
            }
        }

        ASSERT_NE(hd_->get_flat_forward_index(), nullptr);
        status = hd_->migrate_to_flat_forward_index(second_sgid, second_mid);
        ASSERT_EQ(status, true);
        ASSERT_EQ(hd_->get_migrate_mid(), second_mid);
        ASSERT_EQ(hd_->get_migrate_sgid(), second_sgid);

        for (uint16_t mid = 1; mid <= second_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= second_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                TreeMemSeries* ms = hd_->read_flat_forward_index(sgid, mid);
//                std::cout<<ms->metric_id_<<" "<<ms->source_id_<<" "<<ms->labels[0].label<<" "<<ms->labels[0].value<<std::endl;
                ASSERT_EQ(ms->metric_id_, mid);
                ASSERT_EQ(ms->source_id_, sgid);
                ASSERT_EQ(ms->labels[0].label, lb.label);
                ASSERT_EQ(ms->labels[0].value, lb.value);
            }
        }

        uint16_t third_mid = 300;
        uint64_t third_sgid = 5000;
        for (uint16_t mid = 1; mid <= third_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= third_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                label::Labels lbs;
                lbs.emplace_back(lb);
                hd_->set_to_forward_index(sgid, mid, new TreeMemSeries(sgid, mid, lbs, 0));
            }
        }

        ASSERT_NE(hd_->get_flat_forward_index(), nullptr);
        status = hd_->migrate_to_flat_forward_index(third_sgid, third_mid);
        ASSERT_EQ(status, false);
        ASSERT_EQ(hd_->get_migrate_mid(), second_mid);
        ASSERT_EQ(hd_->get_migrate_sgid(), second_sgid);

        for (uint16_t mid = 1; mid <= third_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= third_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                TreeMemSeries* ms = hd_->read_flat_forward_index(sgid, mid);
//                std::cout<<ms->metric_id_<<" "<<ms->source_id_<<" "<<ms->labels[0].label<<" "<<ms->labels[0].value<<std::endl;
                ASSERT_EQ(ms->metric_id_, mid);
                ASSERT_EQ(ms->source_id_, sgid);
                ASSERT_EQ(ms->labels[0].label, lb.label);
                ASSERT_EQ(ms->labels[0].value, lb.value);
            }
        }

        uint16_t forth_mid = 50;
        uint64_t forth_sgid = 8000;
        for (uint16_t mid = 1; mid <= forth_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= forth_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                label::Labels lbs;
                lbs.emplace_back(lb);
                hd_->set_to_forward_index(sgid, mid, new TreeMemSeries(sgid, mid, lbs, 0));
            }
        }

        ASSERT_NE(hd_->get_flat_forward_index(), nullptr);
        status = hd_->migrate_to_flat_forward_index(forth_sgid, forth_mid);
        ASSERT_EQ(status, false);
        ASSERT_EQ(hd_->get_migrate_mid(), second_mid);
        ASSERT_EQ(hd_->get_migrate_sgid(), second_sgid);

        for (uint16_t mid = 1; mid <= forth_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= forth_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                TreeMemSeries* ms = hd_->read_flat_forward_index(sgid, mid);
//                std::cout<<ms->metric_id_<<" "<<ms->source_id_<<" "<<ms->labels[0].label<<" "<<ms->labels[0].value<<std::endl;
                ASSERT_EQ(ms->metric_id_, mid);
                ASSERT_EQ(ms->source_id_, sgid);
                ASSERT_EQ(ms->labels[0].label, lb.label);
                ASSERT_EQ(ms->labels[0].value, lb.value);
            }
        }

        uint16_t fifth_mid = 500;
        uint64_t fifth_sgid = 10000;
        for (uint16_t mid = 1; mid <= fifth_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= fifth_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                label::Labels lbs;
                lbs.emplace_back(lb);
                hd_->set_to_forward_index(sgid, mid, new TreeMemSeries(sgid, mid, lbs, 0));
            }
        }

        ASSERT_NE(hd_->get_flat_forward_index(), nullptr);
        status = hd_->migrate_to_flat_forward_index(fifth_sgid, fifth_mid);
        ASSERT_EQ(status, true);
        ASSERT_EQ(hd_->get_migrate_mid(), fifth_mid);
        ASSERT_EQ(hd_->get_migrate_sgid(), fifth_sgid);

        for (uint16_t mid = 1; mid <= fifth_mid; mid++) {
            for (uint64_t sgid = 1; sgid <= fifth_sgid; sgid++) {
                label::Label lb("label_"+std::to_string(mid), "value_"+std::to_string(sgid));
                TreeMemSeries* ms = hd_->read_flat_forward_index(sgid, mid);
//                std::cout<<ms->metric_id_<<" "<<ms->source_id_<<" "<<ms->labels[0].label<<" "<<ms->labels[0].value<<std::endl;
                ASSERT_EQ(ms->metric_id_, mid);
                ASSERT_EQ(ms->source_id_, sgid);
                ASSERT_EQ(ms->labels[0].label, lb.label);
                ASSERT_EQ(ms->labels[0].value, lb.value);
            }
        }

        hd_->destroy_flat_forward_index();
        ASSERT_EQ(hd_->get_flat_forward_index(), nullptr);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}