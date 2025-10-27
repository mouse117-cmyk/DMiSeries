#define GLOBAL_VALUE_DEFINE

#include "parallel_wal.h"
#include "head/TreeHead.h"
#include "head/TreeHeadAppender.hpp"
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

namespace tsdb::parallel_wal {
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

    class ParallelWalTest : public testing::Test {
    public:
        leveldb::Status setup() {
            //=================TreeSeries==========
            std::string  path = "/home/dell/project/SSD/parallel_wal_test";
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
            slab::Setting *setting = new slab::Setting();
            setting->ssd_device_ = "/home/dell/project/SSD/parallel_wal_test";
            std::string info_path = "/home/dell/project/SSD/parallel_wal_info_test";
            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
            setting->ssd_slab_info_ = "/home/dell/project/SSD/parallel_wal_info_test";
            tree_series_ = new slab::TreeSeries(*setting);

            //==========LevelDB============
            std::string hd_path = "/tmp/parallel_wal_test";
            leveldb::Options options;
            options.create_if_missing = true;
            options.max_imm_num = 3;
            options.write_buffer_size = 256 * 1024 * 1024;
            options.max_file_size = 256 * 1024 * 1024;
            options.use_log = false;
            leveldb::Status st = leveldb::DB::Open(options, hd_path, &db_);
            if (!st.ok())return st;

            boost::filesystem::remove_all(hd_path);
            hd_ = new head::TreeHead(hd_path,"",db_,tree_series_);
            db_->SetTreeHead(hd_);
            return st;
        }

        ~ParallelWalTest() {
            delete db_;
            delete tree_series_;
            delete hd_;
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

        slab::TreeSeries* tree_series_;
        leveldb::DB* db_;
        head::TreeHead* hd_;
        bool release_labels;
    };

    TEST_F(ParallelWalTest, Test1){
        release_labels = true;
        ASSERT_TRUE(setup().ok());
        auto app = hd_->appender();
        int num_ts = 10000;
        int num_labels = 10;
        std::set<std::string> syms;
        uint64_t sgid;
        uint16_t mid;

        std::vector<uint64_t> mid_arr;
        std::vector<uint64_t> sgid_arr;
        std::vector<tsdb::label::Labels> label_arr;
        mid_arr.reserve(num_ts);
        sgid_arr.reserve(num_ts);
        label_arr.reserve(num_ts);
        for (int i = 0; i < num_ts; i++) {
            tsdb::label::Labels lset;
            for (int j = 0; j < num_labels; j++) {
                lset.emplace_back("label_" + std::to_string(j),
                                  "value_" + std::to_string(i));
                syms.insert("label_" + std::to_string(j));
                syms.insert("value_" + std::to_string(i));
            }
            auto ret = app->add(lset, 1, 1,sgid,mid);
            mid_arr.emplace_back(mid);
            sgid_arr.emplace_back(sgid);
            label_arr.emplace_back(lset);
            std::cout<<mid<<" "<<sgid<<" "<<tsdb::label::lbs_string(lset)<<std::endl;
            ASSERT_TRUE(app->commit(release_labels).ok());
        }

        hd_->flush_wal();

        hd_->recover_index_from_log();

        leveldb::Env* env = leveldb::Env::Default();
        leveldb::Status s;
        leveldb::SequentialFile* sf;
        std::string hd_path = "/tmp/parallel_wal_test";
        s = env->NewSequentialFile(hd_path + "/metric" + std::to_string(0) + "/" + "head_index0", &sf);
        if (!s.ok()) {
         delete sf;
        }
        leveldb::log::Reader r(sf, nullptr, false, 0);
        leveldb::Slice record;
        std::string scratch;
        uint64_t i = 0;
        while (r.ReadRecord(&record, &scratch)) {
         if (record.data()[0] == leveldb::log::kSeries) {
             std::vector<tsdb::tsdbutil::TreeRefSeries> rs;
             bool success = leveldb::log::treeSeries(record, &rs);
             if (!success) {
                 std::cout<<"fail"<<std::endl;
             }
             for (auto & r : rs) {
                 ASSERT_EQ(mid_arr[i], r.mid);
                 ASSERT_EQ(sgid_arr[i], r.sgid);
                 ASSERT_EQ(tsdb::label::lbs_string(label_arr[i]), tsdb::label::lbs_string(r.lset));
                 i++;
             }
         }
        }
        delete sf;

        ASSERT_EQ(i, num_ts);

        std::vector<std::string> existing_logs;
        boost::filesystem::path p(hd_path + "/sample" + std::to_string(1));
        boost::filesystem::directory_iterator end_itr;

        for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
            if (boost::filesystem::is_regular_file(itr->path())) {
                std::string current_file = itr->path().filename().string();
                if (current_file.size() > head::HEAD_SAMPLES_LOG_NAME.size() && memcmp(current_file.c_str(), head::HEAD_SAMPLES_LOG_NAME.c_str(), head::HEAD_SAMPLES_LOG_NAME.size()) == 0) {
                    existing_logs.push_back(current_file);
                }
            }
        }
        std::sort(existing_logs.begin(), existing_logs.end(), [&](const std::string& l, const std::string& r) {
            return std::stoi(l.substr(head::HEAD_SAMPLES_LOG_NAME.size())) < std::stoi(r.substr(head::HEAD_SAMPLES_LOG_NAME.size()));
        });

        i = 0;
        for (const auto & existing_log : existing_logs) {
            leveldb::SequentialFile* sf;
            s = env->NewSequentialFile(hd_path + "/sample" + std::to_string(1) + "/" + existing_log, &sf);
            if (!s.ok()) {
                delete sf;
            }
            leveldb::log::Reader r(sf, nullptr, false, 0);
            leveldb::Slice record;
            std::string scratch;
            std::vector<tsdb::tsdbutil::TreeRefSample> rs;
            while (r.ReadRecord(&record, &scratch)) {
                if (record.data()[0] == leveldb::log::kSample) {
                    rs.clear();
                    leveldb::log::treeSamples(record, &rs);

                    for (size_t j = 0; j < rs.size(); j++) {
                        ASSERT_EQ(mid_arr[i], rs[j].mid);
                        ASSERT_EQ(sgid_arr[i], rs[j].sgid);
                        ASSERT_EQ(1, rs[j].t);
                        ASSERT_EQ(1, rs[j].v);
                        i++;
                    }
                }
            }
        }

        ASSERT_EQ(i, num_ts);

        for (int i = 0; i < 5; i++) {
            hd_->clean_sample_log(i);
        }

        hd_->recover_samples_from_log(1);
    }

    TEST_F(ParallelWalTest_Test1_Test, Test2) {
        leveldb::Options options;
        options.create_if_missing = true;
        options.max_imm_num = 3;
        options.write_buffer_size = 256 * 1024 * 1024;
        options.max_file_size = 256 * 1024 * 1024;
        options.use_log = false;

        std::string dir = "/tmp/parallel_wal_test2";
        boost::filesystem::remove_all(dir);
        boost::filesystem::path p(dir);
        leveldb::Env* env = leveldb::Env::Default();
        leveldb::WritableFile* f;
        leveldb::Status s;
        auto status = env->CreateDir(dir);
        if (!status.ok()) {
            std::cout<<status.ToString()<<std::endl;
        }
        status = env->CreateDir(dir + "/metric" + std::to_string(0));
        if (!status.ok()) {
            std::cout<<status.ToString()<<std::endl;
        }
        s = env->NewAppendableFile(dir + "/metric" + std::to_string(0) + "/" + tsdb::head::HEAD_INDEX_LOG_NAME + std::to_string(0), &f);
        if (!s.ok()) {
            delete f;
            std::cout<<s.ToString()<<std::endl;
        }
    }

}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}