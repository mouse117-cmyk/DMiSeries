#define GLOBAL_VALUE_DEFINE
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#include <signal.h>

#include <boost/filesystem.hpp>
#include <jemalloc/jemalloc.h>
#include <yaml-cpp/yaml.h>

#include "chunk/XORChunk.hpp"
#include "db/version_set.h"
#include "head/Head.hpp"
#include "head/HeadAppender.hpp"
#include "head/MemSeries.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"
#include "leveldb/cache.h"
#include "port/port.h"
#include "querier/tsdb_tree_querier.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/thread_pool.h"
#include "testutil/label_generator.h"
#include "testutil/iostat.h"
#include "ValueLog//config.h"
#include "head/SpanHead.h"
#include "head/SpanHeadAppender.h"
#include "querier/SpanQuerier.h"

namespace tsdb {
    namespace db {

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
//                         "keyspace_misses",
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
                                                           {"1-1-12", false},
                                                           {"1-1-24", true},
                                                           {"1-1-all", false},
                                                           {"1-8-1", true},
                                                           {"5-1-1", true},
                                                           {"5-1-12", false},
                                                           {"5-1-24", true},
                                                           {"5-1-all", false},
                                                           {"5-8-1", true},
                                                           {"cpu-max-all-8", true},
                                                           {"double-groupby-5", false},
                                                           {"double-groupby-all", false},
                                                           {"high-cpu-1", false},
                                                           {"high-cpu-all", false},
                                                           {"lastpoint", true}});

        class TSDBTest {
        public:
            TSDBTest() {
                for (int i = 0; i < 50; i++)
                    matchers1.emplace_back("hostname", "host_" + std::to_string(i));
                matchers2 = std::vector<tsdb::label::EqualMatcher>(
                        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
                         tsdb::label::EqualMatcher("__name__", "diskio_reads"),
                         tsdb::label::EqualMatcher("__name__", "kernel_boot_time"),
                         tsdb::label::EqualMatcher("__name__", "mem_total"),
                         tsdb::label::EqualMatcher("__name__", "net_bytes_sent")});
                matchers3 = std::vector<tsdb::label::EqualMatcher>(
                        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_system"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_idle"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_nice"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_iowait"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_irq"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_softirq"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_steal"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_guest"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_guest_nice")});
            }

            void set_parameters(int num_ts_, int tuple_size_, int num_tuple_) {
                num_ts = num_ts_;
                tuple_size = tuple_size_;
                num_tuple = num_tuple_;
                head::MEM_TUPLE_SIZE = tuple_size_;
                for (int i = 50; i < num_ts/100; i++)
                    matchers1.emplace_back("hostname", "host_" + std::to_string(i));
            }


            int load_devops(int num_lines, const std::string& name, int64_t st,
                            int64_t interval, int ts_counter) {
//                std::ifstream file(name);
                std::string line;
                int cur_line = 0;

                std::vector<int64_t> tfluc;
                for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
                std::vector<std::string> items, names, values;
                for (size_t round = 0; round < devops_names.size(); round++) {
                    std::ifstream file(name);
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
                            auto app = head_->appender();
                            tsdb::label::Labels lset;
                            for (size_t j = 0; j < names.size(); j++)
                                lset.emplace_back(names[j], values[j]);
                            lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
                            std::sort(lset.begin(), lset.end());

                            auto r = app->add(lset, tfluc[0], tfluc[0]);
                            if (r.first != ((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                                           (uint64_t)(ts_counter % PRESERVED_BLOCKS)) {
                                std::cout << tsdb::label::lbs_string(lset) << std::endl;
                                std::cout << "TSDBTest::load_devops wrong id exp:" << ts_counter
                                          << " got:" << r.first << std::endl;
                            }

                            for (int k = 1; k < tuple_size; k++)
                                app->add_fast(((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                                              (uint64_t)(ts_counter % PRESERVED_BLOCKS),
                                              st + k * interval * 1000 + tfluc[k], tfluc[k]);
                            app->commit();

                            ts_counter++;
                        }
                        cur_line++;
                    }
//                    for (int i = 0; i < 100000 - cur_line; i++) {
//                        getline(file, line);
//                    }
                    cur_line = 0;
                }
                return ts_counter;
            }

            // Devops labels.
            void head_add3(int64_t st, int64_t interval) {
                int num_lines = num_ts / 100;
                int tscounter;
                if (num_lines > 100000) {
                    tscounter =
                            load_devops(100000, "../test/devops100000.txt", st, interval, 0);
                    tscounter = load_devops(num_lines - 100000, "../test/devops100000-2.txt",
                                            st, interval, tscounter);
                } else
                    tscounter =
                            load_devops(num_lines, "../test/devops100000.txt", st, interval, 0);
                std::cout << "head_add3: " << tscounter << std::endl;
            }

            int load_devops(int num_lines, const std::string& name,
                            std::vector<label::Labels>* lsets, int ts_counter) {
                std::ifstream file(name);
                std::string line;
                int cur_line = 0;

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
                return ts_counter;
            }

            void get_devops_labels(std::vector<label::Labels>* lsets) {
                int num_lines = num_ts / 100;
                int tscounter;
                if (num_lines > 100000) {
                    tscounter = load_devops(100000, "../test/devops100000.txt", lsets, 0);
                    tscounter = load_devops(num_lines - 100000, "../test/devops100000-2.txt",
                                            lsets, tscounter);
                } else
                    tscounter = load_devops(num_lines, "../test/devops100000.txt", lsets, 0);
                std::cout << "get_devops_labels: " << tscounter << std::endl;
            }

            void head_add_fast1(int64_t st, int64_t interval) {
                // auto app = head_->TEST_appender();
                Timer t;
                // int count = 0;
                int64_t d, last_t;
                t.start();
                auto app = head_->appender();
                std::vector<int64_t> tfluc;
                for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
                for (int i = 0; i < num_ts; i++) {
                    for (int k = 0; k < tuple_size; k++)
                        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                                      (uint64_t)(i % PRESERVED_BLOCKS),
                                      st + k * interval * 1000 + tfluc[k], tfluc[k]);
                    app->commit();
                }

            }

            void queryDevOps2(leveldb::DB* db, int64_t endtime,
                              leveldb::Cache* cache = nullptr, int iteration = 1000) {
                int big_iteration = 3;
                // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
                // hours.
                if (query_types["1-1-1"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 3600000, endtime,
                                               cache);
                        Timer t;
                        t.start();

//        std::vector<tsdb::label::MatcherInterface*> matchers(
//            {&matchers1[0], &matchers2[0]});
                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[0]))});
                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                q.select(matchers);
                        while (ss->next()) {
                            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

                            if (series == nullptr) continue;
                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                    series->chain_iterator();
                            while (it->next()) {
//                                std::cout<<it->at().first<< " ";
                                ++total_samples;
                            }
//                            std::cout<<std::endl;
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[1-1-1] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << ":"
                                  << total_samples << std::endl;
                }

                // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12
                // hours.
                if (query_types["1-1-12"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 43200000, endtime,
                                               cache);
                        Timer t;
                        t.start();

//        std::vector<tsdb::label::MatcherInterface*> matchers(
//            {&matchers1[0], &matchers2[0]});
                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[0]))});
                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                q.select(matchers);
                        while (ss->next()) {
                            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                            if (series == nullptr) continue;
                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                    series->chain_iterator();
                            while (it->next()) {
                                ++total_samples;
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[1-1-12] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                if (query_types["1-1-24"] && endtime - 86400000 > -120000) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 86400000, endtime,
                                               cache);
                        Timer t;
                        t.start();

//        std::vector<tsdb::label::MatcherInterface*> matchers(
//            {&matchers1[0], &matchers2[0]});
                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[0]))});
                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                q.select(matchers);
                        while (ss->next()) {
                            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                            if (series == nullptr) continue;
                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                    series->chain_iterator();
                            while (it->next()) {
                                ++total_samples;
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[1-1-24] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                if (query_types["1-1-all"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, 0, endtime, cache);
                        Timer t;
                        t.start();

//        std::vector<tsdb::label::MatcherInterface*> matchers(
//            {&matchers1[0], &matchers2[0]});
                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[0]))});
                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                q.select(matchers);
                        while (ss->next()) {
                            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                            if (series == nullptr) continue;
                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                    series->chain_iterator();
                            while (it->next()) {
                                ++total_samples;
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[1-1-all] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
                // hour.
                if (query_types["1-8-1"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 3600000, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int host = 0; host < 8; host++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1[host], &matchers2[0]});
                            std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                    {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[host])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[0]))});
                            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                    q.select(matchers);
                            while (ss->next()) {
                                std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                if (series == nullptr) continue;
                                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                        series->chain_iterator();
                                while (it->next()) {
                                    ++total_samples;
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[1-8-1] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
                if (query_types["5-1-1"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 3600000, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int j = 0; j < 5; j++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1[0], &matchers2[j]});
                            std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                    {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[j]))});
                            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                    q.select(matchers);
                            while (ss->next()) {
                                std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                if (series == nullptr) continue;
                                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                        series->chain_iterator();
                                while (it->next()) {
                                    ++total_samples;
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[5-1-1] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12
                // hour.
                if (query_types["5-1-12"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 43200000, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int j = 0; j < 5; j++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1[0], &matchers2[j]});
                            std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                    {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[j]))});
                            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                    q.select(matchers);
                            while (ss->next()) {
                                std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                if (series == nullptr) continue;
                                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                        series->chain_iterator();
                                while (it->next()) {
                                    ++total_samples;
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[5-1-12] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                if (query_types["5-1-24"] && endtime - 86400000 > -120000) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 86400000, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int j = 0; j < 5; j++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1[0], &matchers2[j]});
                            std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                    {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[j]))});
                            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                    q.select(matchers);
                            while (ss->next()) {
                                std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                if (series == nullptr) continue;
                                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                        series->chain_iterator();
                                while (it->next()) {
                                    ++total_samples;
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[5-1-24] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                if (query_types["5-1-all"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, 0, endtime, cache);
                        Timer t;
                        t.start();

                        for (int j = 0; j < 5; j++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1[0], &matchers2[j]});
                            std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                    {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[j]))});
                            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                    q.select(matchers);
                            while (ss->next()) {
                                std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                if (series == nullptr) continue;
                                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                        series->chain_iterator();
                                while (it->next()) {
                                    ++total_samples;
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[5-1-all] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
                // hour.
                if (query_types["5-8-1"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 3600000, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int host = 0; host < 8; host++) {
                            for (int j = 0; j < 5; j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1[host], &matchers2[j]});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                        {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[host])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[j]))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q.select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                            ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        ++total_samples;
                                    }
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[5-8-1] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                // Last reading of a metric of a host.
                if (query_types["lastpoint"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime, endtime + 1000, cache);
                        Timer t;
                        t.start();

//        std::vector<tsdb::label::MatcherInterface*> matchers(
//            {&matchers1[0], &matchers2[0]});
                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[0]))});
                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                q.select(matchers);
                        while (ss->next()) {
                            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                            if (series == nullptr) continue;
                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                    series->chain_iterator();
                            while (it->next()) {
                                ++total_samples;
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[lastpoint] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << ":"
                                  << total_samples << std::endl;
                }

                // Aggregate across all CPU metrics per hour over 1 hour for eight hosts
                if (query_types["cpu-max-all-8"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, endtime - 3600000, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int host = 0; host < 8; host++) {
                            for (int j = 0; j < matchers3.size(); j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1[host], &matchers3[j]});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                        {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[host])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers3[j]))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q.select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                            ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        ++total_samples;
                                    }
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[cpu-max-all-8] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / iteration << "us "
                                  << "samples:" << total_samples / iteration << std::endl;
                }

                // Aggregate on across both time and host, giving the average of 5 CPU metrics per host per hour for 24 hours
                if (query_types["double-groupby-5"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < big_iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, 0, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int host = 0; host < matchers1.size(); host++) {
                            for (int j = 0; j < 5; j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1[host], &matchers3[j]});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                        {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[host])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers3[j]))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q.select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                            ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        ++total_samples;
                                    }
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[double-groupby-5] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / big_iteration << "us "
                                  << "samples:" << total_samples / big_iteration << std::endl;
                }

                // Aggregate on across both time and host, giving the average of all (10) CPU metrics per host per hour for 24 hours
                if (query_types["double-groupby-all"]) {
                    int64_t total_samples = 0;
                    int64_t duration = 0;
                    for (int round = 0; round < big_iteration; round++) {
                        querier::SpanQuerier q(db, head_, tree_series_, 0, endtime,
                                               cache);
                        Timer t;
                        t.start();

                        for (int host = 0; host < matchers1.size(); host++) {
                            for (int j = 0; j < matchers3.size(); j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1[host], &matchers3[j]});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
                                        {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[host])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers3[j]))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q.select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                            ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        ++total_samples;
                                    }
                                }
                            }
                        }
                        duration += t.since_start_nano();
                    }
                    if (iteration > 10)
                        std::cout << "[double-groupby-all] duration(total):" << duration / 1000 << "us "
                                  << "duration(avg):" << duration / 1000 / big_iteration << "us "
                                  << "samples:" << total_samples / big_iteration << std::endl;
                }
            }

            void queryDevOps2Parallel(Thread_Pool* pool, int max_thread_num, leveldb::DB* db,
                                      int64_t endtime, leveldb::Cache* cache = nullptr) {
                int iteration = 100000;
                int big_iteration = 64;
                base::WaitGroup wg;

                std::ofstream output_file("parallel_query_latency.log", std::ios_base::app);  // Open file in append mode

                if (!output_file.is_open()) {
                    std::cerr << "Failed to open output file." << std::endl;
                    return;
                }

                for (uint32_t i = 1; i <= max_thread_num; i*=2) {
                    int thread_num = i;
                    output_file << "thread_num: " << thread_num << "\n";
                    output_file.flush();

                    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
                    // hours.
                    if (query_types["1-1-1"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1->at(0), &matchers2->at(0)});
//          std::vector<std::shared_ptr<tsdb::label::MatcherInterface>>matchers(
//              {std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers1[0])), std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(matchers2[0]))});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                        {std::shared_ptr<label::MatcherInterface>(
                                                new label::EqualMatcher(matchers1->at(0))),
                                         std::shared_ptr<label::MatcherInterface>(
                                                 new label::EqualMatcher(matchers2->at(0)))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q->select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        total_samples->fetch_add(1);
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 3600000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[1-1-1] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << ":"
                                  << total_samples.load() << std::endl;

                        output_file << "[1-1-1] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << ":"
                                    << total_samples.load() << "\n";
                        output_file.flush();
                    }

                    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12
                    // hours.
                    if (query_types["1-1-12"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1->at(0), &matchers2->at(0)});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                        {std::shared_ptr<label::MatcherInterface>(
                                                new label::EqualMatcher(matchers1->at(0))),
                                         std::shared_ptr<label::MatcherInterface>(
                                                 new label::EqualMatcher(matchers2->at(0)))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q->select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        total_samples->fetch_add(1);
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 43200000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[1-1-12] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[1-1-12] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    if (query_types["1-1-24"] && endtime - 86400000 > -120000) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1->at(0), &matchers2->at(0)});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                        {std::shared_ptr<label::MatcherInterface>(
                                                new label::EqualMatcher(matchers1->at(0))),
                                         std::shared_ptr<label::MatcherInterface>(
                                                 new label::EqualMatcher(matchers2->at(0)))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q->select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        total_samples->fetch_add(1);
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 86400000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[1-1-24] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[1-1-24] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    if (query_types["1-1-all"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1->at(0), &matchers2->at(0)});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                        {std::shared_ptr<label::MatcherInterface>(
                                                new label::EqualMatcher(matchers1->at(0))),
                                         std::shared_ptr<label::MatcherInterface>(
                                                 new label::EqualMatcher(matchers2->at(0)))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q->select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        total_samples->fetch_add(1);
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, 0, endtime, cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[1-1-all] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[1-1-all] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
                    // hour.
                    if (query_types["1-8-1"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int host = 0; host < 8; host++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1->at(host), &matchers2->at(0)});
                                    std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                            {std::shared_ptr<label::MatcherInterface>(
                                                    new label::EqualMatcher(matchers1->at(0))),
                                             std::shared_ptr<label::MatcherInterface>(
                                                     new label::EqualMatcher(matchers2->at(0)))});
                                    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                            q->select(matchers);
                                    while (ss->next()) {
                                        std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                ss->at();
                                        if (series == nullptr) continue;
                                        std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                series->chain_iterator();
                                        while (it->next()) {
                                            total_samples->fetch_add(1);
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 3600000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[1-8-1] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[1-8-1] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
                    if (query_types["5-1-1"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int j = 0; j < 5; j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1->at(0), &matchers2->at(j)});
                                    std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                            {std::shared_ptr<label::MatcherInterface>(
                                                    new label::EqualMatcher(matchers1->at(0))),
                                             std::shared_ptr<label::MatcherInterface>(
                                                     new label::EqualMatcher(matchers2->at(j)))});
                                    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                            q->select(matchers);
                                    while (ss->next()) {
                                        std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                ss->at();
                                        if (series == nullptr) continue;
                                        std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                series->chain_iterator();
                                        while (it->next()) {
                                            total_samples->fetch_add(1);
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 3600000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[5-1-1] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[5-1-1] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12
                    // hour.
                    if (query_types["5-1-12"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int j = 0; j < 5; j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1->at(0), &matchers2->at(j)});
                                    std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                            {std::shared_ptr<label::MatcherInterface>(
                                                    new label::EqualMatcher(matchers1->at(0))),
                                             std::shared_ptr<label::MatcherInterface>(
                                                     new label::EqualMatcher(matchers2->at(j)))});
                                    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                            q->select(matchers);
                                    while (ss->next()) {
                                        std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                ss->at();
                                        if (series == nullptr) continue;
                                        std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                series->chain_iterator();
                                        while (it->next()) {
                                            total_samples->fetch_add(1);
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 43200000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[5-1-12] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[5-1-12] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    if (query_types["5-1-24"] && endtime - 86400000 > -120000) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int j = 0; j < 5; j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1->at(0), &matchers2->at(j)});
                                    std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                            {std::shared_ptr<label::MatcherInterface>(
                                                    new label::EqualMatcher(matchers1->at(0))),
                                             std::shared_ptr<label::MatcherInterface>(
                                                     new label::EqualMatcher(matchers2->at(j)))});
                                    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                            q->select(matchers);
                                    while (ss->next()) {
                                        std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                ss->at();
                                        if (series == nullptr) continue;
                                        std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                series->chain_iterator();
                                        while (it->next()) {
                                            total_samples->fetch_add(1);
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 86400000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[5-1-24] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[5-1-24] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    if (query_types["5-1-all"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int j = 0; j < 5; j++) {
//            std::vector<tsdb::label::MatcherInterface*> matchers(
//                {&matchers1->at(0), &matchers2->at(j)});
                                    std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                            {std::shared_ptr<label::MatcherInterface>(
                                                    new label::EqualMatcher(matchers1->at(0))),
                                             std::shared_ptr<label::MatcherInterface>(
                                                     new label::EqualMatcher(matchers2->at(j)))});
                                    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                            q->select(matchers);
                                    while (ss->next()) {
                                        std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                ss->at();
                                        if (series == nullptr) continue;
                                        std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                series->chain_iterator();
                                        while (it->next()) {
                                            total_samples->fetch_add(1);
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, 0, endtime, cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[5-1-all] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[5-1-all] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
                    // hour.
                    if (query_types["5-8-1"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int host = 0; host < 8; host++) {
                                    for (int j = 0; j < 5; j++) {
//              std::vector<tsdb::label::MatcherInterface*> matchers(
//                  {&matchers1->at(host), &matchers2->at(j)});
                                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                                {std::shared_ptr<label::MatcherInterface>(
                                                        new label::EqualMatcher(matchers1->at(0))),
                                                 std::shared_ptr<label::MatcherInterface>(
                                                         new label::EqualMatcher(matchers2->at(j)))});
                                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                                q->select(matchers);
                                        while (ss->next()) {
                                            std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                    ss->at();
                                            if (series == nullptr) continue;
                                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                    series->chain_iterator();
                                            while (it->next()) {
                                                total_samples->fetch_add(1);
                                            }
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 3600000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[5-8-1] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[5-8-1] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    // Last reading of a metric of a host.
                    if (query_types["lastpoint"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
//          std::vector<tsdb::label::MatcherInterface*> matchers(
//              {&matchers1->at(0), &matchers2->at(0)});
                                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                        {std::shared_ptr<label::MatcherInterface>(
                                                new label::EqualMatcher(matchers1->at(0))),
                                         std::shared_ptr<label::MatcherInterface>(
                                                 new label::EqualMatcher(matchers2->at(0)))});
                                std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                        q->select(matchers);
                                while (ss->next()) {
                                    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
                                    if (series == nullptr) continue;
                                    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                            series->chain_iterator();
                                    while (it->next()) {
                                        total_samples->fetch_add(1);
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime, endtime + 1000, cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[lastpoint] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << ":"
                                  << total_samples.load() << std::endl;

                        output_file << "[lastpoint] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << ":"
                                    << total_samples.load() << "\n";
                        output_file.flush();
                    }

                    // Aggregate across all CPU metrics per hour over 1 hour for eight hosts
                    if (query_types["cpu-max-all-8"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int host = 0; host < 8; host++) {
                                    for (int j = 0; j < matchers2->size(); j++) {
//              std::vector<tsdb::label::MatcherInterface*> matchers(
//                  {&matchers1->at(host), &matchers2->at(j)});
                                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                                {std::shared_ptr<label::MatcherInterface>(
                                                        new label::EqualMatcher(matchers1->at(host))),
                                                 std::shared_ptr<label::MatcherInterface>(
                                                         new label::EqualMatcher(matchers2->at(j)))});
                                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                                q->select(matchers);
                                        while (ss->next()) {
                                            std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                    ss->at();
                                            if (series == nullptr) continue;
                                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                    series->chain_iterator();
                                            while (it->next()) {
                                                total_samples->fetch_add(1);
                                            }
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, endtime - 3600000, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers3,
                                                    iteration / thread_num, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[cpu-max-all-8] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / iteration << std::endl;

                        output_file << "[cpu-max-all-8] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / iteration << "\n";
                        output_file.flush();
                    }

                    // Aggregate on across both time and host, giving the average of 5 CPU metrics per host per hour for 24 hours
                    if (query_types["double-groupby-5"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int host = 0; host < matchers1->size(); host++) {
                                    for (int j = 0; j < 5; j++) {
//              std::vector<tsdb::label::MatcherInterface*> matchers(
//                  {&matchers1->at(host), &matchers2->at(j)});
                                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                                {std::shared_ptr<label::MatcherInterface>(
                                                        new label::EqualMatcher(matchers1->at(host))),
                                                 std::shared_ptr<label::MatcherInterface>(
                                                         new label::EqualMatcher(matchers2->at(j)))});
                                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                                q->select(matchers);
                                        while (ss->next()) {
                                            std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                    ss->at();
                                            if (series == nullptr) continue;
                                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                    series->chain_iterator();
                                            while (it->next()) {
                                                total_samples->fetch_add(1);
                                            }
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, 0, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers3,
                                                    big_iteration / thread_num / 500, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[double-groupby-5] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / big_iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / big_iteration << std::endl;

                        output_file << "[double-groupby-5] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / big_iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / big_iteration << "\n";

                        output_file.flush();
                    }

                    // Aggregate on across both time and host, giving the average of all (10) CPU metrics per host per hour for 24 hours
                    if (query_types["double-groupby-all"]) {
                        std::atomic<int64_t> total_samples(0);
                        Timer t;
                        t.start();
                        auto func = [](std::vector<tsdb::label::EqualMatcher> *matchers1,
                                       std::vector<tsdb::label::EqualMatcher> *matchers2,
                                       int iteration, std::atomic<int64_t> *total_samples,
                                       base::WaitGroup *_wg, querier::SpanQuerier *q) {
                            for (int round = 0; round < iteration; round++) {
                                for (int host = 0; host < matchers1->size(); host++) {
                                    for (int j = 0; j < matchers2->size(); j++) {
//              std::vector<tsdb::label::MatcherInterface*> matchers(
//                  {&matchers1->at(host), &matchers2->at(j)});
                                        std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers(
                                                {std::shared_ptr<label::MatcherInterface>(
                                                        new label::EqualMatcher(matchers1->at(host))),
                                                 std::shared_ptr<label::MatcherInterface>(
                                                         new label::EqualMatcher(matchers2->at(j)))});
                                        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                                                q->select(matchers);
                                        while (ss->next()) {
                                            std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                                                    ss->at();
                                            if (series == nullptr) continue;
                                            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                                                    series->chain_iterator();
                                            while (it->next()) {
                                                total_samples->fetch_add(1);
                                            }
                                        }
                                    }
                                }
                            }
                            _wg->done();
                        };
                        std::vector<querier::SpanQuerier> queriers;
                        queriers.reserve(thread_num);
                        for (int i = 0; i < thread_num; i++) {
                            wg.add(1);
                            queriers.emplace_back(db, head_, tree_series_, 0, endtime,
                                                  cache);
                            pool->enqueue(std::bind(func, &matchers1, &matchers3,
                                                    big_iteration / thread_num / 500, &total_samples, &wg,
                                                    &queriers[i]));
                        }
                        wg.wait();
                        std::cout << "[double-groupby-all] duration(total):" << t.since_start_nano() / 1000
                                  << "us "
                                  << "duration(avg):" << t.since_start_nano() / 1000 / big_iteration
                                  << "us "
                                  << "samples:" << total_samples.load() / big_iteration << std::endl;

                        output_file << "[double-groupby-all] duration(total):" << t.since_start_nano() / 1000
                                    << "us "
                                    << "duration(avg):" << t.since_start_nano() / 1000 / big_iteration
                                    << "us "
                                    << "samples:" << total_samples.load() / big_iteration << "\n";
                        output_file.flush();
                    }
                    output_file << "\n";
                    output_file.flush();
                }
            }

            leveldb::Status setup() {
                std::string config_file_path = "../config.yaml";
                YAML::Node cfg = YAML::LoadFile(config_file_path);
                std::string sep_db_path = "/tmp/sep_db";
                std::string db_path = cfg["db_path"].as<std::string>();
                std::string log_path = cfg["log_path"].as<std::string>();
                head::wal_num = cfg["wal_num"].as<int>();

                config::tree_series_thread_pool_size = cfg["tree_series_thread_pool_size"].as<int>();
                config::max_slab_memory = cfg["max_slab_memory"].as<int>();
                config::read_buffer_size = cfg["read_buffer_size"].as<int>();
                config::max_series_num = cfg["max_series_num"].as<int>();
                config::slab_item_size = cfg["slab_item_size"].as<int>();
                config::chunk_size = cfg["chunk_size"].as<int>();
                config::slab_size = cfg["slab_size"].as<int>();

                std::string tree_series_path = cfg["tree_series_path"].as<std::string>();
                int fd = ::open(tree_series_path.c_str(), O_WRONLY | O_CREAT, 0644);
                slab::Setting *setting = new slab::Setting();
                setting->ssd_device_ = tree_series_path.data();
                std::string info_path = cfg["tree_series_info_path"].as<std::string>();
                int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
                setting->ssd_slab_info_ = info_path.data();
                tree_series_ = new slab::ValueLog(*setting);

                boost::filesystem::remove_all(sep_db_path);
                boost::filesystem::remove_all(db_path);
                boost::filesystem::remove_all(log_path);

                head_ = new head::SpanHead(sep_db_path, db_path, log_path,"",tree_series_);
                return leveldb::Status();
            }

            leveldb::Status setup(const std::string& sep_db_path, const std::string& db_path, const std::string& log_path) {

                std::string  path = "/home/dell/project/SSD/tree_series_test";
                int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
                slab::Setting *setting = new slab::Setting();
                setting->ssd_device_ = "/home/dell/project/SSD/tree_series_test";
                std::string info_path = "/home/dell/project/SSD/tree_series_info_test";
                int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
                setting->ssd_slab_info_ = "/home/dell/project/SSD/tree_series_info_test";
                tree_series_ = new slab::ValueLog(*setting);

                boost::filesystem::remove_all(sep_db_path);
                boost::filesystem::remove_all(db_path);
                boost::filesystem::remove_all(log_path);

                head_ = new head::SpanHead(sep_db_path, db_path, log_path,"",tree_series_);
                db_ = head_->get_second_db();
                return leveldb::Status();
            }

            int num_ts;
            int tuple_size;
            int num_tuple;
//            std::unique_ptr<::tsdb::head::Head> head_;

            slab::ValueLog* tree_series_;
//            head::TreeHead* head_;
            head::SpanHead* head_;
            leveldb::DB* db_;

            std::vector<tsdb::label::EqualMatcher> matchers1;
            std::vector<tsdb::label::EqualMatcher> matchers2;
            std::vector<tsdb::label::EqualMatcher> matchers3;
        };

        void mem_usage(double& vm_usage, double& resident_set) {
            vm_usage = 0.0;
            resident_set = 0.0;
            std::ifstream stat_stream("/proc/self/stat",
                                      std::ios_base::in);  // get info from proc directory
            // create some variables to get info
            std::string pid, comm, state, ppid, pgrp, session, tty_nr;
            std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
            std::string utime, stime, cutime, cstime, priority, nice;
            std::string O, itrealvalue, starttime;
            unsigned long vsize;
            long rss;
            stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >>
                        tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >>
                        stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
                        starttime >> vsize >> rss;  // don't care about the rest
            stat_stream.close();
            long page_size_kb = sysconf(_SC_PAGE_SIZE) /
                                1024;  // for x86-64 is configured to use 2MB pages
            vm_usage = vsize / 1024.0;
            resident_set = rss * page_size_kb;
        }

        void TestParallelInsertAndQuery(uint64_t thread_num, uint64_t num, uint64_t interval = 30,
                                        uint64_t numtuple = 90, uint64_t query_thread_num = 0) {
            printf("TestParallelInsertAndQuery200\n");

            bool bg = true;
            size_t sz = sizeof(bg);
            mallctl("opt.background_thread", NULL, 0, &bg, sz);
            ssize_t t = 0;
            sz = sizeof(t);
            mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
            mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

            // std::string sep_db_path = "/mnt/nvme1/sep_db";
            // std::string db_path = "/mnt/HDD/tsdb_big";
            // std::string log_path = "/mnt/nvme1/tsdb_big";
            std::string sep_db_path = "/tmp/sep_db";
            std::string db_path = "/tmp/tsdb_big";
            std::string log_path = "/tmp/tsdb_big_log";

//            boost::filesystem::remove_all(dbpath);
//            boost::filesystem::remove_all(log_path);

            TSDBTest tsdbtest;
            tsdbtest.set_parameters(num, 32, numtuple);
//            tsdbtest.setup(dbpath, log_path);

            tsdbtest.setup(sep_db_path, db_path, log_path);

//            std::atomic<bool> running = true;
//            auto stat_thread = tsdb::testutil::StartIostat("/mnt/nvme/iostat_output.txt", running);

            Thread_Pool pool(thread_num >= query_thread_num ? thread_num : query_thread_num);
            base::WaitGroup wg;
            std::vector<std::unique_ptr<db::AppenderInterface>> apps;
            apps.reserve(thread_num);

            std::vector<label::Labels> lsets;
            lsets.reserve(num);
//            tsdbtest.get_devops_labels(&lsets);
            testutil::get_devops_labels(num, &lsets);

            tsdbtest.head_->enable_concurrency();

            tsdbtest.head_->bg_flush_data();


            leveldb::Options ms_opts;
            ms_opts.create_if_missing = true;
            ms_opts.use_log = false;
            tsdbtest.head_->set_mergeset_manager(db_path + "/mergeset", ms_opts);

            int64_t last_t = 0, d;
            double vm, rss;
            mem_usage(vm, rss);
            std::cout << "Virtual Memory: " << (vm / 1024)
                      << "MB\nResident set size: " << (rss / 1024) << "MB\n"
                      << std::endl;
            Timer timer;
            timer.start();

            auto func = [](db::AppenderInterface* app,
                           std::vector<tsdb::label::Labels>* _lsets, uint64_t left, uint64_t right,
                           base::WaitGroup* _wg, uint64_t num_tuple, uint64_t tuple_size,
                           uint64_t interval, Timer* timer, uint64_t tid) {
                std::vector<uint64_t> tsids;
                std::vector<uint64_t> sgids;
                std::vector<uint16_t >mids;
                tsids.reserve(right - left);

                std::vector<int64_t> tfluc;
                for (uint64_t j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
//                int64_t ts = 0;
//                for (uint64_t j = 0; j < tuple_size; j++) {
//                    ts = j;
//                    tfluc.push_back(ts);
//                }
                uint64_t sgid;
                for (uint64_t i = left; i < right; i++) {
                    auto r = app->add(std::move(_lsets->at(i)), tfluc[0], tfluc[0],sgid, 0);
                    for (uint64_t k = 1; k < tuple_size; k++){
                        app->add_fast(sgid, k * interval * 1000 + tfluc[k], tfluc[k]);
                    }

                    app->commit();
                    sgids.push_back(sgid);
                }

                int64_t d = timer->since_start_nano(), last_t = 0;
                printf(
                        "[thread id]:%d [Labels Insertion duration (ms)]:%ld [throughput]:%f\n",
                        tid, d / 1000000,
                        (double)(right - left) * (double)(tuple_size) / (double)(d)*1000000000);

                for (uint64_t tuple = 1; tuple < num_tuple; tuple++) {
                    tfluc.clear();
                    for (uint64_t j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
//                    for (uint64_t j = 0; j < tuple_size; j++) {
//                        ts = j;
//                        tfluc.push_back(ts);
//                    }
                    int64_t st = tuple * tuple_size * 1000 * interval;

                    for (uint64_t i = 0; i < right - left; i++) {
                        for (uint64_t k = 0; k < tuple_size; k++) {
                            app->add_fast(sgids[i], st + k * interval * 1000 + tfluc[k], tfluc[k]);
                        }

                        app->commit();
                    }

                    if ((tuple + 1) % 15 == 0) {
                        d = timer->since_start_nano();
                        printf(
                                "[thread id]:%d [#tuples]:%d [st]:%d [Insertion duration (ms)]:%ld "
                                "[throughput]:%f\n",
                                tid, tuple + 1, tuple * tuple_size * 1000, (d - last_t) / 1000000,
                                (double)(right - left) * (double)(15) * (double)(tuple_size) /
                                (double)(d - last_t) * 1000000000);
                        last_t = d;
                    }
                }

                _wg->done();
            };

            for (uint64_t i = 0; i < thread_num; i++) {
                wg.add(1);
                apps.push_back(std::move(tsdbtest.head_->appender()));
                pool.enqueue(std::bind(
                        func, apps.back().get(), &lsets, i * lsets.size() / thread_num,
                        (i + 1) * lsets.size() / thread_num, &wg, tsdbtest.num_tuple,
                        tsdbtest.tuple_size, interval, &timer, i));
            }
            wg.wait();

            d = timer.since_start_nano();
            std::cout << "[TestParallelInsertAndQuery:Total Insertion duration (ms)]:" << (d / 1000000)
                      << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
                      << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                         (double)(tsdbtest.tuple_size)
                      << " [throughput]:"
                      << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                          (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
                      << std::endl;

//            sleep(60);
//            tsdbtest.head_->set_mem_to_disk_migration_threshold(2lu * 1024lu * 1024lu *
//                                                                1024lu);
//            tsdbtest.head_->enable_migration();
//            sleep(60);
//            tsdbtest.head_->disable_migration();
            // tsdbtest.head_->full_migrate();
            sleep(10);
            tsdbtest.head_->stop_bg_flush_data();
            tsdbtest.head_->stop_bg_wal();
            sleep(10);
//            tsdbtest.db_->PrintLevel();

            tsdbtest.head_->migrate_inverted_index(1);
            tsdbtest.head_->migrate_to_flat_forward_index(tsdbtest.head_->get_last_source_group_id(),tsdbtest.head_->get_last_metric_id());
            double vm2, rss2;
            mem_usage(vm2, rss2);
            std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
                      << std::endl;
            std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
                      << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;
            std::cout << "inverted index size:"
                      << tsdbtest.head_->inverted_index()->mem_postings()->mem_size()
                      << std::endl;

            std::remove("parallel_query_latency.log");

            uint64_t epoch = 1;
            sz = sizeof(epoch);
            mallctl("epoch", &epoch, &sz, &epoch, sz);

            size_t allocated, active, mapped;
            sz = sizeof(size_t);
            mallctl("stats.allocated", &allocated, &sz, NULL, 0);
            mallctl("stats.active", &active, &sz, NULL, 0);
            mallctl("stats.mapped", &mapped, &sz, NULL, 0);
            printf("allocated/active/mapped (MB): %zu/%zu/%zu\n", allocated/1024/1024, active/1024/1024, mapped/1024/1024);

            uint64_t end_time = (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
                                (tsdbtest.tuple_size - 1) * 1000 * interval;

            // todo query
            leveldb::Cache* cache = nullptr;
            if (query_thread_num == 0)
                tsdbtest.queryDevOps2(
                        tsdbtest.db_,
                        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
                        (tsdbtest.tuple_size - 1) * 1000 * interval,
                        cache);
            else
                tsdbtest.queryDevOps2Parallel(
                        &pool, query_thread_num, tsdbtest.db_,
                        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
                        (tsdbtest.tuple_size - 1) * 1000 * interval,
                        cache);
            sleep(3);
            if (query_thread_num == 0)
                tsdbtest.queryDevOps2(
                        tsdbtest.db_,
                        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
                        (tsdbtest.tuple_size - 1) * 1000 * interval,
                        cache);
            else
                tsdbtest.queryDevOps2Parallel(
                        &pool, query_thread_num, tsdbtest.db_,
                        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
                        (tsdbtest.tuple_size - 1) * 1000 * interval,
                        cache);
            sleep(3);
            if (query_thread_num == 0)
                tsdbtest.queryDevOps2(
                        tsdbtest.db_,
                        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
                        (tsdbtest.tuple_size - 1) * 1000 * interval,
                        cache);
            else
                tsdbtest.queryDevOps2Parallel(
                        &pool, query_thread_num, tsdbtest.db_,
                        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
                        (tsdbtest.tuple_size - 1) * 1000 * interval,
                        cache);
            // ProfilerStop();

            std::cout<<"\n\ntotal write bytes: "<<slab::slab_total_write_bytes<<std::endl;

//            tsdb::testutil::StopIostat(stat_thread, running);
        }

    }  // namespace db
}  // namespace tsdb

int main(int argc, char* argv[]) {
    if (argc == 5){
        tsdb::db::TestParallelInsertAndQuery(
                std::stoi(argv[1]), std::stoi(argv[2]), std::stoi(argv[3]),
                std::stoi(argv[4]));
    } else if (argc == 6) {
        tsdb::db::TestParallelInsertAndQuery(
                std::stoi(argv[1]), std::stoi(argv[2]), std::stoi(argv[3]),
                std::stoi(argv[4]), std::stoi(argv[5]));
    }
}