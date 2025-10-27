#ifndef LABEL_GENERATOR_H
#define LABEL_GENERATOR_H

#include "leveldb/db.h"
#include "head/TreeHead.h"

namespace tsdb::testutil {
    std::vector<std::vector<std::string>> devops({
        {"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait", "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice"},
        {"reads", "writes", "read_bytes", "write_bytes", "read_time", "write_time", "io_time"},
        {"total", "free", "used", "used_percent", "inodes_total", "inodes_free", "inodes_used"},
        {"boot_time", "interrupts", "context_switches", "processes_forked", "disk_pages_in", "disk_pages_out"},
        {"total", "available", "used", "free", "cached", "buffered", "used_percent", "available_percent", "buffered_percent"},
        {"bytes_sent", "bytes_recv", "packets_sent", "packets_recv", "err_in", "err_out", "drop_in", "drop_out"},
//        {"accepts", "active", "handled", "reading", "requests", "waiting", "writing"},
        {"accepts", "active", "handled", "reading", "requests", "waiting"},
        {"numbackends", "xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
                     "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks", "blk_read_time", "blk_write_time"},
        {"uptime_in_seconds", "total_connections_received", "expired_keys", "evicted_keys", "keyspace_hits",
                     "keyspace_misses", "instantaneous_ops_per_sec", "instantaneous_input_kbps", "instantaneous_output_kbps",
                     "connected_clients", "used_memory", "used_memory_rss", "used_memory_peak", "used_memory_lua",
                     "rdb_changes_since_last_save", "sync_full", "sync_partial_ok", "sync_partial_err", "pubsub_channels",
                     "pubsub_patterns", "latest_fork_usec", "connected_slaves", "master_repl_offset", "repl_backlog_active",
                     "repl_backlog_size", "repl_backlog_histlen", "mem_fragmentation_ratio",
                     "used_cpu_sys", "used_cpu_user", "used_cpu_sys_children", "used_cpu_user_children"}
    });

    std::vector<std::string> devops_names({
        "cpu_", "diskio_", "disk_", "kernel_", "mem_", "net_", "nginx_", "postgres_", "redis_"
    });

    int load_devops(uint64_t num_lines, const std::string& filename, std::vector<label::Labels>* lsets, uint64_t ts_counter) {
//        std::ifstream file(filename);
        std::string line;
        uint64_t cur_line = 0;

//        uint64_t num = 0;
//        for (uint64_t i = 0; i < devops.size(); i++) {
//            num += devops[i].size();
//        }
//        std::cout<<num<<std::endl;
//        while (getline(file, line)) {
//            if (line.find("host_12345") != std::string::npos) {
//                std::cout<<line<<std::endl;
//            }
//        }

        std::vector<std::string> items, names, values;
        for (size_t round = 0; round < devops_names.size(); round++) {
            std::ifstream file(filename);
            line.clear();
            while (cur_line < num_lines) {
                getline(file, line);
//                std::cout<<line<<std::endl;

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
//            for (int i = 0; i < 100000 - cur_line; i++) {
//                getline(file, line);
////                std::cout<<line<<std::endl;
//            }
            cur_line = 0;
        }
        return ts_counter;
    }

    void get_devops_labels(uint64_t num_ts, std::vector<label::Labels>* lsets) {
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

}

#endif
