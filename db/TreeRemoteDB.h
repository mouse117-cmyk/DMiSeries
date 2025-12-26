#ifndef TREEREMOTEDB_H
#define TREEREMOTEDB_H

#include "RemoteDB.h"
#include "head/TreeHead.h"
#include "querier/tsdb_tree_querier.h"
#include "third_party/httplib.h"
#include "protobuf/types.pb.h"
#include "protobuf/remote.pb.h"
//#include "ValueLog/ThreadPool.h"

namespace tsdb {
    namespace db {

        class TreeRemoteDB {
        private:
            std::string dir_;
            leveldb::DB *db_;
            head::TreeHead* tree_head_;
            slab::ValueLog* value_log_;
            httplib::Server server_;
            error::Error err_;

            // Used for HTTP requests.
            querier::TreeQuerier *cached_querier_;

            Thread_Pool pool_;

            void init_http_proto_server();

            leveldb::Status setup(const std::string & dir, const std::string  & log_path);

            void multi_add(db::AppenderInterface* appender, prometheus::WriteRequest* writeRequest, uint64_t left, uint64_t right, base::WaitGroup* _wg);
            void HandleInsert(const httplib::Request &req, httplib::Response &resp);

            void HandleQuery(const httplib::Request &req, httplib::Response &resp);

        public:
            TreeRemoteDB(const std::string &dir, leveldb::DB *db, head::TreeHead *tree_head, slab::ValueLog * tree_series);
            TreeRemoteDB(const std::string &dir, const std::string &log_path);

            std::string dir() { return dir_; }

            head::TreeHead *head() { return tree_head_; }
            leveldb::DB* db() { return db_; }
            slab::ValueLog* tree_series() { return value_log_; }

            error::Error error() { return err_; }

            std::unique_ptr<db::AppenderInterface> appender() {
                return tree_head_->appender();
            }

            querier::TreeQuerier *querier(int64_t mint, int64_t maxt) {
                querier::TreeQuerier *q = new querier::TreeQuerier(db_, tree_head_, value_log_, mint, maxt);
                return q;
            }

            void print_level(bool hex = false, bool print_stats = false) {
                db_->PrintLevel(hex, print_stats);
            }

            ~TreeRemoteDB();
        };

    }
}
#endif //TREEREMOTEDB_H
