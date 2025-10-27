#ifndef TEMPUSCSTORE_SPANREMOTEDB_H
#define TEMPUSCSTORE_SPANREMOTEDB_H

#include "RemoteDB.h"
#include "head/SpanHead.h"
#include "querier/SpanQuerier.h"
#include "third_party/httplib.h"
#include "protobuf/types.pb.h"
#include "protobuf/remote.pb.h"
#include <snappy.h>
//#include "TreeSeries/ThreadPool.h"

namespace tsdb {
    namespace db {

        class SpanRemoteDB {
        private:
            std::string sep_db_path_;
            std::string db_path_;
            leveldb::DB *sep_db_;
            leveldb::DB *db_;
            head::SpanHead* head_;
            slab::TreeSeries* tree_series_;
            httplib::Server server_;
            error::Error err_;

            // Used for HTTP requests.
            querier::SpanQuerier *cached_querier_;

            Thread_Pool pool_;

            void init_http_proto_server();
            void init_http_proto_server(int port);

            leveldb::Status setup(const std::string& sep_db_path, const std::string & dir, const std::string  & log_path);
            leveldb::Status setup(const std::string& sep_db_path, const std::string & dir, const std::string  & log_path,const std::string  & tree_series_path,const std::string  & tree_series_info_path);

            void multi_add(db::AppenderInterface* appender, prometheus::WriteRequest* writeRequest, uint64_t left, uint64_t right, base::WaitGroup* _wg);
            void HandleInsert(const httplib::Request &req, httplib::Response &resp);

            void HandleQuery(const httplib::Request &req, httplib::Response &resp);

        public:
            SpanRemoteDB(const std::string& sep_db_path, const std::string &dir, leveldb::DB *sep_db, leveldb::DB *db, head::SpanHead *head, slab::TreeSeries * tree_series);
            SpanRemoteDB(const std::string& sep_db_path, const std::string &dir, const std::string &log_path);
            SpanRemoteDB(const std::string& sep_db_path, const std::string &dir, const std::string &log_path,int port,const std::string  & tree_series_path,const std::string  & tree_series_info_path);

            std::string sep_dir() {return sep_db_path_; }
            std::string dir() { return db_path_; }

            head::SpanHead *head() { return head_; }
            leveldb::DB* sep_db() { return sep_db_; }
            leveldb::DB* db() { return db_; }
            slab::TreeSeries* tree_series() { return tree_series_; }

            error::Error error() { return err_; }

            std::unique_ptr<db::AppenderInterface> appender() {
                return head_->appender();
            }

            querier::SpanQuerier *querier(int64_t mint, int64_t maxt) {
                querier::SpanQuerier *q = new querier::SpanQuerier(db_, head_, tree_series_, mint, maxt);
                return q;
            }

            void print_level(bool hex = false, bool print_stats = false) {
                sep_db_->PrintLevel(hex, print_stats);
                db_->PrintLevel(hex, print_stats);
            }

            ~SpanRemoteDB();
        };

    }
}
#endif //TEMPUSCSTORE_SPANREMOTEDB_H