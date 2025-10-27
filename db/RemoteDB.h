#ifndef REMOTEDB_H
#define REMOTEDB_H

#include "TreeSeries/ThreadPool.h"
#include "head/Head.hpp"
#include "querier/tsdb_querier.h"
#include "third_party/httplib.h"
#include "protobuf/types.pb.h"
#include "protobuf/remote.pb.h"
#include <snappy.h>


namespace tsdb {
    namespace db {

#define prom_host "192.168.1.102"
#define prom_port 9966

        class RemoteDB {
        private:
            std::string dir_;
            leveldb::DB* db_;
            std::unique_ptr<head::Head> head_;
            httplib::Server server_;
            error::Error err_;

            // Used for HTTP requests.
            std::unique_ptr<db::AppenderInterface> cached_appender_;
            querier::TSDBQuerier* cached_querier_;

            Thread_Pool pool_;

            void init_http_proto_server();

            void multi_add(db::AppenderInterface* appender, prometheus::WriteRequest* writeRequest, uint64_t left, uint64_t right, base::WaitGroup* _wg);
            void handleInsert(const httplib::Request& req, httplib::Response& resp);
            void handleQuery(const httplib::Request&req, httplib::Response& resp);

        public:
            RemoteDB(const std::string& dir, leveldb::DB* db);

            std::string dir() { return dir_; }

            head::Head* head() { return head_.get(); }

            error::Error error() { return err_; }

            std::unique_ptr<db::AppenderInterface> appender() {
                return head_->appender();
            }

            querier::TSDBQuerier* querier(int64_t mint, int64_t maxt) {
                querier::TSDBQuerier* q =
                        new querier::TSDBQuerier(db_, head_.get(), mint, maxt);
                return q;
            }

            void print_level(bool hex = false, bool print_stats = false) {
                db_->PrintLevel(hex, print_stats);
            }

            ~RemoteDB();
        };

        void AddLabels(prometheus::TimeSeries* timeSeries, const label::Labels& labels);
        void AddSample(prometheus::TimeSeries* timeSeries, int64_t timestamp, double value);
        void AddSamples(prometheus::TimeSeries* timeSeries, std::unique_ptr<tsdb::querier::SeriesIteratorInterface> seriesIterator);
        void AddSeries(prometheus::TimeSeries* timeSeries, std::unique_ptr<tsdb::querier::SeriesInterface> series);
        void AddSeriesSet(prometheus::QueryResult* queryResult, tsdb::querier::SeriesSetInterface* seriesSet);
        void AddQueryResult(prometheus::ReadResponse* readResponse, tsdb::querier::SeriesSetInterface* seriesSet);

    };
}

#endif //REMOTEDB_H