#include "db/RemoteDB.h"
#include "label/EqualMatcher.hpp"

namespace tsdb {
    namespace db {

        RemoteDB::RemoteDB(const std::string& dir, leveldb::DB* db)
                : dir_(dir),
                  db_(db),
                  head_(new head::Head(dir, "", db, true)),
                  pool_(32),
                  cached_querier_(nullptr) {
            init_http_proto_server();
        }

        RemoteDB::~RemoteDB() {
            delete cached_querier_;
//            delete db_;
            server_.stop();
        }

        void RemoteDB::multi_add(db::AppenderInterface* appender, prometheus::WriteRequest* writeRequest, uint64_t left, uint64_t right, base::WaitGroup* _wg) {
            for (uint64_t i = left; i < right; i++) {
                auto ts = writeRequest->timeseries(i);
                label::Labels label_set;
                for (auto& lb : ts.labels()) {
                    label_set.emplace_back(lb.name(), lb.value());
                }
                for (auto& sample : ts.samples()) {
                    appender->add(label_set, sample.timestamp(), sample.value());
                }
            }
            appender->commit();
            _wg->done();
        }

        void RemoteDB::handleInsert(const httplib::Request &req,httplib::Response &resp) {
            if (!this->cached_appender_) {
                this->cached_appender_ = this->appender();
            }

            std::string data;
            snappy::Uncompress(req.body.data(), req.body.size(), &data);
            prometheus::WriteRequest write_requset;
            write_requset.ParseFromString(data);

            uint64_t batch_size = write_requset.timeseries_size() / 32;
//            uint64_t batch_size = 10000;
            std::vector<std::unique_ptr<db::AppenderInterface>> apps;
            base::WaitGroup wg;

            for (uint64_t i = 0; i < write_requset.timeseries_size(); i+=batch_size) {
                wg.add(1);
                apps.push_back(std::move(head_->appender()));
                auto right = std::min(i+batch_size, uint64_t(write_requset.timeseries_size()));
                AppenderInterface* app = apps.back().get();
                pool_.enqueue([this, app, &write_requset, i, right, &wg]{
                    return multi_add(app, &write_requset, i, right, &wg);
                });
            }
            wg.wait();

//            int ts_cnt = 0, smp_cnt = 0;
//            for (auto& ts : write_requset.timeseries()) {
//                ts_cnt++;
//                label::Labels label_set;
//                for (auto& lb : ts.labels()) {
////                    label::lbs_add(label_set, label::Label(lb.name(), lb.value()));
//                    label_set.emplace_back(lb.name(), lb.value());
//                }
//                for (auto& sample : ts.samples()) {
//                    smp_cnt++;
//                    this->cached_appender_->add(label_set, sample.timestamp(), sample.value());
////                    std::cout<<sample.timestamp()<<" "<<sample.value()<<std::endl;
//                }
//            }
//            this->cached_appender_->commit();
//            std::cout<<"Insert "<<ts_cnt<<" timeseries, "<<smp_cnt<<" samples"<<std::endl;

            std::cout<<"Insert "<<write_requset.timeseries().size()<<" timeseries, "
                     <<write_requset.timeseries().size()*write_requset.timeseries(0).samples().size()<<" samples"
                     <<std::endl;

            data.clear();
            resp.set_content(data, "text/plain");
        }

        void RemoteDB::handleQuery(const httplib::Request &req, httplib::Response &resp) {
            std::string data;
            snappy::Uncompress(req.body.data(), req.body.size(), &data);
            prometheus::ReadRequest read_request;
            read_request.ParseFromString(data);

            prometheus::ReadResponse read_resp;
            for (auto& qry : read_request.queries()) {
                if (!this->cached_querier_) {
                    this->cached_querier_ = this->querier(qry.start_timestamp_ms(), qry.end_timestamp_ms());
                }
                if (this->cached_querier_->mint() != qry.start_timestamp_ms() || this->cached_querier_->maxt() != qry.end_timestamp_ms()) {
                    delete this->cached_querier_;
                    this->cached_querier_ = querier(qry.start_timestamp_ms(), qry.end_timestamp_ms());
                }

                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers;
                for (auto& matcher : qry.matchers()) {
                    switch (matcher.type()) {
                        case prometheus::LabelMatcher_Type_EQ:
                            matchers.emplace_back(std::shared_ptr<tsdb::label::MatcherInterface>(new tsdb::label::EqualMatcher(matcher.name(), matcher.value())));
                            break;
                        case prometheus::LabelMatcher_Type_NEQ:
                            std::cout<<"not support matcher type NEQ"<<std::endl;
                            break;
                        case prometheus::LabelMatcher_Type_RE:
                            std::cout<<"not support matcher type RE"<<std::endl;
                            break;
                        case prometheus::LabelMatcher_Type_NRE:
                            std::cout<<"not support matcher type NRE"<<std::endl;
                            break;
                        default:
                            std::cout<<"illegal matcher type"<<std::endl;
                    }
//                    std::cout<<matcher.name()<<" "<<matcher.value()<<" ";
                }
//                std::cout<<std::endl;

                prometheus::QueryResult* query_result = read_resp.add_results();
                std::unique_ptr<tsdb::querier::SeriesSetInterface> series_set = this->cached_querier_->select(matchers);
                while (series_set->next()) {
                    prometheus::TimeSeries* timeseries = query_result->add_timeseries();
                    std::unique_ptr<tsdb::querier::SeriesInterface> series = series_set->at();

                    tsdb::label::Labels series_labels = series->labels();
                    for (auto& lb : series_labels) {
                        prometheus::Label* label = timeseries->add_labels();
                        label->set_name(lb.label);
                        label->set_value(lb.value);
                    }

                    std::unique_ptr<tsdb::querier::SeriesIteratorInterface> series_iter = series->iterator();
                    while (series_iter->next()) {
                        prometheus::Sample* sample = timeseries->add_samples();
                        sample->set_timestamp(series_iter->at().first);
                        sample->set_value(series_iter->at().second);
                    }
                }
            }
            std::string resp_data, compressed_data;
            read_resp.SerializeToString(&resp_data);
            snappy::Compress(resp_data.data(), resp_data.size(), &compressed_data);
            resp.set_content(compressed_data, "text/plain");
        }

        void RemoteDB::init_http_proto_server() {
            server_.Post("/insert", [this](const httplib::Request& req, httplib::Response& resp) {
                this->handleInsert(req, resp);
            });

            server_.Post("/query", [this](const httplib::Request& req, httplib::Response& resp) {
                this->handleQuery(req, resp);
            });

            std::thread t([this]() { this->server_.listen(prom_host, prom_port); });
            t.detach();
        }


        void AddLabels(prometheus::TimeSeries* timeSeries, const label::Labels& labels) {
            for (auto& lbl : labels) {
                prometheus::Label* label = timeSeries->add_labels();
                label->set_name(lbl.label);
                label->set_value(lbl.value);
            }
        }

        void AddSample(prometheus::TimeSeries* timeSeries, int64_t timestamp, double value) {
            prometheus::Sample* sample = timeSeries->add_samples();
            sample->set_timestamp(timestamp);
            sample->set_value(value);
        }

        void AddSamples(prometheus::TimeSeries* timeSeries, std::unique_ptr<tsdb::querier::SeriesIteratorInterface> seriesIterator) {
            while (seriesIterator->next()) {
                AddSample(timeSeries, seriesIterator->at().first, seriesIterator->at().second);
            }
        }

        void AddSeries(prometheus::TimeSeries* timeSeries, std::unique_ptr<tsdb::querier::SeriesInterface> series) {
            AddLabels(timeSeries, series->labels());
            AddSamples(timeSeries, series->iterator());
        }

        void AddSeriesSet(prometheus::QueryResult* queryResult, tsdb::querier::SeriesSetInterface* seriesSet) {
            while (seriesSet->next()) {
                prometheus::TimeSeries* timeSeries = queryResult->add_timeseries();
                AddSeries(timeSeries, seriesSet->at());
            }
        }

        void AddQueryResult(prometheus::ReadResponse* readResponse, tsdb::querier::SeriesSetInterface* seriesSet) {
            prometheus::QueryResult* queryResult = readResponse->add_results();
            AddSeriesSet(queryResult, seriesSet);
        }
    }
}
