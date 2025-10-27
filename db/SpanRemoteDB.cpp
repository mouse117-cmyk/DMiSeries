#include "db/SpanRemoteDB.h"
#include "label/EqualMatcher.hpp"

namespace tsdb {
    namespace db {

        SpanRemoteDB::SpanRemoteDB(const std::string& sep_db_path, const std::string& db_path, leveldb::DB* sep_db, leveldb::DB* db, head::SpanHead* head, slab::TreeSeries* tree_series)
                : sep_db_path_(sep_db_path),
                  db_path_(db_path),
                  sep_db_(sep_db),
                  db_(db),
                  head_(head),
                  tree_series_(tree_series),
                  cached_querier_(nullptr),
                  pool_(4) {
            init_http_proto_server();
        }

        SpanRemoteDB::SpanRemoteDB(const std::string& sep_db_path, const std::string &db_path, const std::string &log_path)
                :
                  sep_db_path_(sep_db_path),
                  db_path_(db_path),
                  cached_querier_(nullptr),
                  pool_(4) {
            setup(sep_db_path, db_path, log_path);
            init_http_proto_server();
        }

        SpanRemoteDB::SpanRemoteDB(const std::string& sep_db_path, const std::string &db_path, const std::string &log_path,int port,const std::string  & tree_series_path,const std::string  & tree_series_info_path)
            :
              sep_db_path_(sep_db_path),
              db_path_(db_path),
              cached_querier_(nullptr),
              pool_(4) {
              setup(sep_db_path, db_path, log_path,tree_series_path,tree_series_info_path);
              init_http_proto_server(port);
        }

        SpanRemoteDB::~SpanRemoteDB() {
            delete cached_querier_;
//            delete db_;
            delete head_;
            // delete tree_series_;
            server_.stop();
        }

        void SpanRemoteDB::init_http_proto_server() {
            server_.set_read_timeout(1000);
            server_.set_write_timeout(1000);
            server_.set_payload_max_length(1000000000000);
            server_.set_keep_alive_timeout(1000);
            server_.Post("/insert", [this](const httplib::Request& req, httplib::Response& resp) {
                this->HandleInsert(req, resp);
            });

            server_.Post("/query", [this](const httplib::Request& req, httplib::Response& resp) {
                this->HandleQuery(req, resp);
            });

            std::thread t([this]() { this->server_.listen(prom_host, prom_port); });
            t.detach();
        }

        void SpanRemoteDB::init_http_proto_server(int port) {
            server_.set_read_timeout(1000);
            server_.set_write_timeout(1000);
            server_.set_payload_max_length(1000000000000);
            server_.set_keep_alive_timeout(1000);
            server_.Post("/insert", [this](const httplib::Request& req, httplib::Response& resp) {
                this->HandleInsert(req, resp);
            });

            server_.Post("/query", [this](const httplib::Request& req, httplib::Response& resp) {
                this->HandleQuery(req, resp);
            });

            std::thread t([this,port]() { this->server_.listen(prom_host, port); });
            t.detach();
        }

        leveldb::Status SpanRemoteDB::setup(const std::string& sep_db_path, const std::string& dbpath, const std::string& log_path) {
            //=================TreeSeries==========
            std::string  path = "/home/dell/project/SSD/tree_series_test";
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
            slab::Setting *setting = new slab::Setting();
            setting->ssd_device_ = "/home/dell/project/SSD/tree_series_test";
            std::string info_path = "/home/dell/project/SSD/tree_series_info_test";
            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
            setting->ssd_slab_info_ = "/home/dell/project/SSD/tree_series_info_test";
            tree_series_ = new slab::TreeSeries(*setting);

            boost::filesystem::remove_all(sep_db_path);
            boost::filesystem::remove_all(dbpath);
            boost::filesystem::remove_all(log_path);

            head_ = new head::SpanHead(sep_db_path, dbpath, log_path,"",tree_series_);
            db_ = head_->get_second_db();
            return leveldb::Status::OK();
        }

        leveldb::Status SpanRemoteDB::setup(const std::string& sep_db_path, const std::string & dbpath, const std::string  & log_path,const std::string  & tree_series_path,const std::string  & tree_series_info_path) {
            //=================TreeSeries==========
            std::string  path = tree_series_path;
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
            slab::Setting *setting = new slab::Setting();
            setting->ssd_device_ = new char[tree_series_path.length() + 1];
            std::strcpy(setting->ssd_device_, tree_series_path.c_str());
            std::string info_path = tree_series_info_path;
            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
            setting->ssd_slab_info_ = new char[tree_series_info_path.length() + 1];
            std::strcpy(setting->ssd_slab_info_, tree_series_info_path.c_str());
            tree_series_ = new slab::TreeSeries(*setting);

            boost::filesystem::remove_all(sep_db_path);
            boost::filesystem::remove_all(dbpath);
            boost::filesystem::remove_all(log_path);

            head_ = new head::SpanHead(sep_db_path, dbpath, log_path,"",tree_series_);
            db_ = head_->get_second_db();
            return leveldb::Status::OK();
        }

        void SpanRemoteDB::multi_add(db::AppenderInterface* appender, prometheus::WriteRequest* writeRequest, uint64_t left, uint64_t right, base::WaitGroup* _wg) {
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

        void SpanRemoteDB::HandleInsert(const httplib::Request &req,httplib::Response &resp) {
            MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16);
            auto appender = head_->appender();
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

            std::cout<<"Insert "<<write_requset.timeseries_size()<<" timeseries, "
                <<write_requset.timeseries().size()*write_requset.timeseries(0).samples_size()<<" samples"
            <<std::endl;

            data.clear();
            resp.set_content(data, "text/plain");
        }

        void SpanRemoteDB::HandleQuery(const httplib::Request &req, httplib::Response &resp) {
            MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16);
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
                    if (series == nullptr) continue;

                    tsdb::label::Labels series_labels = series->labels();
                    for (auto& lb : series_labels) {
                        prometheus::Label* label = timeseries->add_labels();
                        label->set_name(lb.label);
                        label->set_value(lb.value);
//                        std::cout<<lb.label<<" "<<lb.value<<" ";
                    }
//                    std::cout<<std::endl;

                    std::unique_ptr<tsdb::querier::SeriesIteratorInterface> series_iter = series->chain_iterator();
                    while (series_iter->next()) {
                        prometheus::Sample* sample = timeseries->add_samples();
                        sample->set_timestamp(series_iter->at().first);
                        sample->set_value(series_iter->at().second);

//                        std::cout<<series_iter->at().first<<" "<<series_iter->at().second<<" ";
                    }
//                    std::cout<<std::endl;
                }
            }
            std::string resp_data, compressed_data;
            read_resp.SerializeToString(&resp_data);
            snappy::Compress(resp_data.data(), resp_data.size(), &compressed_data);
            resp.set_content(compressed_data, "text/plain");
        }

    }
}
