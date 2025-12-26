//#include "db/TreeRemoteDB.h"
//#include "label/EqualMatcher.hpp"
//
//namespace tsdb {
//    namespace db {
//
//        TreeRemoteDB::TreeRemoteDB(const std::string& dir, leveldb::DB* db, head::TreeHead* tree_head, slab::ValueLog* tree_series)
//                : dir_(dir),
//                  db_(db),
//                  tree_head_(tree_head),
//                  tree_series_(tree_series),
//                  cached_querier_(nullptr),
//                  pool_(32) {
//            init_http_proto_server();
//        }
//
//        TreeRemoteDB::TreeRemoteDB(const std::string &dir, const std::string &log_path)
//                : dir_(dir),
//                  cached_querier_(nullptr),
//                  pool_(32) {
//            setup(dir, log_path);
//            init_http_proto_server();
//        }
//
//        TreeRemoteDB::~TreeRemoteDB() {
//            delete cached_querier_;
////            delete db_;
//            delete tree_head_;
//            delete tree_series_;
//            server_.stop();
//        }
//
//        void TreeRemoteDB::init_http_proto_server() {
//            server_.set_read_timeout(1000);
//            server_.set_write_timeout(1000);
//            server_.set_payload_max_length(1000000000000);
//            server_.set_keep_alive_timeout(1000);
//            server_.Post("/insert", [this](const httplib::Request& req, httplib::Response& resp) {
//                this->HandleInsert(req, resp);
//            });
//
//            server_.Post("/query", [this](const httplib::Request& req, httplib::Response& resp) {
//                this->HandleQuery(req, resp);
//            });
//
//            std::thread t([this]() { this->server_.listen(prom_host, prom_port); });
//            t.detach();
//        }
//
//        leveldb::Status TreeRemoteDB::setup(const std::string& dbpath, const std::string& log_path) {
//            //=================ValueLog==========
////                std::string  path = "/mnt/nvme/tree_series/tree_series_test";
////                int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
////                slab::Setting *setting = new slab::Setting();
////                setting->ssd_device_ = "/mnt/nvme/tree_series/tree_series_test";
////                std::string info_path = "/mnt/nvme/tree_series/tree_series_info_test";
////                int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
////                setting->ssd_slab_info_ = "/mnt/nvme/tree_series/tree_series_info_test";
////                tree_series_ = new slab::ValueLog(*setting);
//
//            std::string  path = "/home/dell/project/SSD/tree_series_test";
//            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
//            slab::Setting *setting = new slab::Setting();
//            setting->ssd_device_ = "/home/dell/project/SSD/tree_series_test";
//            std::string info_path = "/home/dell/project/SSD/tree_series_info_test";
//            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
//            setting->ssd_slab_info_ = "/home/dell/project/SSD/tree_series_info_test";
//            tree_series_ = new slab::ValueLog(*setting);
//
//            //==========LevelDB============
////                std::string dbpath = "/tmp/tsdb_big";
////                std::string dbpath = "/mnt/HDD/tree_head_test";
//
//            boost::filesystem::remove_all(dbpath);
//            boost::filesystem::remove_all(log_path);
//
//            leveldb::Options options;
//            options.create_if_missing = true;
//            options.max_imm_num = 3;
//            options.write_buffer_size = 4 * 256 * 1024 * 1024;
//            options.max_file_size = 4 * 256 * 1024 * 1024;
//            options.use_log = false;
//            leveldb::Status st = leveldb::DB::Open(options, dbpath, &db_);
//            if (!st.ok())return st;
//
//            boost::filesystem::remove_all(dbpath);
//            tree_head_ = new head::TreeHead(dbpath, log_path,"",db_,tree_series_);
//            db_->SetTreeHead(tree_head_);
//            return st;
//        }
//
//        void TreeRemoteDB::multi_add(db::AppenderInterface* appender, prometheus::WriteRequest* writeRequest, uint64_t left, uint64_t right, base::WaitGroup* _wg) {
//            for (uint64_t i = left; i < right; i++) {
//                auto ts = writeRequest->timeseries(i);
//                label::Labels label_set;
//                for (auto& lb : ts.labels()) {
//                    label_set.emplace_back(lb.name(), lb.value());
//                }
//                for (auto& sample : ts.samples()) {
//                    appender->add(label_set, sample.timestamp(), sample.value());
//                }
//            }
//            appender->commit();
//            _wg->done();
//        }
//
//        void TreeRemoteDB::HandleInsert(const httplib::Request &req,httplib::Response &resp) {
//            MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16);
//            auto appender = tree_head_->appender();
//            std::string data;
//            snappy::Uncompress(req.body.data(), req.body.size(), &data);
//            prometheus::WriteRequest write_requset;
//            write_requset.ParseFromString(data);
//
//            uint64_t batch_size = write_requset.timeseries_size() / 32;
////            uint64_t batch_size = 10000;
//            std::vector<std::unique_ptr<db::AppenderInterface>> apps;
//            base::WaitGroup wg;
//
//            for (uint64_t i = 0; i < write_requset.timeseries_size(); i+=batch_size) {
//                wg.add(1);
//                apps.push_back(std::move(tree_head_->appender()));
//                auto right = std::min(i+batch_size, uint64_t(write_requset.timeseries_size()));
//                AppenderInterface* app = apps.back().get();
//                pool_.enqueue([this, app, &write_requset, i, right, &wg]{
//                    return multi_add(app, &write_requset, i, right, &wg);
//                });
//            }
//            wg.wait();
//
//
////            int ts_cnt = 0, smp_cnt = 0;
////            for (auto& ts : write_requset.timeseries()) {
////                ts_cnt++;
////                label::Labels label_set;
////                for (auto& lb : ts.labels()) {
//////                    label::lbs_add(label_set, label::Label(lb.name(), lb.value()));
////                    label_set.emplace_back(lb.name(), lb.value());
////                }
////                for (auto& sample : ts.samples()) {
////                    smp_cnt++;
////                    appender->add(label_set, sample.timestamp(), sample.value());
//////                    std::cout<<sample.timestamp()<<" "<<sample.value()<<std::endl;
////                }
////            }
////            appender->commit();
//
//            std::cout<<"Insert "<<write_requset.timeseries_size()<<" timeseries, "
//                <<write_requset.timeseries().size()*write_requset.timeseries(0).samples_size()<<" samples"
//            <<std::endl;
//
//            data.clear();
//            resp.set_content(data, "text/plain");
//        }
//
//        void TreeRemoteDB::HandleQuery(const httplib::Request &req, httplib::Response &resp) {
//            MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16);
//            std::string data;
//            snappy::Uncompress(req.body.data(), req.body.size(), &data);
//            prometheus::ReadRequest read_request;
//            read_request.ParseFromString(data);
//
//            prometheus::ReadResponse read_resp;
//            for (auto& qry : read_request.queries()) {
//                if (!this->cached_querier_) {
//                    this->cached_querier_ = this->querier(qry.start_timestamp_ms(), qry.end_timestamp_ms());
//                }
//                if (this->cached_querier_->mint() != qry.start_timestamp_ms() || this->cached_querier_->maxt() != qry.end_timestamp_ms()) {
//                    delete this->cached_querier_;
//                    this->cached_querier_ = querier(qry.start_timestamp_ms(), qry.end_timestamp_ms());
//                }
//
//                std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers;
//                for (auto& matcher : qry.matchers()) {
//                    switch (matcher.type()) {
//                        case prometheus::LabelMatcher_Type_EQ:
//                            matchers.emplace_back(std::shared_ptr<tsdb::label::MatcherInterface>(new tsdb::label::EqualMatcher(matcher.name(), matcher.value())));
//                            break;
//                        case prometheus::LabelMatcher_Type_NEQ:
//                            std::cout<<"not support matcher type NEQ"<<std::endl;
//                            break;
//                        case prometheus::LabelMatcher_Type_RE:
//                            std::cout<<"not support matcher type RE"<<std::endl;
//                            break;
//                        case prometheus::LabelMatcher_Type_NRE:
//                            std::cout<<"not support matcher type NRE"<<std::endl;
//                            break;
//                        default:
//                            std::cout<<"illegal matcher type"<<std::endl;
//                    }
////                    std::cout<<matcher.name()<<" "<<matcher.value()<<" ";
//                }
////                std::cout<<std::endl;
//
//                prometheus::QueryResult* query_result = read_resp.add_results();
//                std::unique_ptr<tsdb::querier::SeriesSetInterface> series_set = this->cached_querier_->select(matchers);
//                while (series_set->next()) {
//                    prometheus::TimeSeries* timeseries = query_result->add_timeseries();
//                    std::unique_ptr<tsdb::querier::SeriesInterface> series = series_set->at();
//                    if (series == nullptr) continue;
//
//                    tsdb::label::Labels series_labels = series->labels();
//                    for (auto& lb : series_labels) {
//                        prometheus::Label* label = timeseries->add_labels();
//                        label->set_name(lb.label);
//                        label->set_value(lb.value);
////                        std::cout<<lb.label<<" "<<lb.value<<" ";
//                    }
////                    std::cout<<std::endl;
//
//                    std::unique_ptr<tsdb::querier::SeriesIteratorInterface> series_iter = series->chain_iterator();
//                    while (series_iter->next()) {
//                        prometheus::Sample* sample = timeseries->add_samples();
//                        sample->set_timestamp(series_iter->at().first);
//                        sample->set_value(series_iter->at().second);
//
////                        std::cout<<series_iter->at().first<<" "<<series_iter->at().second<<" ";
//                    }
////                    std::cout<<std::endl;
//                }
//            }
//            std::string resp_data, compressed_data;
//            read_resp.SerializeToString(&resp_data);
//            snappy::Compress(resp_data.data(), resp_data.size(), &compressed_data);
//            resp.set_content(compressed_data, "text/plain");
//        }
//
//    }
//}