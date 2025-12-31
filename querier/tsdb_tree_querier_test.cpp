//#define GLOBAL_VALUE_DEFINE
//#include "tsdb_tree_querier.h"
//#include "label/EqualMatcher.hpp"
//
//namespace tsdb::querier {
//    class TreeQuerierTest : public testing::Test {
//    public:
//        leveldb::Status setup() {
//            //=================ValueLog==========
//            std::string  path = "/home/dell/project/SSD/value_log_test";
//            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
//            slab::Setting *setting = new slab::Setting();
//            setting->ssd_device_ = "/home/dell/project/SSD/value_log_test";
//            std::string info_path = "/home/dell/project/SSD/value_log_info_test";
//            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
//            setting->ssd_slab_info_ = "/home/dell/project/SSD/value_log_info_test";
//            value_log_ = new slab::ValueLog(*setting);
//
//            //==========LevelDB============
//            std::string hd_path = "/tmp/tree_head_test";
//            leveldb::Options options;
//            options.create_if_missing = true;
//            options.max_imm_num = 3;
//            options.write_buffer_size = 256 * 1024;
//            options.max_file_size = 256 * 1024;
//            options.use_log = false;
//            leveldb::Status st = leveldb::DB::Open(options, hd_path, &db_);
//            if (!st.ok()) return st;
//
//            boost::filesystem::remove_all(hd_path);
//            tree_head_ = new head::TreeHead(hd_path,"",db_,value_log_);
//            db_->SetTreeHead(tree_head_);
//            return st;
//        }
//
//        std::pair<uint16_t , uint64_t> AddSamples(int64_t num) {
//            label::Labels  lbs;
//            lbs.emplace_back("__name__", "cpu");
//            lbs.emplace_back("label_1", "value_1");
//            lbs.emplace_back("label_2", "value_2");
//
//            uint16_t mid;
//            uint64_t sgid;
//            auto app = tree_head_->appender();
//
//            for (int64_t i = 0; i < num; i++) {
//                app->add(lbs, i, i, sgid, mid);
//            }
//            app->commit();
//
//
//            return std::make_pair(tree_head_->get_last_metric_id(), tree_head_->get_last_source_group_id());
//        }
//
//        head::TreeHead* tree_head_;
//        leveldb::DB* db_;
//        slab::ValueLog* value_log_;
//    };
//
//    TEST_F(TreeQuerierTest, Test1) {
//        setup();
//
//        uint64_t num = 10000;
//        AddSamples(num);
//
//        uint64_t min_time = 1000;
//        uint64_t max_time = 10000;
//
//        uint64_t idx = min_time;
//
////        slab
//        auto ts_iter = new ValueLogIterator(value_log_, min_time, max_time, 1, 1);
//        while (ts_iter->next()) {
//            auto it = ts_iter->at();
//            ASSERT_EQ(idx, it.first);
//            ASSERT_EQ(idx, it.second);
//            idx++;
////            std::cout<<it.first<<" "<<it.second<<std::endl;
//        }
//
////        head chunk
//        label::Labels lbs;
//        std::string head_chunk;
//        tree_head_->series(1, 1, lbs, &head_chunk);
//
//        std::cout<<"******* head chunk *********"<<std::endl;
//        auto th_iter = new TreeHeadIterator(head_chunk, min_time, max_time);
//        while (th_iter->next()) {
//            auto it = th_iter->at();
//            ASSERT_EQ(idx, it.first);
//            ASSERT_EQ(idx, it.second);
//            idx++;
////            std::cout<<it.first<<" "<<it.second<<std::endl;
//        }
//
////        auto head_data = tree_head_->DecodeChunk((uint8_t *)head_chunk.data());
////        for (uint64_t i = 0; i < head_data.first.size(); i++) {
////            ASSERT_EQ(idx, head_data.first.at(i));
////            ASSERT_EQ(idx, head_data.second.at(i));
////            idx++;
////        }
//
//        ASSERT_EQ(idx, max_time);
//
//        std::cout<<label::lbs_string(lbs)<<std::endl;
//
//        ASSERT_EQ(ts_iter->seek(2000), true);
//
//        delete ts_iter;
//    }
//
//    TEST_F(TreeQuerierTest, Test2) {
//        setup();
//
//        uint64_t num = 10000;
//        AddSamples(num);
//
//        uint64_t min_time = 1000;
//        uint64_t max_time = 10000;
//
//        uint32_t sid = 0;
//        value_log_->GetMemSlabID(sid, 1, 1);
//        for (uint32_t i = 0; i < sid; i++) {
////            std::cout
////            <<"sid: "<< i << " start time : " << value_log_->GetMemSlabInfo(i)->start_time_
////            << " nalloc: " << value_log_->GetMemSlabInfo(i)->nalloc_
////            << " free: " << value_log_->GetMemSlabInfo(i)->free_
////            << " mem: " << value_log_->GetMemSlabInfo(i)->mem_
////            << std::endl;
//
//            if (i != sid-1) {
//                ASSERT_EQ(value_log_->GetMemSlabInfo(i)->nalloc_, slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE);
//            }
//        }
//
//        for (uint32_t i = 0; i < sid; i++) {
//            std::string key;
//            value_log_->EnCodeKey(&key, 1, 1, value_log_->GetMemSlabInfo(i)->start_time_[0]);
//            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, leveldb::Slice(reinterpret_cast<char *>(value_log_->GetMemSlab(i)), slab::SLAB_SIZE));
//            ASSERT_EQ(s.ok(), true);
//        }
//
//        uint64_t idx = min_time;
//
////        leveldb memtable
//        MemtableIterator* iter = new MemtableIterator(value_log_, db_->mem(), min_time, max_time, 1, 1);
//        while (iter->next()) {
//            auto it = iter->at();
//            ASSERT_EQ(idx, it.first);
//            ASSERT_EQ(idx, it.second);
//            idx++;
////            std::cout<<it.first<<" "<<it.second<<std::endl;
//        }
//
////        head chunk
//        label::Labels lbs;
//        std::string head_chunk;
//        tree_head_->series(1, 1, lbs, &head_chunk);
//
//        std::cout<<"******* head chunk *********"<<std::endl;
//        auto th_iter = new TreeHeadIterator(head_chunk, min_time, max_time);
//        while (th_iter->next()) {
//            auto it = th_iter->at();
//            ASSERT_EQ(idx, it.first);
//            ASSERT_EQ(idx, it.second);
//            idx++;
////            std::cout<<it.first<<" "<<it.second<<std::endl;
//        }
//
//        ASSERT_EQ(idx, max_time);
//
//        delete iter;
//    }
//
//    TEST_F(TreeQuerierTest, Test3) {
//        setup();
//
//        uint64_t num = 10000;
//        AddSamples(num);
//
//        uint64_t min_time = 1000;
//        uint64_t max_time = 10000;
//
////        uint32_t sid = 0;
////        value_log_->GetMemSlabID(sid, 1, 1);
////        for (uint32_t i = 0; i < sid; i++) {
////            if (i != sid-1) {
////                ASSERT_EQ(value_log_->GetMemSlabInfo(i)->nalloc_, slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE);
////            }
////        }
////
////        for (uint32_t i = 0; i < sid; i++) {
////            std::string key;
////            value_log_->EnCodeKey(&key, 1, 1, value_log_->GetMemSlabInfo(i)->start_time_);
////            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, leveldb::Slice(reinterpret_cast<char *>(value_log_->GetMemSlab(i)), slab::SLAB_SIZE));
////            ASSERT_EQ(s.ok(), true);
////        }
//
//        uint64_t idx = min_time;
//
//        TreeQuerier* q = new TreeQuerier(db_, tree_head_ ,value_log_, min_time, max_time);
//        label::Labels  lbs;
//        lbs.emplace_back("__name__", "cpu");
//        lbs.emplace_back("label_1", "value_1");
//        std::vector<std::shared_ptr<label::MatcherInterface>> matchers({
//            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[0].label, lbs[0].value)),
//            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[1].label, lbs[1].value)),
//                    });
//        std::unique_ptr<querier::SeriesSetInterface> ss = q->select(matchers);
//
//        while (ss->next()) {
//            std::unique_ptr<querier::SeriesInterface> series = ss->at();
//            std::unique_ptr<querier::SeriesIteratorInterface> it = series->iterator();
////            std::unique_ptr<querier::SeriesIteratorInterface> it = series->chain_iterator();
//            while (it->next()) {
//                auto t = it->at();
//                ASSERT_EQ(idx, t.first);
//                ASSERT_EQ(idx, t.second);
//                idx++;
////                std::cout<<it->at().first<<" "<<it->at().second<<std::endl;
//            }
//        }
//
//        ASSERT_EQ(idx, max_time);
//
//        delete q;
//    }
//
//    TEST_F(TreeQuerierTest, Test4) {
//        setup();
//
//        uint64_t num = 1000000;
//        AddSamples(num);
//
//        uint64_t min_time = 1000;
//        uint64_t max_time = 10000;
//
//        uint32_t sid = 0;
//        value_log_->GetMemSlabID(sid, 1, 1);
//        for (uint32_t i = 0; i < sid; i++) {
//            if (i != sid-1) {
//                ASSERT_EQ(value_log_->GetMemSlabInfo(i)->nalloc_, slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE);
//            }
//        }
//
//        for (uint32_t i = 0; i < sid; i++) {
//            std::string key;
//            value_log_->EnCodeKey(&key, 1, 1, value_log_->GetMemSlabInfo(i)->start_time_[0]);
//            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, leveldb::Slice(reinterpret_cast<char *>(value_log_->GetMemSlab(i)), slab::SLAB_SIZE));
//            auto tms = tree_head_->get_from_forward_index(1, 1);
//            tms->level_flush_time_ = value_log_->GetMemSlabInfo(i)->end_time_[0];
//            ASSERT_EQ(s.ok(), true);
//        }
//
//        sleep(3);
//
//        uint64_t idx = min_time;
//
//        TreeQuerier* q = new TreeQuerier(db_, tree_head_ ,value_log_, min_time, max_time);
//        label::Labels  lbs;
//        lbs.emplace_back("__name__", "cpu");
//        lbs.emplace_back("label_1", "value_1");
//        std::vector<std::shared_ptr<label::MatcherInterface>> matchers({
//            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[0].label, lbs[0].value)),
//            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[1].label, lbs[1].value)),
//                    });
//        std::unique_ptr<querier::SeriesSetInterface> ss = q->select(matchers);
//
//        while (ss->next()) {
//            std::unique_ptr<querier::SeriesInterface> series = ss->at();
////            std::unique_ptr<querier::SeriesIteratorInterface> it = series->iterator();
//            std::unique_ptr<querier::SeriesIteratorInterface> it = series->chain_iterator();
//            while (it->next()) {
//                auto t = it->at();
////                ASSERT_EQ(idx, t.first);
////                ASSERT_EQ(idx, t.second);
//                idx++;
//                std::cout<<it->at().first<<" "<<it->at().second<<std::endl;
//            }
//        }
//
////        ASSERT_EQ(idx, max_time+1);
//
//        delete q;
//    }
//
//}
//
//int main(int argc, char** argv) {
//    testing::InitGoogleTest(&argc, argv);
//    return RUN_ALL_TESTS();
//}