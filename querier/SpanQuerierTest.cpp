#define GLOBAL_VALUE_DEFINE

#include "SpanQuerier.h"
#include "label/EqualMatcher.hpp"

namespace tsdb::querier {
    class TreeQuerierTest : public testing::Test {
    public:
        void setup() {

            // std::string sep_db_path = "/mnt/nvme1/sep_db";
            // std::string db_path = "/mnt/HDD/tsdb_big";
            // std::string log_path = "/mnt/nvme1/tsdb_big";
            std::string sep_db_path = "/tmp/sep_db";
            std::string db_path = "/tmp/tsdb_big";
            std::string log_path = "/tmp/tsdb_big_log";

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
        }

        std::pair<uint16_t , uint64_t> AddSamples(int64_t num) {
            label::Labels  lbs;
            lbs.emplace_back("__name__", "cpu");
            lbs.emplace_back("label_1", "value_1");
            lbs.emplace_back("label_2", "value_2");

            uint16_t mid;
            uint64_t sgid;
            auto app = head_->appender();

            for (int64_t i = 0; i < num; i++) {
                app->add(lbs, i, i, sgid, mid);
            }
            app->commit();


            return std::make_pair(head_->get_last_metric_id(), head_->get_last_source_group_id());
        }

        head::SpanHead* head_;
        leveldb::DB* db_;
        slab::ValueLog* tree_series_;
    };

    TEST_F(TreeQuerierTest, Test1) {
        setup();

        uint64_t num = 10000;
        AddSamples(num);

        uint64_t min_time = 1000;
        uint64_t max_time = 10000;

        uint64_t idx = min_time;

//        slab
        auto ts_iter = new ValueLogIterator(tree_series_, min_time, max_time, 1, 1);
        while (ts_iter->next()) {
            auto it = ts_iter->at();
            ASSERT_EQ(idx, it.first);
            ASSERT_EQ(idx, it.second);
            idx++;
//            std::cout<<it.first<<" "<<it.second<<std::endl;
        }

//        head chunk
        label::Labels lbs;
        std::string head_chunk;
        head_->series(1, 1, lbs, &head_chunk);

        std::cout<<"******* head chunk *********"<<std::endl;
        auto th_iter = new SpanHeadIterator(head_chunk, min_time, max_time);
        while (th_iter->next()) {
            auto it = th_iter->at();
            ASSERT_EQ(idx, it.first);
            ASSERT_EQ(idx, it.second);
            idx++;
//            std::cout<<it.first<<" "<<it.second<<std::endl;
        }

//        auto head_data = tree_head_->DecodeChunk((uint8_t *)head_chunk.data());
//        for (uint64_t i = 0; i < head_data.first.size(); i++) {
//            ASSERT_EQ(idx, head_data.first.at(i));
//            ASSERT_EQ(idx, head_data.second.at(i));
//            idx++;
//        }

        ASSERT_EQ(idx, max_time);

        std::cout<<label::lbs_string(lbs)<<std::endl;

//        ASSERT_EQ(ts_iter->seek(2000), true);

        delete ts_iter;
    }

    TEST_F(TreeQuerierTest, Test2) {
        setup();

        uint64_t num = 10000;
        AddSamples(num);

        uint64_t min_time = 1000;
        uint64_t max_time = 10000;

        uint32_t sid = 0;
        tree_series_->GetMemSlabID(sid, 1, 1);
        for (uint32_t i = 0; i <= sid; i++) {
//            std::cout
//            <<"sid: "<< i << " start time : " << tree_series_->GetMemSlabInfo(i)->start_time_
//            << " nalloc: " << tree_series_->GetMemSlabInfo(i)->nalloc_
//            << " free: " << tree_series_->GetMemSlabInfo(i)->free_
//            << " mem: " << tree_series_->GetMemSlabInfo(i)->mem_
//            << std::endl;

            if (i != sid) {
                ASSERT_EQ(tree_series_->GetMemSlabInfo(i)->nalloc_, slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE);
            }
        }

        for (uint32_t i = 0; i <= sid; i++) {
            std::string key;
            tree_series_->EnCodeKey(&key, 1, 1, tree_series_->GetMemSlabInfo(i)->end_time_[0]);
            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, leveldb::Slice(reinterpret_cast<char *>(tree_series_->GetMemSlab(i)), slab::SLAB_SIZE));
            ASSERT_EQ(s.ok(), true);
//            std::cout<<sid<<" "<<tree_series_->GetMemSlabInfo(i)->end_time_[0]<<std::endl;
        }

        uint64_t idx = min_time;

//        leveldb memtable
        MemtableIterator* iter = new MemtableIterator(tree_series_, db_->mem(), min_time, max_time, 1, 1);
        while (iter->next()) {
            auto it = iter->at();
            ASSERT_EQ(idx, it.first);
            ASSERT_EQ(idx, it.second);
            idx++;
            // std::cout<<it.first<<" "<<it.second<<std::endl;
        }

//        head chunk
        label::Labels lbs;
        std::string head_chunk;
        head_->series(1, 1, lbs, &head_chunk);

        std::cout<<"******* head chunk *********"<<std::endl;
        auto th_iter = new SpanHeadIterator(head_chunk, min_time, max_time);
        while (th_iter->next()) {
            auto it = th_iter->at();
            ASSERT_EQ(idx, it.first);
            ASSERT_EQ(idx, it.second);
            idx++;
//            std::cout<<it.first<<" "<<it.second<<std::endl;
        }

        ASSERT_EQ(idx, max_time);

        delete iter;
    }

    TEST_F(TreeQuerierTest, Test3) {
        setup();

        uint64_t num = 20000;
        AddSamples(num);

        uint64_t min_time = 1000;
        uint64_t max_time = 20000;

//        uint32_t sid = 0;
//        tree_series_->GetMemSlabID(sid, 1, 1);
//        for (uint32_t i = 0; i < sid; i++) {
//            if (i != sid-1) {
//                ASSERT_EQ(tree_series_->GetMemSlabInfo(i)->nalloc_, slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE);
//            }
//        }
//
//        for (uint32_t i = 0; i < sid; i++) {
//            std::string key;
//            tree_series_->EnCodeKey(&key, 1, 1, tree_series_->GetMemSlabInfo(i)->start_time_);
//            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, leveldb::Slice(reinterpret_cast<char *>(tree_series_->GetMemSlab(i)), slab::SLAB_SIZE));
//            ASSERT_EQ(s.ok(), true);
//        }

        uint64_t idx = min_time;

        SpanQuerier* q = new SpanQuerier(db_, head_ ,tree_series_, min_time, max_time);
        label::Labels  lbs;
        lbs.emplace_back("__name__", "cpu");
        lbs.emplace_back("label_1", "value_1");
        std::vector<std::shared_ptr<label::MatcherInterface>> matchers({
            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[0].label, lbs[0].value)),
            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[1].label, lbs[1].value)),
                    });
        std::unique_ptr<querier::SeriesSetInterface> ss = q->select(matchers);
        while (ss->next()) {
            std::unique_ptr<querier::SeriesInterface> series = ss->at();
            std::unique_ptr<querier::SeriesIteratorInterface> it = series->iterator();
//            std::unique_ptr<querier::SeriesIteratorInterface> it = series->chain_iterator();
            while (it->next()) {
                auto t = it->at();
                ASSERT_EQ(idx, t.first);
                ASSERT_EQ(idx, t.second);
                idx++;
                std::cout<<it->at().first<<" "<<it->at().second<<std::endl;
            }
        }

        ASSERT_EQ(idx, max_time);

        delete q;
    }

    TEST_F(TreeQuerierTest, Test4) {
        setup();

        uint64_t num = 1000000;
        AddSamples(num);

        uint64_t min_time = 1000;
        uint64_t max_time = 10000;

        uint32_t sid = 0;
        tree_series_->GetMemSlabID(sid, 1, 1);
        for (uint32_t i = 0; i < sid; i++) {
            if (i != sid-1) {
                ASSERT_EQ(tree_series_->GetMemSlabInfo(i)->nalloc_, slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE);
            }
        }

        for (uint32_t i = 0; i < sid; i++) {
            std::string key;
            tree_series_->EnCodeKey(&key, 1, 1, tree_series_->GetMemSlabInfo(i)->start_time_[0]);
            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, leveldb::Slice(reinterpret_cast<char *>(tree_series_->GetMemSlab(i)), slab::SLAB_SIZE));
            auto tms = head_->get_from_forward_index(1, 1);
            tms->level_flush_time_ = tree_series_->GetMemSlabInfo(i)->end_time_[0];
            ASSERT_EQ(s.ok(), true);
        }

        sleep(3);

        uint64_t idx = min_time;

        SpanQuerier* q = new SpanQuerier(db_, head_ ,tree_series_, min_time, max_time);
        label::Labels  lbs;
        lbs.emplace_back("__name__", "cpu");
        lbs.emplace_back("label_1", "value_1");
        std::vector<std::shared_ptr<label::MatcherInterface>> matchers({
            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[0].label, lbs[0].value)),
            std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher(lbs[1].label, lbs[1].value)),
                    });
        std::unique_ptr<querier::SeriesSetInterface> ss = q->select(matchers);

        while (ss->next()) {
            std::unique_ptr<querier::SeriesInterface> series = ss->at();
//            std::unique_ptr<querier::SeriesIteratorInterface> it = series->iterator();
            std::unique_ptr<querier::SeriesIteratorInterface> it = series->chain_iterator();
            while (it->next()) {
                auto t = it->at();
//                ASSERT_EQ(idx, t.first);
//                ASSERT_EQ(idx, t.second);
                idx++;
                std::cout<<it->at().first<<" "<<it->at().second<<std::endl;
            }
        }

//        ASSERT_EQ(idx, max_time+1);

        delete q;
    }

}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}