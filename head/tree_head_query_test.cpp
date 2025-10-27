#define GLOBAL_VALUE_DEFINE

#include <gtest/gtest.h>
#include "TreeHead.h"
#include "label/EqualMatcher.hpp"

namespace tsdb::head {

    class TreeHeadQueryTest : public testing::Test {
    public:
        TreeHead* tree_head_;
        leveldb::DB* db_;
        slab::TreeSeries* tree_series_;

        leveldb::Status setup() {
            //=================TreeSeries==========
            std::string  path = "/home/dell/project/SSD/tree_series_test";
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
            slab::Setting *setting = new slab::Setting();
            setting->ssd_device_ = "/home/dell/project/SSD/tree_series_test";
            std::string info_path = "/home/dell/project/SSD/tree_series_info_test";
            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
            setting->ssd_slab_info_ = "/home/dell/project/SSD/tree_series_info_test";
            tree_series_ = new slab::TreeSeries(*setting);

            //==========LevelDB============
            std::string hd_path = "/tmp/tree_head_test";
            leveldb::Options options;
            options.create_if_missing = true;
            options.max_imm_num = 3;
            options.write_buffer_size = 256 * 1024 * 1024;
            options.max_file_size = 256 * 1024 * 1024;
            options.use_log = false;
            leveldb::Status st = leveldb::DB::Open(options, hd_path, &db_);
            if (!st.ok()) return st;

            boost::filesystem::remove_all(hd_path);
            tree_head_ = new TreeHead(hd_path,"",db_,tree_series_);
            db_->SetTreeHead(tree_head_);
            return st;
        }

        std::pair<uint16_t , uint64_t> AddLabels(int64_t num) {
            label::Labels lbs;
            auto app = tree_head_->appender();
            uint64_t sgid;
            uint16_t mid;
            leveldb::Status s;
            for (int64_t i = 0; i < num; i++) {
                lbs.emplace_back("__name__", "cpu");
                lbs.emplace_back("label_1", "value_"+std::to_string(i%2));
                lbs.emplace_back("label_2", "value_"+std::to_string(i%4));
                lbs.emplace_back("label_3", "value_"+std::to_string(i%8));
                lbs.emplace_back("label_4", "value_"+std::to_string(i%16));

                s = app->add(lbs, i, i, sgid, mid);
                if (!s.ok()) {
                    std::cout<<s.ToString()<<std::endl;
                }
                lbs.clear();
//                std::cout<<mid<<" "<<sgid<<std::endl;
            }
            s = app->commit();
            if (!s.ok()) {
                std::cout<<s.ToString()<<std::endl;
            }

            return std::make_pair(tree_head_->get_last_metric_id(), tree_head_->get_last_source_group_id());
        }

        std::unique_ptr<index::PostingsInterface> QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>> matchers) {
            return tree_head_->select(matchers);
        }

    };


    TEST_F(TreeHeadQueryTest, Test1) {
        ASSERT_TRUE(setup().ok());
        auto ret = AddLabels(100);
        ASSERT_EQ(ret.first, 1);
        ASSERT_EQ(ret.second, 16);

//        sgid: 1 {__name__=cpu,label_1=value_0,label_2=value_0,label_3=value_0,label_4=value_0}
//        sgid: 2 {__name__=cpu,label_1=value_1,label_2=value_1,label_3=value_1,label_4=value_1}
//        sgid: 3 {__name__=cpu,label_1=value_0,label_2=value_2,label_3=value_2,label_4=value_2}
//        sgid: 4 {__name__=cpu,label_1=value_1,label_2=value_3,label_3=value_3,label_4=value_3}
//        sgid: 5 {__name__=cpu,label_1=value_0,label_2=value_0,label_3=value_4,label_4=value_4}
//        sgid: 6 {__name__=cpu,label_1=value_1,label_2=value_1,label_3=value_5,label_4=value_5}
//        sgid: 7 {__name__=cpu,label_1=value_0,label_2=value_2,label_3=value_6,label_4=value_6}
//        sgid: 8 {__name__=cpu,label_1=value_1,label_2=value_3,label_3=value_7,label_4=value_7}
//        sgid: 9 {__name__=cpu,label_1=value_0,label_2=value_0,label_3=value_0,label_4=value_8}
//        sgid: 10 {__name__=cpu,label_1=value_1,label_2=value_1,label_3=value_1,label_4=value_9}
//        sgid: 11 {__name__=cpu,label_1=value_0,label_2=value_2,label_3=value_2,label_4=value_10}
//        sgid: 12 {__name__=cpu,label_1=value_1,label_2=value_3,label_3=value_3,label_4=value_11}
//        sgid: 13 {__name__=cpu,label_1=value_0,label_2=value_0,label_3=value_4,label_4=value_12}
//        sgid: 14 {__name__=cpu,label_1=value_1,label_2=value_1,label_3=value_5,label_4=value_13}
//        sgid: 15 {__name__=cpu,label_1=value_0,label_2=value_2,label_3=value_6,label_4=value_14}
//        sgid: 16 {__name__=cpu,label_1=value_1,label_2=value_3,label_3=value_7,label_4=value_15}

        auto matcher0 = std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher("__name__", "cpu"));
        auto matcher1 = std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher("label_1", "value_1"));
        auto matcher2 = std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher("label_2", "value_1"));
        auto matcher3 = std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher("label_3", "value_1"));
        auto matcher4 = std::shared_ptr<label::MatcherInterface>(new label::EqualMatcher("label_4", "value_1"));

        int i = 0;
        std::vector<std::shared_ptr<label::MatcherInterface>> matchers;
        std::unique_ptr<index::PostingsInterface> postings;

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher0}));
        i = 1;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i++);
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher1}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 2;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher2}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 4;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher3}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 8;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher4}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 16;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher0, matcher1}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 2;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher0, matcher1, matcher2}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 4;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher0, matcher1, matcher2, matcher3}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 8;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher0, matcher2, matcher3}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 8;
        }

        postings = QueryPostings(std::vector<std::shared_ptr<label::MatcherInterface>>({matcher0, matcher1, matcher2, matcher3, matcher4}));
        i = 2;
        while (postings->next()) {
            uint64_t sgid = postings->at();
            ASSERT_EQ(sgid, i);
            i += 16;
//            std::cout<<sgid<<std::endl;
        }
    }

}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}