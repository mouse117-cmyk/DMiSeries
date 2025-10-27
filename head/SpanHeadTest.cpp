#define GLOBAL_VALUE_DEFINE

#include "SpanHead.h"
#include "SpanHeadAppender.h"

namespace tsdb::head{
    class SpanHeadTest : public testing::Test {
    public :
        SpanHead* head_;
        slab::TreeSeries* tree_series_;

        void setup(const std::string& sep_db_path, const std::string& db_path, const std::string& log_path) {

            std::string  path = "/home/dell/project/SSD/tree_series_test";
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
            slab::Setting *setting = new slab::Setting();
            setting->ssd_device_ = "/home/dell/project/SSD/tree_series_test";
            std::string info_path = "/home/dell/project/SSD/tree_series_info_test";
            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
            setting->ssd_slab_info_ = "/home/dell/project/SSD/tree_series_info_test";
            tree_series_ = new slab::TreeSeries(*setting);

            boost::filesystem::remove_all(sep_db_path);
            boost::filesystem::remove_all(db_path);
            boost::filesystem::remove_all(log_path);

            head_ = new head::SpanHead(sep_db_path, db_path, log_path,"",tree_series_);
        }
    };

    TEST_F(SpanHeadTest, L1_Encode_Decode_Test) {
        uint64_t sgid = 1234;
        uint16_t mid = 503;
        uint64_t end_time = 38920582;

        uint64_t t_sgid;
        uint16_t t_mid;
        uint64_t t_end_time;

        std::string key;
        slab::encode_l1key(&key, sgid, mid, end_time);
        slab::decode_l1key(key, t_sgid, t_mid, t_end_time);
        ASSERT_EQ(t_sgid, sgid);
        ASSERT_EQ(t_mid, mid);
        ASSERT_EQ(t_end_time, end_time);

        bool mem = true;
        uint32_t sid_ = 3214;
        uint8_t item_idx = 42;

        bool t_mem;
        uint32_t t_sid;
        uint8_t t_idx;

        std::string val;
        slab::encode_l1val(&val, mem, sid_, item_idx);
        slab::decode_l1val(val, t_mem, t_sid, t_idx);
        ASSERT_EQ(t_mem, mem);
        ASSERT_EQ(t_sid, sid_);
        ASSERT_EQ(t_idx, item_idx);
    }

    TEST_F(SpanHeadTest, SEP_DB_TEST) {
        std::string sep_db_path = "/mnt/nvme1/sep_db";
        std::string db_path = "/mnt/HDD/tsdb_big";
        std::string log_path = "/mnt/nvme1/tsdb_big";

        setup(sep_db_path, db_path, log_path);

        auto sep_db = tree_series_->GetDB();
        ASSERT_NE(sep_db, nullptr);

        std::string key;
        std::string val;
        leveldb::WriteOptions write_option = leveldb::WriteOptions();
        for (uint64_t i = 1; i < 10000; i++) {
            for (uint16_t j = 1; j < 500; j++) {
                for (uint64_t t = 1; t < 10000; t++) {
                    key.clear();
                    val.clear();
                    slab::encode_l1key(&key, i, j, t);
                    slab::encode_l1val(&val, true, 1, 1);
                    tree_series_->InsertDB(i, j, t, true, 1, 1);
                }
            }
        }
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}