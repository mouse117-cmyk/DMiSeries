#ifndef TSDB_UNITTEST_MERGESETPOSTINGS_H
#define TSDB_UNITTEST_MERGESETPOSTINGS_H
#include "mem/mergeset.h"
#include "label/Label.hpp"
#include "prefix_postings.h"
#include <shared_mutex>
#define MERGSET_POSTINGS_MAX_FILE_SIZE 1024*1024
namespace tsdb::index{
    struct ConcurrencyPostingList{
        PrefixPostingsV2 posting_list_;
        std::shared_mutex mtx_;
        std::atomic<uint32_t> hot_value_{0};

    };
    class MergeSetPostings{
    private:
        std::string dir_;
        leveldb::Options opts_;
        mem::MergeSet* m_;
        std::string key_;
    public:
        MergeSetPostings(std::string dir);
        ~MergeSetPostings();
        void DefaultOpts();
        inline leveldb::Slice LabelConvertSlice(const label::Label& l){
            std::string str = l.label + l.value;
            return leveldb::Slice(str.data(), str.size());
        }
        // Implementations of the DB interface
        leveldb::Status Put(const leveldb::WriteOptions&, const leveldb::Slice& key,
                            const leveldb::Slice& value) ;
        leveldb::Status Delete(const leveldb::WriteOptions&, const leveldb::Slice& key) ;
        leveldb::Status Get(const leveldb::ReadOptions& options, const leveldb::Slice& key,
                            std::string* value) ;
        leveldb::Status AddMergeSet(const leveldb::WriteOptions& options, const label::Label& l, ConcurrencyPostingList* cp) ;
        std::vector<uint64_t> GetAndReadMergeSet(const leveldb::ReadOptions& options, const label::Label& l);

        leveldb::Iterator* iterator(leveldb::ReadOptions opts);
        void CompactAll() { m_->CompactRange(nullptr, nullptr); }
    };
}
#endif //TSDB_UNITTEST_MERGESETPOSTINGS_H
