#include <iostream>
#include "MergeSetPostings.h"
namespace tsdb::index{
    void MergeSetPostings::DefaultOpts() {
        opts_.create_if_missing = true;
        opts_.max_file_size = MERGSET_POSTINGS_MAX_FILE_SIZE;
        opts_.max_imm_num = 3;
        opts_.use_log = 0;
        opts_.write_buffer_size = MERGSET_POSTINGS_MAX_FILE_SIZE;
    }
    MergeSetPostings::MergeSetPostings(std::string dir):dir_(dir) {
        DefaultOpts();
//        m_ = new mem::MergeSet(opts_,dir_);
        mem::MergeSet::MergeSetOpen(opts_, dir_, &m_);
    }
    MergeSetPostings::~MergeSetPostings() {
//        delete m_;
    }

    leveldb::Status
    MergeSetPostings::Put(const leveldb::WriteOptions &o, const leveldb::Slice &key, const leveldb::Slice &value) {
        return m_->Put(o,key,value);
    }

    leveldb::Status MergeSetPostings::Delete(const leveldb::WriteOptions &o, const leveldb::Slice &key) {
        return m_->Delete(o,key);
    }

    leveldb::Status
    MergeSetPostings::Get(const leveldb::ReadOptions &options, const leveldb::Slice &key, std::string *value) {
        return m_->Get(options,key,value);
    }

    leveldb::Status
    MergeSetPostings::AddMergeSet(const leveldb::WriteOptions &options, const label::Label& l, ConcurrencyPostingList* cp) {
        std::string value;
        cp->mtx_.lock();
        cp->posting_list_.reset_cursor();
        leveldb::PutFixed32(&value, cp->posting_list_.size());
        while (cp->posting_list_.next()) {
//            leveldb::PutFixed64BE(&value,cp->posting_list_.at());
            leveldb::PutFixed64(&value,cp->posting_list_.at());
        }
//        auto key = LabelConvertSlice(l);
        key_.clear();
        key_ = l.label+l.value;
        cp->mtx_.unlock();
//        Put(options,key,value);
        Put(options,key_,value);
        return leveldb::Status::OK();
    }

    std::vector<uint64_t>
    MergeSetPostings::GetAndReadMergeSet(const leveldb::ReadOptions &options, const label::Label& l) {
//        auto key = LabelConvertSlice(l);
//        std::string res;
//        Get(options,key,&res);
//        uint32_t id_num = res.size();
//        std::vector<uint64_t>id_list;
//        for(uint32_t i=0;i<id_num;i++){
//            id_list.emplace_back(leveldb::DecodeFixed64BE(&res.c_str()[i*8]));
//        }
//        return id_list;

        std::string key = l.label+l.value;
        std::vector<uint64_t>id_list;

//        std::string res;
//        Get(options,key,&res);
//        if (res.size() == 0) return id_list;
//        leveldb::Slice val(res.data(), res.size());
//        uint32_t id_num;
//        leveldb::GetFixed32(&val, &id_num);
//
//        std::cout<<val.data()<<" "<<val.size()<<" "<<id_num<<std::endl;
//
//        uint64_t id;
//        for(uint32_t i=0;i<id_num;i++){
//            leveldb::GetFixed64(&val, &id);
//            id_list.emplace_back(id);
//        }

        auto iter = iterator(options);
        iter->Seek(key);

        while (iter->Valid() && iter->key() == key) {
            leveldb::Slice val = iter->value();
            uint32_t id_num;
            leveldb::GetFixed32(&val, &id_num);

            uint64_t id;
            for(uint32_t i=0;i<id_num;i++){
                leveldb::GetFixed64(&val, &id);
                id_list.emplace_back(id);
            }
            iter->Next();
        }


        return id_list;
    }

    leveldb::Iterator*
    MergeSetPostings::iterator(leveldb::ReadOptions opts) {
        return m_->NewIterator(opts);
    }

}