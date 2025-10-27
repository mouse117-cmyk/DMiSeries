//#ifndef TSDB_TREE_QUERIER_H
//#define TSDB_TREE_QUERIER_H
//
//#include "chunk/XORChunk.hpp"
//#include "leveldb/cache.h"
//#include "leveldb/db.h"
//#include "leveldb/slice.h"
//#include "leveldb/db/version_set.h"
//#include "leveldb/db/memtable.h"
//#include "leveldb/table/merger.h"
//#include "querier/QuerierInterface.hpp"
//#include "querier/SeriesInterface.hpp"
//#include "querier/SeriesSetInterface.hpp"
//#include "TreeSeries/TreeSeries.h"
//#include "head/TreeHead.h"
//
//namespace tsdb{
//    namespace head {
//        class TreeHead;
//    }
//
//    namespace querier {
//    class TreeQuerier;
//
//    struct CachedSamples {
//        std::vector<int64_t>* timestamps;
//        std::vector<double>* values;
//    };
//
//    class TreeHeadIterator : public querier::SeriesIteratorInterface {
//    private:
//        chunk::XORChunk chunk_;
//        std::unique_ptr<chunk::XORIterator> iter_;
//        mutable bool end_;
//        int64_t min_time_;
//        int64_t max_time_;
//        mutable bool init_;
//        uint16_t mid_;
//        uint64_t sgid_;
//
//    public:
//        TreeHeadIterator(const std::string& chunk_content, int64_t mint, int64_t maxt);
//        bool seek(int64_t t) const;
//        std::pair<int64_t , double> at() const;
//        bool next() const;
//        bool error() const {return end_;}
//    };
//
//    class SlabArrayIterator : public querier::SeriesIteratorInterface {
//    private:
//        int64_t min_time_;
//        int64_t max_time_;
//
//        uint64_t sgid_;
//        uint16_t mid_;
//
//        leveldb::Status s_;
//        mutable tsdb::error::Error err_;
//
//        slab::TreeSeries* tree_series_;
//        mutable std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_iter_;
//
//        mutable std::vector<int64_t>* t_;
//        mutable std::vector<double>* v_;
//        mutable int sub_idx_;
//        mutable int slab_idx_;
//        mutable int item_idx_;
//        mutable bool init_;
//
//        leveldb::Cache* cache_;
//        mutable leveldb::Cache::Handle* handle_;
//
//        void decode_value(const leveldb::Slice& key, const leveldb::Slice& s) const;
//        void lookup_cached_ts(const leveldb::Slice& key, CachedSamples** samples, bool* create_ts) const;
//
//    public:
//        SlabArrayIterator(slab::TreeSeries* tree_series, std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_array, int64_t min_time, int64_t max_time, uint64_t sgid, uint16_t mid, leveldb::Cache* cache = nullptr);
//        ~SlabArrayIterator();
//        bool seek(int64_t t) const override;
//        std::pair<int64_t, double> at() const override;
//        bool next() const override;
//        bool error() const {return err_;}
//
//        void SlabIterator();
//        void DecodeSlab(const slab::SlabInfo* sinfo, slab::Slab* s) const;
//        int GetItemIdx(const slab::SlabInfo* sinfo, slab::Slab* s) const;
//        slab::Item* GetSlabItem(const slab::SlabInfo* sinfo, slab::Slab* s, uint32_t idx) const;
//        std::vector<slab::Item*> GetSlabItemArray(const slab::SlabInfo* sinfo, slab::Slab* s) const;
//        std::vector<chunk::XORChunk*> GetSlabChunkArray(const slab::SlabInfo* sinfo, slab::Slab* s) const;
//        void DecodeItemChunk(slab::Item* item) const;
//        void DecodeItemChunkHeader(slab::Item* item, uint16_t& mid, uint64_t& sgid, int64_t& start_time) const;
//    };
//
//    class TreeSeriesIterator : public tsdb::querier::SeriesIteratorInterface {
//    private:
//        int64_t min_time_;
//        int64_t max_time_;
//
//        uint16_t mid_;
//        uint64_t sgid_;
//
//        leveldb::Status s_;
//        mutable tsdb::error::Error err_;
//
//        slab::TreeSeries* tree_series_;
//        std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_array_;
//        std::unique_ptr<SlabArrayIterator> slab_iter_;
//
//    public:
//        TreeSeriesIterator(slab::TreeSeries* tree_series, int64_t min_time, int64_t max_time, uint64_t sgid, uint16_t mid, leveldb::Cache* cache = nullptr);
//        ~TreeSeriesIterator();
//        bool seek(int64_t t) const override;
//        std::pair<int64_t, double> at() const override;
//        bool next() const override;
//        bool error() const {return err_;}
//    };
//
//    class MemtableIterator : public querier::SeriesIteratorInterface {
//    private:
//        int64_t min_time_;
//        int64_t max_time_;
//
//        uint16_t mid_;
//        uint64_t sgid_;
//
//        leveldb::Status s_;
//        mutable error::Error err_;
//
//        slab::TreeSeries* tree_series_;
//        std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_array_;
//        std::unique_ptr<SlabArrayIterator> slab_iter_;
//
//        std::unique_ptr<leveldb::Iterator> mem_iter_;
//
//    public:
//        MemtableIterator(slab::TreeSeries* tree_series, leveldb::MemTable* mem, int64_t min_time, int64_t max_time, uint64_t sgid, uint16_t mid, leveldb::Cache* cache = nullptr);
//        ~MemtableIterator();
//
//        void GetSlabArray();
//
//        bool seek(int64_t t) const override;
//        std::pair<int64_t, double> at() const override;
//        bool next() const override;
//        bool error() const {return err_;}
//    };
//
//    class L0TreeSeriesIterator : public querier::SeriesIteratorInterface {
//    private:
//        const TreeQuerier* q_;
//        int partition_;
//        uint64_t sgid_;
//        uint16_t mid_;
//
//        int64_t min_time_;
//        int64_t max_time_;
//
//        leveldb::Status s_;
//        mutable error::Error err_;
//
//        slab::TreeSeries* tree_series_;
//        std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_array_;
//        std::unique_ptr<SlabArrayIterator> slab_iter_;
//
//        std::unique_ptr<leveldb::Iterator> iter_;
//
//    public:
//        L0TreeSeriesIterator(const querier::TreeQuerier* q, int partition, uint64_t sgid,  uint16_t mid, int64_t mint, int64_t maxt);
//        L0TreeSeriesIterator(const querier::TreeQuerier* q, uint64_t sgid, uint16_t mid, leveldb::Iterator* it, int64_t mint, int64_t maxt);
//        ~L0TreeSeriesIterator();
//
//        void GetSlabArray();
//
//        bool seek(int64_t t) const override;
//        std::pair<int64_t , double> at() const override;
//        bool next() const override;
//        bool error() const override {return err_;}
//    };
//
//    class L1TreeSeriesIterator : public querier::SeriesIteratorInterface {
//    private:
//        const TreeQuerier* q_;
//        int partition_;
//        uint64_t sgid_;
//        uint16_t mid_;
//
//        int64_t min_time_;
//        int64_t max_time_;
//
//        leveldb::Status s_;
//        mutable error::Error err_;
//
//        slab::TreeSeries* tree_series_;
//        std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_array_;
//        std::unique_ptr<SlabArrayIterator> slab_iter_;
//
//        std::unique_ptr<leveldb::Iterator> iter_;
//
//    public:
//        L1TreeSeriesIterator(const querier::TreeQuerier* q, int partition, uint64_t sgid,  uint16_t mid, int64_t mint, int64_t maxt);
//        ~L1TreeSeriesIterator();
//
//        void GetSlabArray();
//
//        bool seek(int64_t t) const override;
//        std::pair<int64_t , double> at() const override;
//        bool next() const override;
//        bool error() const override {return err_;}
//    };
//
//    class ListMergeSeriesIterator : public querier::SeriesIteratorInterface {
//    private:
//        std::vector<querier::SeriesIteratorInterface*>  iters_;
//        mutable int idx_;
//        mutable bool err_;
//
//    public:
//        ListMergeSeriesIterator(const std::vector<querier::SeriesIteratorInterface*>& iters) : iters_(iters), idx_(0), err_(false) {}
//        ~ListMergeSeriesIterator();
//
//        bool seek(int64_t t) const override;
//        std::pair<int64_t , double> at() const override {return iters_[idx_]->at();}
//        bool next() const override;
//        bool error() const override {return err_;}
//    };
//
//    class TreeQuerierSeries : public querier::SeriesInterface {
//    private:
//        const TreeQuerier* q_;
//        uint64_t sgid_;
//        uint16_t mid_;
//        int64_t head_flush_time_;
//        int64_t level_flush_time_;
//        mutable label::Labels lset_;
//        mutable std::string head_chunk_contents_;
//        mutable bool init_;
//
//    public:
//        TreeQuerierSeries(const TreeQuerier* q, uint64_t sgid, uint16_t mid);
//
//        const label::Labels & labels() const override;
//        std::unique_ptr<querier::SeriesIteratorInterface> iterator() override;
//        std::unique_ptr<querier::SeriesIteratorInterface> chain_iterator() override;
//
//        uint32_t get_sid(slab::TreeSeries* tree_series, head::TreeHead* tree_head, uint64_t sgid, uint16_t mid) {
////            auto tms = tree_head->get_from_forward_index(sgid, mid);
//            auto tms = tree_head->read_flat_forward_index(sgid, mid);
//            return tms->sid_;
//        }
//
//        std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> get_head_slab_array(slab::TreeSeries* tree_series, head::TreeHead* tree_head, uint64_t sgid, uint16_t mid) {
////            auto tms = tree_head->get_from_forward_index(sgid, mid);
//            auto tms = tree_head->read_flat_forward_index(sgid, mid);
//            std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_array;
//            if (tms->sid_ == std::numeric_limits<uint32_t>::max())  return slab_array;
//            auto sinfo = tree_series->GetMemSlabInfo(tms->sid_);
//            slab::SlabInfo* new_sinfo = new slab::SlabInfo();
//            for(uint8_t i=0;i<sinfo->idx_;i++){
//                if(sgid == tree_series->GetSinfoSourceID(sinfo,i)&&tree_series->GetSinfoMetricID(sinfo,i)){
//                    new_sinfo->start_time_[0] = sinfo->start_time_[i];
//                    break;
//                }
//            }
//            new_sinfo->source_id_[0] = sgid / slab::CY_NUM;
//            new_sinfo->metric_id_[0] = mid / slab::CX_NUM;
//            new_sinfo->idx_ = 1;
//            new_sinfo->nalloc_ = sinfo->nalloc_.load();
//            slab_array.emplace_back(new_sinfo, tree_series->GetMemSlab(tms->sid_));
//            return slab_array;
//        }
//    };
//
//    class TreeQuerierSeriesSet : public querier::SeriesSetInterface {
//    private:
//        const TreeQuerier* q_;
//        std::unique_ptr<index::PostingsInterface> p_;
//        mutable uint64_t sgid_{};
//        mutable uint16_t mid_;
//        error::Error err_;
//        std::vector<std::shared_ptr<label::MatcherInterface>> matchers_;
//
//    public:
//        TreeQuerierSeriesSet(const TreeQuerier* q, const std::vector<std::shared_ptr<label::MatcherInterface>>& l);
//
//        bool next() const override;
//        std::unique_ptr<querier::SeriesInterface> at() override;
//        uint64_t  current_sgid() override { return sgid_; }
//        uint16_t current_mid() override { return mid_; }
//        bool error() const override {return err_;}
//    };
//
//    class TreeQuerier : public QuerierInterface {
//    private:
//        friend class TreeHeadIterator;
//        friend class TreeSeriesIterator;
//        friend class MemtableIterator;
//        friend class L0TreeSeriesIterator;
//        friend class L1TreeSeriesIterator;
//        friend class TreeQuerierSeries;
//        friend class TreeQuerierSeriesSet;
//
//        mutable leveldb::DB* db_;
//        mutable tsdb::head::TreeHead* tree_head_;
//        mutable slab::TreeSeries* tree_series_;
//
//        mutable leveldb::Version* current_;
//        bool need_unref_current_;
//        int64_t min_time_;
//        int64_t max_time_;
//        const leveldb::Comparator* cmp_;
//        mutable tsdb::error::Error err_;
//        leveldb::Status s_;
//
//        leveldb::MemTable* mem_;
//        std::vector<leveldb::MemTable*> imms_;
//
//        std::vector<std::pair<int64_t, int64_t>> l0_partitions_;
//        std::vector<int> l0_indexes_;
//        std::vector<std::pair<int64_t, int64_t>> l1_partitions_;
//        std::vector<int> l1_indexes_;
//
//        leveldb::Cache* cache_;
//
//        void register_mem_partitions();
//        void register_disk_partitions();
//
//    public:
//        TreeQuerier(leveldb::DB* db, head::TreeHead* tree_head, slab::TreeSeries* tree_series, int64_t min_time, int64_t max_time, leveldb::Cache* cache = nullptr);
//
//        ~TreeQuerier();
//
//        int64_t mint() { return min_time_; }
//        int64_t maxt() { return max_time_; }
//        error::Error error() const override { return err_; }
//
//        std::unique_ptr<querier::SeriesSetInterface> select(std::vector<std::shared_ptr<label::MatcherInterface>>& l) const;
//
//        std::vector<std::string> label_values(const std::string& s) const override {return {};};
//        std::vector<std::string> label_names() const override {return {};}
//    };
//
//
//    }
//
//}
//
//#endif
