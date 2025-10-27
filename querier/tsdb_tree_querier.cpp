//#include "tsdb_tree_querier.h"
//
//#include <memory>
//#include "util/mutexlock.h"
//
//namespace tsdb::querier {
//
//    static void DeleteTSSamples(const leveldb::Slice& key, void* value) {
//        CachedSamples* tf = reinterpret_cast<CachedSamples*>(value);
//        delete tf->timestamps;
//        delete tf->values;
//        delete tf;
//    }
//
//    std::pair<std::vector<int64_t>, std::vector<double>> DecodeChunk(const std::string& chunk_content, uint64_t &sgid, uint16_t &mid) {
//        if (chunk_content.empty()) return std::make_pair(std::vector<int64_t>(), std::vector<double>());
//
//        std::vector<int64_t> t;
//        std::vector<double> v;
//
//        auto s = leveldb::Slice(chunk_content.data(), chunk_content.size());
//        leveldb::Slice tmp_value;
//        mid = leveldb::DecodeFixed16(s.data() + 1);
//        sgid = leveldb::DecodeFixed64BE(s.data() + 3);
//        tmp_value = leveldb::Slice(s.data() + 11, s.size() - 11);
//
//        tsdb::chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()), tmp_value.size());
//        auto iter = c.xor_iterator();
//        while (iter->next()) {
//            t.push_back(iter->at().first);
//            v.push_back((iter->at().second));
//        }
//        return std::make_pair(t, v);
//    }
//
//    /* ****************************************
//     *  TreeHeadIterator
//     * **************************************** */
//
//    TreeHeadIterator::TreeHeadIterator(const std::string& chunk_content, int64_t mint, int64_t maxt)
//          : end_(false),
//            min_time_(mint),
//            max_time_(maxt),
//            init_(false){
//        auto s = leveldb::Slice(chunk_content.data(), chunk_content.size());
//        leveldb::Slice tmp_value;
//        mid_ = leveldb::DecodeFixed16(s.data() + 1);
//        sgid_ = leveldb::DecodeFixed64BE(s.data() + 3);
//        tmp_value = leveldb::Slice(s.data() + 11, s.size() - 11);
//        chunk_ = tsdb::chunk::XORChunk(reinterpret_cast<const uint8_t*>(tmp_value.data()), tmp_value.size());
//        iter_ = chunk_.xor_iterator();
//    }
//
//    bool TreeHeadIterator::seek(int64_t t) const {
//        if (t > max_time_) return false;
//        if (!init_) {
//            init_ = true;
//            while (true) {
//                if (!iter_->next()) {
//                    end_ = true;
//                    break;
//                }
//                if (iter_->at().first > max_time_) {
//                    end_ = true;
//                    break;
//                } else if (iter_->at().first >= min_time_)
//                    break;
//            }
//        }
//        if (end_) return false;
//        while (iter_->next()) {
//            if (iter_->at().first > max_time_) {
//                end_ = true;
//                return false;
//            } else if (iter_->at().first >= t)
//                return true;
//        }
//        end_ = true;
//        return false;
//    }
//
//    std::pair<int64_t, double> TreeHeadIterator::at() const {
//        if (end_) return {0, 0};
//        return iter_->at();
//    }
//
//    bool TreeHeadIterator::next() const {
//        if (!init_) {
//            init_ = true;
//            while (true) {
//                if (!iter_->next()) {
//                    end_ = true;
//                    break;
//                }
//                if (iter_->at().first > max_time_) {
//                    end_ = true;
//                    break;
//                } else if (iter_->at().first >= min_time_)
//                    break;
//            }
//            return !end_;
//        }
//        if (end_) return false;
//        end_ = !iter_->next();
//        if (!end_ && iter_->at().first > max_time_) end_ = true;
//        return !end_;
//    }
//
//    /* ****************************************
//     *  SlabArrayIterator
//     * **************************************** */
//
//    SlabArrayIterator::SlabArrayIterator(slab::TreeSeries *tree_series, std::vector<std::pair<const slab::SlabInfo*, slab::Slab*>> slab_array, int64_t min_time, int64_t max_time, uint64_t sgid, uint16_t mid, leveldb::Cache *cache)
//            : slab_iter_(slab_array),
//              min_time_(min_time),
//              max_time_(max_time),
//              sgid_(sgid),
//              mid_(mid),
//              t_(nullptr),
//              v_(nullptr),
//              init_(false),
//              sub_idx_(0),
//              slab_idx_(0),
//              item_idx_(0),
//              tree_series_(tree_series),
//              cache_(cache),
//              handle_(nullptr){
//        if (min_time_ < 0) min_time_ = 0;
//    }
//
//    SlabArrayIterator::~SlabArrayIterator() {
//        if (handle_) cache_->Release(handle_);
//        if (!cache_) {
//            if (t_) delete t_;
//            if (v_) delete v_;
//        }
//        slab_iter_.clear();
//    }
//
//    inline void SlabArrayIterator::lookup_cached_ts(const leveldb::Slice &key, CachedSamples **samples, bool *create_ts) const {
//        handle_ = cache_->Lookup(key);
//        if (handle_) {
//            *samples = reinterpret_cast<CachedSamples*>(cache_->Value(handle_));
//            t_ = (*samples)->timestamps;
//            v_ = (*samples)->values;
//            return;
//        }
//        *samples =  new CachedSamples();
//        (*samples)->timestamps = new std::vector<int64_t>();
//        (*samples)->values = new std::vector<double>();
//        t_ = (*samples)->timestamps;
//        v_ = (*samples)->values;
//        t_->reserve(32);
//        v_->reserve(32);
//        *create_ts = true;
//    }
//
//    // decode from chunk s
//    void SlabArrayIterator::decode_value(const leveldb::Slice& key, const leveldb::Slice& s) const {
//        CachedSamples* samples = nullptr;
//        bool create_ts = false;
//        if (cache_) {
//            if (handle_)
//                cache_->Release(handle_);
//            lookup_cached_ts(key, &samples, &create_ts);
//            if (!create_ts) return;
//        } else if (!t_) {
//            t_ = new std::vector<int64_t>();
//            v_ = new std::vector<double>();
//            t_->reserve(32);
//            v_->reserve(32);
//        } else {
//            t_->clear();
//            v_->clear();
//        }
//
//        uint32_t tmp_size;
//        leveldb::Slice tmp_value;
//        uint16_t mid = leveldb::DecodeFixed16(s.data() + 1);
//        uint64_t sgid = leveldb::DecodeFixed64BE(s.data() + 3);
//
//        if (mid != mid_ || sgid != sgid_) return;
//
//        tmp_value = leveldb::Slice(s.data() + 11, s.size() - 11);
//
//        chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()), tmp_value.size());
//        auto iter = c.xor_iterator();
//        while (iter->next()) {
//            t_->push_back(iter->at().first);
//            v_->push_back(iter->at().second);
//        }
//
//        if (cache_ && create_ts) {
//            handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4, &DeleteTSSamples);
//        }
//    }
//
//    bool SlabArrayIterator::seek(int64_t t) const {
//        if (err_ || t > max_time_) return false;
//
////        init_ = true;
////        for (auto& pair : slab_iter_) {
////            if (pair.first->start_time_ > t) return false;
////            if (pair.first->end_time_ < t) continue;
////            int idx = GetItemIdx(pair.first, pair.second);
////            if (idx == std::numeric_limits<int>::max()) continue;
////            for (int i = idx; i < pair.first->nalloc_; i++) {
////                auto item = GetSlabItem(pair.first, pair.second, i);
////                if (item->timestamp_ > t) return false;
////                DecodeItemChunk(item);
////                sub_idx_ = 0;
////                while (sub_idx_ < t_->size()) {
////                    if (t_->at(sub_idx_) >= t) {
////                        return true;
////                    }
////                    sub_idx_++;
////                }
////            }
////        }
//
//        return false;
//    }
//
//    std::pair<int64_t, double> SlabArrayIterator::at() const {
//        if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
//        return {0, 0};
//    }
//
//    bool SlabArrayIterator::next() const {
//        if (err_) return false;
//        if (slab_iter_.empty()) return false;
//
//        if (!init_) {
//            item_idx_ = GetItemIdx(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second);
////            item_idx_ = 0;
//            while (item_idx_ == std::numeric_limits<int>::max()) {
//                slab_idx_++;
//                if (slab_idx_ >= slab_iter_.size()) return false;
//                item_idx_ = GetItemIdx(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second);
//            }
//            auto item = GetSlabItem(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second, item_idx_);
//            if (item == nullptr) return false;
//            DecodeItemChunk(item);
//            item_idx_++;
//            init_ = true;
//
//            while(true) {
//                while (sub_idx_ < t_->size()) {
//                    if (t_->at(sub_idx_) > max_time_) {
//                        err_.set("larger than max time 1");
//                        return false;
//                    } else if (t_->at(sub_idx_) >= min_time_) {
//                        return true;
//                    }
//                    sub_idx_++;
//                }
//                // next item
//                if (sub_idx_ >= t_->size()) {
//                    sub_idx_ = 0;
//                    if (item_idx_ >= slab_iter_[slab_idx_].first->nalloc_) {
//                        // next slab
//                        slab_idx_++;
//                        if (slab_idx_ >= slab_iter_.size()) {
//                            return false;
//                        }
//                        item_idx_ = GetItemIdx(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second);
////                        item_idx_ = 0;
//                        while (item_idx_ == std::numeric_limits<int>::max()) {
//                            slab_idx_++;
//                            if (slab_idx_ >= slab_iter_.size()) return false;
//                            item_idx_ = GetItemIdx(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second);
//                        }
//                        auto item = GetSlabItem(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second, item_idx_);
//                        DecodeItemChunk(item);
//                        item_idx_++;
//                    } else {
//                        uint16_t mid = 0;
//                        uint64_t sgid = 0;
//                        int64_t start_time = std::numeric_limits<int64_t>::max();
//                        auto item = GetSlabItem(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second, item_idx_);
//                        DecodeItemChunkHeader(item, mid, sgid, start_time);
//                        item_idx_++;
//                        if (mid != mid_ || sgid != sgid_) continue;
//                        DecodeItemChunk(item);
//                    }
//                }
//            }
//        }
//
//        sub_idx_++;
//        if (sub_idx_ >= t_->size()) {
//            sub_idx_ = 0;
//            while (true) {
//                if (item_idx_ >= slab_iter_[slab_idx_].first->nalloc_) {
//                    // next slab
//                    slab_idx_++;
//                    if (slab_idx_ >= slab_iter_.size()) {
//                        return false;
//                    }
//                    item_idx_ = GetItemIdx(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second);
////                    item_idx_ = 0;
//                    while (item_idx_ == std::numeric_limits<int>::max()) {
//                        slab_idx_++;
//                        if (slab_idx_ >= slab_iter_.size()) return false;
//                        item_idx_ = GetItemIdx(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second);
//                    }
//                    auto item = GetSlabItem(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second, item_idx_);
//                    DecodeItemChunk(item);
//                    item_idx_++;
//                } else {
//                    uint16_t mid = 0;
//                    uint64_t sgid = 0;
//                    int64_t start_time = std::numeric_limits<int64_t>::max();
//                    auto item = GetSlabItem(slab_iter_[slab_idx_].first, slab_iter_[slab_idx_].second, item_idx_);
//                    DecodeItemChunkHeader(item, mid, sgid, start_time);
//                    item_idx_++;
//                    if (mid != mid_ || sgid != sgid_) continue;
//                    DecodeItemChunk(item);
//                }
//
//                if (sub_idx_ >= t_->size()) continue;
//                if (t_->at(sub_idx_) > max_time_) {
//                    err_.set("no more data 1");
//                    return false;
//                }
//                while (sub_idx_ < t_->size()) {
//                    if (t_->at(sub_idx_) > max_time_) {
//                        err_.set("larger than max time 2");
//                        return false;
//                    } else if (t_->at(sub_idx_) >= min_time_) {
//                        return true;
//                    }
//                    sub_idx_++;
//                }
//                item_idx_++;
//            }
//        } else if (t_->at(sub_idx_) > max_time_) {
//            err_.set("no more data 2");
//            return false;
//        }
//
//        return true;
//    }
//
//    void SlabArrayIterator::SlabIterator() {
//        for (auto& it : slab_iter_) {
//            DecodeSlab(it.first, it.second);
//        }
//    }
//
//    void SlabArrayIterator::DecodeSlab(const slab::SlabInfo *sinfo, slab::Slab *s) const {
//        if (sinfo == nullptr || sinfo->nalloc_ == 0 || s == nullptr) return;
//        for (uint32_t i = 0; i < sinfo->nalloc_; i++) {
//            auto item = GetSlabItem(sinfo, s, i);
//            DecodeItemChunk(item);
//        }
//    }
//
//    int SlabArrayIterator::GetItemIdx(const slab::SlabInfo *sinfo, slab::Slab *s) const {
//        if (sinfo == nullptr || sinfo->nalloc_ == 0 || s == nullptr) return std::numeric_limits<int>::max();
//        int prev_item_idx = -1;
//        for (int i = 0; i < sinfo->nalloc_; i++) {
//            auto item = (slab::Item*)((uint8_t *)s->data_ + (i*slab::SLAB_ITEM_SIZE));
//
//            uint16_t mid = 0;
//            uint64_t sgid = 0;
//            int64_t start_time = std::numeric_limits<int64_t>::max();
//            DecodeItemChunkHeader(item, mid, sgid, start_time);
////            std::cout<<mid<<" "<<sgid<<" "<<start_time<<std::endl;
//            if (mid != mid_ || sgid != sgid_) continue;
//            if (start_time < min_time_) prev_item_idx = i;
//            if (start_time == min_time_) return i;
//            if (start_time > min_time_) {
//                if (prev_item_idx >= 0) {
//                    return prev_item_idx;
//                } else {
//                    return i;
//                }
//            }
//        }
//        if (prev_item_idx >= 0) return prev_item_idx;
//        return std::numeric_limits<int>::max();
//    }
//
//    slab::Item* SlabArrayIterator::GetSlabItem(const slab::SlabInfo *sinfo, slab::Slab *s, uint32_t idx) const {
//        if (sinfo == nullptr || sinfo->nalloc_ == 0 || s == nullptr) return nullptr;
//        auto item = (slab::Item*)((uint8_t *)s->data_ + (idx*slab::SLAB_ITEM_SIZE));
////        std::unique_ptr<slab::Item> res = std::make_unique<slab::Item>();
////        res->timestamp_ = item->timestamp_;
////        memcpy(res->chunk_, item->chunk_, slab::CHUNK_SIZE);
////        return res;
////        tree_series_->PrintItem(item);
//        return item;
//    }
//
//    std::vector<slab::Item*> SlabArrayIterator::GetSlabItemArray(const slab::SlabInfo *sinfo, slab::Slab *s) const {
//        if (sinfo == nullptr || sinfo->nalloc_ == 0 || s == nullptr) return std::vector<slab::Item*>();
//        std::vector<slab::Item*> item_arr;
//        for (uint32_t i = 0; i < sinfo->nalloc_; i++) {
//            item_arr.emplace_back(GetSlabItem(sinfo, s, i));
//        }
//        return item_arr;
//    }
//
//    std::vector<chunk::XORChunk*> SlabArrayIterator::GetSlabChunkArray(const slab::SlabInfo *sinfo, slab::Slab *s) const {
//        if (sinfo == nullptr || sinfo->nalloc_ == 0 || s == nullptr) return std::vector<chunk::XORChunk*>();
//        std::vector<chunk::XORChunk*> chunk_arr;
//        for (uint32_t i = 0; i < sinfo->nalloc_; i++) {
//            chunk_arr.emplace_back((chunk::XORChunk*)(GetSlabItem(sinfo, s, i)->chunk_));
//        }
//        return chunk_arr;
//    }
//
//    void SlabArrayIterator::DecodeItemChunk(slab::Item* item) const {
//        if(item == nullptr){
//            return;
//        }
//        std::string key;
//        tree_series_->EnCodeKey(&key, sgid_, mid_, item->timestamp_);
//        decode_value(key, leveldb::Slice(reinterpret_cast<const char *>(item->chunk_), slab::CHUNK_SIZE));
//    }
//
//    void SlabArrayIterator::DecodeItemChunkHeader(slab::Item *item, uint16_t &mid, uint64_t &sgid, int64_t &start_time) const {
//        if (item == nullptr) return;
//        leveldb::Slice tmp_value;
//        auto s = leveldb::Slice(reinterpret_cast<const char *>(item->chunk_), slab::CHUNK_SIZE);
//        mid = leveldb::DecodeFixed16(s.data() + 1);
//        sgid = leveldb::DecodeFixed64BE(s.data() + 3);
//
//        tmp_value = leveldb::Slice(s.data() + 11, s.size() - 11);
//        chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()), tmp_value.size());
//        auto iter = c.xor_iterator();
//        while (iter->next()) {
//            start_time = iter->timestamp;
//            return;
//        }
//   }
//
//    /* ****************************************
//     *  TreeSeriesIterator
//     * **************************************** */
//
//    TreeSeriesIterator::TreeSeriesIterator(slab::TreeSeries *tree_series, int64_t min_time, int64_t max_time, uint64_t sgid, uint16_t mid, leveldb::Cache *cache)
//        : min_time_(min_time),
//          max_time_(max_time),
//          mid_(mid),
//          sgid_(sgid),
//          tree_series_(tree_series),
//          slab_array_(){
//        if (min_time_ < 0) min_time_ = 0;
//        tree_series->Scan(sgid, mid, min_time, max_time, slab_array_);
//        slab_iter_ = std::unique_ptr<SlabArrayIterator>(new SlabArrayIterator(tree_series, slab_array_, min_time, max_time, sgid, mid, cache));
//    }
//
//    TreeSeriesIterator::~TreeSeriesIterator() {
//        for (auto& it : slab_array_) {
//            if (it.first->mem_ == false) {
//                tree_series_->FreeBufSlab(it.second);
//            }
//        }
//    }
//
//    bool TreeSeriesIterator::seek(int64_t t) const {
//        return slab_iter_->seek(t);
//    }
//
//    std::pair<int64_t, double> TreeSeriesIterator::at() const {
//        return slab_iter_->at();
//    }
//
//    bool TreeSeriesIterator::next() const {
//        return slab_iter_->next();
//    }
//
//    /* ****************************************
//     *  MemtableIterator
//     * **************************************** */
//
//    MemtableIterator::MemtableIterator(slab::TreeSeries *tree_series, leveldb::MemTable* mem, int64_t min_time, int64_t max_time, uint64_t sgid, uint16_t mid, leveldb::Cache *cache)
//            : min_time_(min_time),
//              max_time_(max_time),
//              mid_(mid),
//              sgid_(sgid),
//              tree_series_(tree_series),
//              slab_array_(){
//        mem_iter_.reset(mem->NewIterator());
//        if (min_time_ < 0) min_time_ = 0;
//
//        GetSlabArray();
//        slab_iter_ = std::make_unique<SlabArrayIterator>(tree_series, slab_array_, min_time_, max_time_, sgid, mid, cache);
//    }
//
//    MemtableIterator::~MemtableIterator() {
//        for (auto& it : slab_array_) {
//            delete it.first;
//        }
//    }
//
//    void MemtableIterator::GetSlabArray() {
//        std::string key;
//        tree_series_->EnCodeKey(&key, sgid_, mid_, min_time_);
//        leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
//        mem_iter_->Seek(lk.internal_key());
//        if (!mem_iter_->Valid()) {
//            err_.set("error next seek 1");
//            return;
//        }
//        mem_iter_->Prev();
//        if (!mem_iter_->Valid()) {
//            mem_iter_->Seek(lk.internal_key());
//        }
//
//        uint16_t mid;
//        uint64_t sgid;
//        uint64_t t;
//        tree_series_->DeCodeKey(mem_iter_->key().ToString(), sgid, mid, t);
//        if (sgid != sgid_ || mid != mid_) {
//            mem_iter_->Next();
//            if (!mem_iter_->Valid()) {
//                err_.set("error next seek 2");
//                return;
//            }
//            tree_series_->DeCodeKey(mem_iter_->key().ToString(), sgid, mid, t);
//            if (sgid != sgid_ || mid != mid_) return;
//        }
//        auto sinfo = new slab::SlabInfo();
//        sinfo->source_id_[0] = sgid;
//        sinfo->metric_id_[0] = mid;
//        sinfo->idx_ = 1;
//        sinfo->nalloc_ = slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE;
//        slab_array_.emplace_back(sinfo, (slab::Slab*)(mem_iter_->value().data()));
//
//        while (true) {
//            mem_iter_->Next();
//            if (!mem_iter_->Valid()) {
//                err_.set("error next seek 3");
//                return;
//            }
//            tree_series_->DeCodeKey(mem_iter_->key().ToString(), sgid, mid, t);
//            if (sgid != sgid_ || mid != mid_) return;
//            auto sinfo = new slab::SlabInfo();
//            sinfo->source_id_[0] = sgid;
//            sinfo->metric_id_[0] = mid;
//            sinfo->idx_ = 1;
//            sinfo->nalloc_ = slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE;
//            slab_array_.emplace_back(sinfo, (slab::Slab*)(mem_iter_->value().data()));
//        }
//    }
//
//    bool MemtableIterator::seek(int64_t t) const {
//        return slab_iter_->seek(t);
//    }
//
//    std::pair<int64_t, double> MemtableIterator::at() const {
//        return slab_iter_->at();
//    }
//
//    bool MemtableIterator::next() const {
//        return slab_iter_->next();
//    }
//
//    /* ****************************************
//     *  L0TreeSeriesIterator
//     * **************************************** */
//
//    L0TreeSeriesIterator::L0TreeSeriesIterator(const querier::TreeQuerier *q, uint64_t sgid, uint16_t mid, leveldb::Iterator *it, int64_t mint, int64_t maxt)
//        : q_(q),
//          tree_series_(q->tree_series_),
//          sgid_(sgid),
//          mid_(mid),
//          min_time_(mint),
//          max_time_(maxt),
//          iter_(it) {
//        GetSlabArray();
//        slab_iter_ = std::make_unique<SlabArrayIterator>(tree_series_, slab_array_, min_time_, max_time_, sgid, mid);
//    }
//
//    L0TreeSeriesIterator::L0TreeSeriesIterator(const querier::TreeQuerier *q, int partition, uint64_t sgid, uint16_t mid, int64_t mint, int64_t maxt)
//        : q_(q),
//          tree_series_(q->tree_series_),
//          partition_(partition),
//          sgid_(sgid),
//          mid_(mid),
//          min_time_(mint),
//          max_time_(maxt) {
//        std::vector<leveldb::Iterator*> list;
//        q_->current_->AddIterators(leveldb::ReadOptions(), 0, q_->l0_indexes_[partition_], &list, sgid_, mid_);
//        iter_.reset(leveldb::NewMergingIterator(q_->cmp_, &list[0], list.size()));
//
//        GetSlabArray();
//        slab_iter_ = std::make_unique<SlabArrayIterator>(tree_series_, slab_array_, min_time_, max_time_, sgid, mid);
//    }
//
//    L0TreeSeriesIterator::~L0TreeSeriesIterator() {
//        for (auto& it : slab_array_) {
//            delete it.first;
//        }
//    }
//
//    void L0TreeSeriesIterator::GetSlabArray() {
//        std::string key;
//        tree_series_->EnCodeKey(&key, sgid_, mid_, min_time_);
//        leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
//        iter_->Seek(lk.internal_key());
//        if (!iter_->Valid()) {
//            err_.set("error next seek 1");
//            return;
//        }
//        iter_->Prev();
//        if (!iter_->Valid()) {
//            iter_->Seek(lk.internal_key());
//        }
//
//        uint16_t mid;
//        uint64_t sgid;
//        uint64_t t;
//        tree_series_->DeCodeKey(iter_->key().ToString(), sgid, mid, t);
//        if (sgid != sgid_ || mid != mid_) {
//            iter_->Next();
//            if (!iter_->Valid()) {
//                err_.set("error next seek 2");
//                return;
//            }
//            tree_series_->DeCodeKey(iter_->key().ToString(), sgid, mid, t);
//            if (sgid != sgid_ || mid != mid_) return;
//        }
//        auto sinfo = new slab::SlabInfo();
//        sinfo->source_id_[0] = sgid;
//        sinfo->metric_id_[0] = mid;
//        sinfo->idx_ = 1;
//        sinfo->nalloc_ = slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE;
//        slab_array_.emplace_back(sinfo, (slab::Slab*)(iter_->value().data()));
//
//        while (true) {
//            iter_->Next();
//            if (!iter_->Valid()) {
//                err_.set("error next seek 3");
//                return;
//            }
//            tree_series_->DeCodeKey(iter_->key().ToString(), sgid, mid, t);
//            if (sgid != sgid_ || mid != mid_) return;
//            if (t > max_time_) return;
//            auto sinfo = new slab::SlabInfo();
//            sinfo->source_id_[0] = sgid;
//            sinfo->metric_id_[0] = mid;
//            sinfo->idx_ = 1;
//            sinfo->nalloc_ = slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE;
//            slab_array_.emplace_back(sinfo, (slab::Slab*)(iter_->value().data()));
//        }
//    }
//
//    bool L0TreeSeriesIterator::seek(int64_t t) const {
//        return slab_iter_->seek(t);
//    }
//
//    std::pair<int64_t, double> L0TreeSeriesIterator::at() const {
//        return slab_iter_->at();
//    }
//
//    bool L0TreeSeriesIterator::next() const {
//        return slab_iter_->next();
//    }
//
//    /* ****************************************
//     *  L1TreeSeriesIterator
//     * **************************************** */
//
//    L1TreeSeriesIterator::L1TreeSeriesIterator(const querier::TreeQuerier *q, int partition, uint64_t sgid, uint16_t mid, int64_t mint, int64_t maxt)
//            : q_(q),
//              tree_series_(q->tree_series_),
//              partition_(partition),
//              sgid_(sgid),
//              mid_(mid),
//              min_time_(mint),
//              max_time_(maxt) {
//        std::vector<leveldb::Iterator*> list;
//        q_->current_->AddIterators(leveldb::ReadOptions(), 1, q_->l1_indexes_[partition_], &list, sgid_, mid_);
//        iter_.reset(leveldb::NewMergingIterator(q_->cmp_, &list[0], list.size()));
//
//        GetSlabArray();
//        slab_iter_ = std::make_unique<SlabArrayIterator>(tree_series_, slab_array_, min_time_, max_time_, sgid, mid);
//    }
//
//    L1TreeSeriesIterator::~L1TreeSeriesIterator() {
//        for (auto& it : slab_array_) {
//            delete it.first;
//        }
//    }
//
//    void L1TreeSeriesIterator::GetSlabArray() {
//        std::string key;
//        tree_series_->EnCodeKey(&key, sgid_, mid_, min_time_);
//        leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
//        iter_->Seek(lk.internal_key());
//        if (!iter_->Valid()) {
//            err_.set("error next seek 1");
//            return;
//        }
//        iter_->Prev();
//        if (!iter_->Valid()) {
//            iter_->Seek(lk.internal_key());
//        }
//
//        uint16_t mid;
//        uint64_t sgid;
//        uint64_t t;
//        tree_series_->DeCodeKey(iter_->key().ToString(), sgid, mid, t);
//        if (sgid != sgid_ || mid != mid_) {
//            iter_->Next();
//            if (!iter_->Valid()) {
//                err_.set("error next seek 2");
//                return;
//            }
//            tree_series_->DeCodeKey(iter_->key().ToString(), sgid, mid, t);
//            if (sgid != sgid_ || mid != mid_) return;
//        }
//        auto sinfo = new slab::SlabInfo();
//        sinfo->source_id_[0] = sgid;
//        sinfo->metric_id_[0] = mid;
//        sinfo->idx_ = 1;
//        sinfo->nalloc_ = slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE;
//        slab_array_.emplace_back(sinfo, (slab::Slab*)(iter_->value().data()));
//
//        while (true) {
//            iter_->Next();
//            if (!iter_->Valid()) {
//                err_.set("error next seek 3");
//                return;
//            }
//            tree_series_->DeCodeKey(iter_->key().ToString(), sgid, mid, t);
//            if (sgid != sgid_ || mid != mid_) return;
//            auto sinfo = new slab::SlabInfo();
//            sinfo->source_id_[0] = sgid;
//            sinfo->metric_id_[0] = mid;
//            sinfo->idx_ = 1;
//            sinfo->nalloc_ = slab::SLAB_SIZE / slab::SLAB_ITEM_SIZE;
//            slab_array_.emplace_back(sinfo, (slab::Slab*)(iter_->value().data()));
//        }
//    }
//
//    bool L1TreeSeriesIterator::seek(int64_t t) const {
//        return slab_iter_->seek(t);
//    }
//
//    std::pair<int64_t, double> L1TreeSeriesIterator::at() const {
//        return slab_iter_->at();
//    }
//
//    bool L1TreeSeriesIterator::next() const {
//        return slab_iter_->next();
//    }
//
//    /* ****************************************
//     *  ListMergeSeriesIterator
//     * **************************************** */
//
//    ListMergeSeriesIterator::~ListMergeSeriesIterator() {
//        for (auto& it : iters_) delete it;
//    }
//
//    bool ListMergeSeriesIterator::seek(int64_t t) const {
//        if (err_) return false;
//        while (idx_ < iters_.size()) {
//            if (!iters_[idx_]->seek(t)) {
//                idx_++;
//                continue;
//            }
//            return true;
//        }
//        return false;
//    }
//
//    bool ListMergeSeriesIterator::next() const {
//        if (err_ || idx_ >= iters_.size()) {
//            return false;
//        }
//        if (iters_[idx_]->next()) {
//            return true;
//        }
//        while (++idx_ < iters_.size()) {
//            if (iters_[idx_]->next()) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    /* ****************************************
//     *  TreeQuerierSeries
//     * **************************************** */
//
//    TreeQuerierSeries::TreeQuerierSeries(const TreeQuerier *q, uint64_t sgid, uint16_t mid) : q_(q), sgid_(sgid), mid_(mid), init_(false) {
//        auto tms = q_->tree_head_->read_flat_forward_index(sgid, mid);
////        if (tms == nullptr) {
////            tms = q_->tree_head_->get_from_forward_index(sgid-1, mid);
////        }
//        head_flush_time_ = tms->head_flush_time();
//        level_flush_time_ = tms->level_flush_time();
//    }
//
//    const label::Labels& TreeQuerierSeries::labels() const {
//        if (init_) return lset_;
//        q_->tree_head_->series(sgid_, mid_, lset_, &head_chunk_contents_);
//        init_ = true;
//        return lset_;
//    }
//
//    std::unique_ptr<querier::SeriesIteratorInterface> TreeQuerierSeries::iterator() {
//        if (!init_) {
//            q_->tree_head_->series(sgid_, mid_, lset_, &head_chunk_contents_);
//            init_ = true;
//        }
//
//        std::vector<querier::SeriesIteratorInterface*> iters;
//        iters.reserve(q_->l1_indexes_.size() + q_->l0_indexes_.size() + q_->imms_.size() + 2);
//
//        if (q_->min_time_ <= level_flush_time_) {
//            // Add L1 iterators
//            for (int i = 0; i < q_->l1_indexes_.size(); i++) {
//                iters.push_back(new L1TreeSeriesIterator(q_, i, sgid_, mid_, q_->min_time_, q_->max_time_));
//            }
//
//            // Add L0 iterators
//            for (int i = 0; i < q_->l0_indexes_.size(); i++) {
//                querier::MergeSeriesIterator* mit = new querier::MergeSeriesIterator();
//                std::vector<leveldb::Iterator*> list;
//                q_->current_->AddIterators(leveldb::ReadOptions(), 0, q_->l0_indexes_[i], &list, sgid_, mid_);
//                for (leveldb::Iterator* it : list) {
//                    mit->push_back(new L0TreeSeriesIterator(q_, sgid_, mid_, it, q_->min_time_, q_->max_time_));
//                }
//                iters.push_back(mit);
//            }
//
//            // Add mem iterators
//            for (int i = 0; i < q_->imms_.size(); i++) {
//                iters.push_back(new MemtableIterator(q_->tree_series_, q_->imms_[i], q_->min_time_, q_->max_time_, sgid_, mid_));
//            }
//            if (q_->mem_) {
//                iters.push_back(new MemtableIterator(q_->tree_series_, q_->mem_, q_->min_time_, q_->max_time_, sgid_, mid_));
//            }
//        }
////        bool flag = 0;
//        if (q_->max_time_ > level_flush_time_) {
//            // Add TreeSeries iterator
//            iters.push_back(new TreeSeriesIterator(q_->tree_series_, std::max(q_->min_time_,level_flush_time_), q_->max_time_, sgid_, mid_));
////            auto slab_array = get_head_slab_array(q_->tree_series_, q_->tree_head_, sgid_, mid_);
////            if(q_->min_time_>slab_array[0].first->start_time_[0]){
////                iters.push_back(new SlabArrayIterator(q_->tree_series_, slab_array, q_->min_time_, q_->max_time_, sgid_, mid_));
////                flag = 1;
////            }
//        }
//
//        if (q_->max_time_ >= head_flush_time_) {
//            // Add head iterator
//            if (!head_chunk_contents_.empty()) {
//                iters.push_back(new TreeHeadIterator(head_chunk_contents_, q_->min_time_, q_->max_time_));
//
//            }
////            if(!flag){
////                auto slab_array = get_head_slab_array(q_->tree_series_, q_->tree_head_, sgid_, mid_);
////                iters.push_back(new SlabArrayIterator(q_->tree_series_, slab_array, q_->min_time_, q_->max_time_, sgid_, mid_));
////            }
//
//        }
//
//        return std::unique_ptr<querier::SeriesIteratorInterface>(new querier::MergeSeriesIterator(iters));
//    }
//
//    std::unique_ptr<querier::SeriesIteratorInterface> TreeQuerierSeries::chain_iterator() {
//        if (!init_) {
//            q_->tree_head_->series(sgid_, mid_, lset_, &head_chunk_contents_);
//            init_ = true;
//        }
//
//        std::vector<querier::SeriesIteratorInterface*> iters;
//        iters.reserve(q_->l1_indexes_.size() + q_->l0_indexes_.size() + q_->imms_.size() + 2);
//
//        if (q_->min_time_ <= level_flush_time_) {
//            // Add L1 iterators
//            for (int i = 0; i < q_->l1_indexes_.size(); i++) {
//                iters.push_back(new L1TreeSeriesIterator(q_, i, sgid_, mid_, q_->min_time_, q_->max_time_));
//            }
//
//            // Add L0 iterators
//            for (int i = 0; i < q_->l0_indexes_.size(); i++) {
//                std::vector<leveldb::Iterator*> list;
//                q_->current_->AddIterators(leveldb::ReadOptions(), 0, q_->l0_indexes_[i], &list, sgid_, mid_);
//                for (leveldb::Iterator* it : list) {
//                    iters.push_back(new L0TreeSeriesIterator(q_, sgid_, mid_, it, q_->min_time_, q_->max_time_));
//                }
//            }
//
//            // Add mem iterators
//            for (int i = 0; i < q_->imms_.size(); i++) {
//                iters.push_back(new MemtableIterator(q_->tree_series_, q_->imms_[i], q_->min_time_, q_->max_time_, sgid_, mid_));
//            }
//            if (q_->mem_) {
//                iters.push_back(new MemtableIterator(q_->tree_series_, q_->mem_, q_->min_time_, q_->max_time_, sgid_, mid_));
//            }
//        }
////        bool flag = 0;
//        if (q_->max_time_ > level_flush_time_) {
//            // Add TreeSeries iterator
//            iters.push_back(new TreeSeriesIterator(q_->tree_series_, std::max(level_flush_time_,q_->min_time_), q_->max_time_, sgid_, mid_));
////            auto slab_array = get_head_slab_array(q_->tree_series_, q_->tree_head_, sgid_, mid_);
////            if(q_->min_time_>slab_array[0].first->start_time_[0]){
////                iters.push_back(new SlabArrayIterator(q_->tree_series_, slab_array, q_->min_time_, q_->max_time_, sgid_, mid_));
////                flag = 1;
////            }
//        }
//
//        if (q_->max_time_ >= head_flush_time_) {
//            // Add head iterator
//            if (!head_chunk_contents_.empty()) {
//                iters.push_back(new TreeHeadIterator(head_chunk_contents_, q_->min_time_, q_->max_time_));
//            }
////            if(!flag){
////                 auto slab_array = get_head_slab_array(q_->tree_series_, q_->tree_head_, sgid_, mid_);
////                 iters.push_back(new SlabArrayIterator(q_->tree_series_, slab_array, q_->min_time_, q_->max_time_, sgid_, mid_));
////            }
//        }
//
//        return std::unique_ptr<querier::SeriesIteratorInterface>(new querier::ListMergeSeriesIterator(iters));
//    }
//
//
//    /* ****************************************
//     *  TreeQuerierSeriesSet
//     * **************************************** */
//
//    TreeQuerierSeriesSet::TreeQuerierSeriesSet(const tsdb::querier::TreeQuerier *q, const std::vector<std::shared_ptr<label::MatcherInterface>> &l)
//        : q_(q),
//          matchers_(l),
//          sgid_(0),
//          mid_(0) {
//        auto p = q->tree_head_->select(l);
//        if (!p) {
//            err_.set("error TreeQuerierSeriesSet postings for matchers");
//            LOG_DEBUG << "error TreeQuerierSeriesSet postings for matchers";
//            p_.reset();
//        } else {
//            p_  = std::move(p);
//        }
//        for (auto& it : matchers_) {
//            if (it->name() == "__name__") {
//                bool is_exist = true;
//                mid_ = q_->tree_head_->get_or_create_metric_id(label::Labels({label::Label(it->name(), it->value())}), is_exist);
//                if (!is_exist) mid_ = 0;
//                break;
//            }
//        }
//    }
//
//    bool TreeQuerierSeriesSet::next() const {
//        if (err_) return false;
//        if (mid_ == 0) return false;
//        if (!p_->next()) return false;
//
//        sgid_ = p_->at();
//        return true;
//    }
//
//    std::unique_ptr<querier::SeriesInterface> TreeQuerierSeriesSet::at() {
//        if (err_) return nullptr;
//        if (mid_ == 0 || sgid_ == 0) return nullptr;
////        if (q_->tree_head_->get_from_forward_index(sgid_, mid_) == nullptr) return nullptr;
//        if (q_->tree_head_->read_flat_forward_index(sgid_, mid_) == nullptr) return nullptr;
//        return std::unique_ptr<querier::SeriesInterface>(new TreeQuerierSeries(q_, sgid_, mid_));
//    }
//
//    /* ****************************************
//     *  TreeQuerier
//     * **************************************** */
//
//    TreeQuerier::TreeQuerier(leveldb::DB *db, head::TreeHead *tree_head, slab::TreeSeries* tree_series, int64_t min_time, int64_t max_time, leveldb::Cache *cache)
//        : db_(db),
//          tree_head_(tree_head),
//          tree_series_(tree_series),
//          need_unref_current_(true),
//          min_time_(min_time),
//          max_time_(max_time),
//          cmp_(db->internal_comparator()),
//          cache_(cache) {
//        if (min_time_ < 0) min_time_ = 0;
//        leveldb::MutexLock l(db_->mutex());
//        register_mem_partitions();
//        register_disk_partitions();
//    }
//
//    TreeQuerier::~TreeQuerier() {
//        if (need_unref_current_) {
//            leveldb::MutexLock l(db_->mutex());
//            current_->Unref();
//        }
//    }
//
//    void TreeQuerier::register_mem_partitions() {
//        mem_ = db_->mem();
//        imms_ = *db_->imms();
//    }
//
//    void TreeQuerier::register_disk_partitions() {
//        current_ = db_->current();
//        current_->Ref();
//        current_->OverlappingPartitions(0, min_time_, max_time_, &l0_partitions_,
//                                        &l0_indexes_);
//        current_->OverlappingPartitions(1, min_time_, max_time_, &l1_partitions_,
//                                        &l1_indexes_);
//    }
//
//    std::unique_ptr<::tsdb::querier::SeriesSetInterface> TreeQuerier::select(std::vector<std::shared_ptr<::tsdb::label::MatcherInterface>> &l) const {
//        return std::unique_ptr<::tsdb::querier::SeriesSetInterface>(new TreeQuerierSeriesSet(this, l));
//    }
//
//}