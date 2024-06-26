//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
    max_size_ = max_size;
    size_ = 0;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K& key,
                                                  V& value,
                                                  const KC& cmp) const -> bool {
    for (uint32_t i = 0; i < size_; i++) {
        const auto& kv = array_[i];
        const K& this_key = kv.first;
        if (!cmp(this_key, key)) {
            value = kv.second;
            return true;
        }
    }
    return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K& key,
                                                  const V& value,
                                                  const KC& cmp) -> bool {
    for (uint32_t i = 0; i < size_; i++) {
        auto& kv = array_[i];
        const K& this_key = kv.first;
        if (!cmp(this_key, key)) {
            kv.second = value;
            return false;
        }
    }
    if (size_ == max_size_) {
        return false;
    }
    array_[size_] = std::make_pair(key, value);
    size_++;
    return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K& key, const KC& cmp)
    -> bool {
    int index = -1;
    for (uint32_t i = 0; i < size_; i++) {
        auto& kv = array_[i];
        const K& this_key = kv.first;
        if (!cmp(this_key, key)) {
            index = i;
            break;
        }
    }
    if (index == -1) {
        return false;
    }

    RemoveAt(index);
    return true;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
    size_--;
    for (uint32_t i = bucket_idx; i < size_; i++) {
        array_[i] = array_[i + 1];
    }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const
    -> K {
    return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const
    -> V {
    return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const
    -> const std::pair<K, V>& {
    return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
    return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
    return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
    return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>,
                                          RID,
                                          GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>,
                                          RID,
                                          GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>,
                                          RID,
                                          GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>,
                                          RID,
                                          GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>,
                                          RID,
                                          GenericComparator<64>>;

}  // namespace bustub
