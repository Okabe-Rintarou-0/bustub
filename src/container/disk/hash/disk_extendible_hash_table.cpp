//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(
    const std::string& name,
    BufferPoolManager* bpm,
    const KC& cmp,
    const HashFunction<K>& hash_fn,
    uint32_t header_max_depth,
    uint32_t directory_max_depth,
    uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
    BasicPageGuard header_guard = bpm->NewPageGuarded(&header_page_id_);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K& key,
                                                 std::vector<V>* result,
                                                 Transaction* transaction) const
    -> bool {
    ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
    uint32_t hash = Hash(key);
    uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
    page_id_t directory_page_id =
        header_page->GetDirectoryPageId(directory_idx);
    if (directory_page_id == -1) {
        return false;
    }

    ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
    header_guard.Drop();
    auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
    uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    if (bucket_page_id == -1) {
        return false;
    }

    ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
    directory_guard.Drop();
    auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
    V value{};
    bool found = bucket_page->Lookup(key, value, cmp_);
    if (found) {
        result->push_back(value);
    }
    return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K& key,
                                               const V& value,
                                               Transaction* transaction)
    -> bool {
    ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
    uint32_t hash = Hash(key);
    uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
    page_id_t directory_page_id =
        header_page->GetDirectoryPageId(directory_idx);
    if (directory_page_id == -1) {
        header_guard.Drop();
        WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
        auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
        bool inserted =
            InsertToNewDirectory(header_page, directory_idx, hash, key, value);
        return inserted;
    }

    ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
    header_guard.Drop();
    auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
    uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    if (bucket_page_id == -1) {
        directory_guard.Drop();
        WritePageGuard directory_guard =
            bpm_->FetchPageWrite(directory_page_id);
        auto directory_page =
            directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
        bool inserted =
            InsertToNewBucket(directory_page, bucket_idx, key, value);
        return inserted;
    }

    WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    directory_guard.Drop();
    auto bucket_page =
        bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // If the bucket page is full, we need to split it
    page_id_t last_bucket_page_id;
    while (!bucket_page->Insert(key, value, cmp_)) {
        directory_guard.Drop();
        WritePageGuard directory_guard =
            bpm_->FetchPageWrite(directory_page_id);
        auto directory_page =
            directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
        bool need_global_incr = directory_page->GetLocalDepth(bucket_idx) ==
                                directory_page->GetGlobalDepth();
        if (need_global_incr) {
            // is full
            if (directory_page->MaxSize() == directory_page->Size()) {
                return false;
            }
            directory_page->IncrGlobalDepth();
        }
        last_bucket_page_id = bucket_page_id;
        SplitBucket(directory_page, bucket_page, bucket_idx);
        bucket_idx = directory_page->HashToBucketIndex(hash);
        bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
        if (bucket_page_id != last_bucket_page_id) {
            bucket_guard.Drop();
            bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
        }
    }

    return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(
    ExtendibleHTableHeaderPage* header,
    uint32_t directory_idx,
    uint32_t hash,
    const K& key,
    const V& value) -> bool {
    page_id_t directory_page_id;

    BasicPageGuard guard = bpm_->NewPageGuarded(&directory_page_id);
    WritePageGuard directory_guard = guard.UpgradeWrite();
    auto directory_page =
        directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
    InsertToNewBucket(directory_page, 0, key, value);
    header->SetDirectoryPageId(directory_idx, directory_page_id);
    return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(
    ExtendibleHTableDirectoryPage* directory,
    uint32_t bucket_idx,
    const K& key,
    const V& value) -> bool {
    page_id_t bucket_page_id;
    BasicPageGuard guard = bpm_->NewPageGuarded(&bucket_page_id);
    WritePageGuard bucket_guard = guard.UpgradeWrite();
    auto bucket_page =
        bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
    bucket_page->Insert(key, value, cmp_);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::SplitBucket(
    ExtendibleHTableDirectoryPage* directory,
    ExtendibleHTableBucketPage<K, V, KC>* old_bucket,
    uint32_t bucket_idx) {
    uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
    uint32_t old_mask = directory->GetLocalDepthMask(bucket_idx);
    uint32_t old_pattern = bucket_idx & old_mask;
    uint32_t global_depth = directory->GetGlobalDepth();
    uint32_t new_pattern = bucket_idx ^ (1 << local_depth);
    local_depth++;
    page_id_t new_bucket_page_id;

    uint32_t new_bucket_idx;
    uint32_t old_bucket_idx;
    uint32_t new_mask = directory->GetLocalDepthMask(bucket_idx);
    BasicPageGuard guard = bpm_->NewPageGuarded(&new_bucket_page_id);
    WritePageGuard new_bucket_guard = guard.UpgradeWrite();
    auto new_bucket =
        new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    uint32_t rest_bits = 1 << (global_depth - local_depth);
    for (uint32_t i = 0; i < rest_bits; i++) {
        new_bucket_idx = (i << local_depth) + new_pattern;
        old_bucket_idx = (i << local_depth) + old_pattern;
        directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
        directory->SetLocalDepth(new_bucket_idx, local_depth);
        directory->SetLocalDepth(old_bucket_idx, local_depth);
    }
    new_bucket->Init(bucket_max_size_);
    MigrateEntries(old_bucket, new_bucket, new_bucket_idx, new_mask);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(
    ExtendibleHTableDirectoryPage* directory,
    uint32_t new_bucket_idx,
    page_id_t new_bucket_page_id,
    uint32_t new_local_depth,
    uint32_t local_depth_mask) {
    throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(
    ExtendibleHTableBucketPage<K, V, KC>* old_bucket,
    ExtendibleHTableBucketPage<K, V, KC>* new_bucket,
    uint32_t new_bucket_idx,
    uint32_t local_depth_mask) {
    uint32_t old_size = old_bucket->Size();
    uint32_t rehash;
    uint32_t pattern;
    uint32_t new_pattern = new_bucket_idx & local_depth_mask;
    for (int i = old_size - 1; i >= 0; i--) {
        std::pair<K, V> entry = old_bucket->EntryAt(i);
        rehash = Hash(entry.first);
        pattern = rehash & local_depth_mask;
        if (pattern == new_pattern) {
            old_bucket->RemoveAt(i);
            new_bucket->Insert(entry.first, entry.second, cmp_);
            i--;
        }
    }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K& key,
                                               Transaction* transaction)
    -> bool {
    ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
    uint32_t hash = Hash(key);
    uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
    page_id_t directory_page_id =
        header_page->GetDirectoryPageId(directory_idx);
    if (directory_page_id == -1) {
        return false;
    }

    ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
    header_guard.Drop();
    auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
    uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    if (bucket_page_id == -1) {
        return false;
    }

    WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    directory_guard.Drop();
    auto bucket_page =
        bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    bool removed = bucket_page->Remove(key, cmp_);
    if (!bucket_page->IsEmpty()) {
        return removed;
    }

    WritePageGuard directory_write_guard =
        bpm_->FetchPageWrite(directory_page_id);
    auto directory_mut =
        directory_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
    uint32_t global_depth;
    uint32_t split_bucket_idx;
    uint32_t curr_local_depth;
    uint32_t split_local_depth;
    page_id_t split_bucket_page_id;
    do {
        split_bucket_idx = directory_mut->GetSplitImageIndex(bucket_idx);
        curr_local_depth = directory_mut->GetLocalDepth(bucket_idx);
        global_depth = directory_mut->GetGlobalDepth();
        if (curr_local_depth == 0) {
            return removed;
        }
        split_local_depth = directory_mut->GetLocalDepth(split_bucket_idx);
        if (curr_local_depth != split_local_depth) {
            // we can't do merge here
            return removed;
        }

        split_bucket_page_id = directory_mut->GetBucketPageId(split_bucket_idx);
        directory_mut->SetLocalDepth(split_bucket_idx, split_local_depth - 1);
        uint32_t pattern =
            directory_mut->GetLocalDepthMask(bucket_idx) & bucket_idx;
        uint32_t rest_bits = 1 << (global_depth - curr_local_depth);
        uint32_t old_bucket_idx;
        for (uint32_t i = 0; i < rest_bits; i++) {
            old_bucket_idx = (i << curr_local_depth) + pattern;
            directory_mut->SetBucketPageId(old_bucket_idx,
                                           split_bucket_page_id);
            directory_mut->SetLocalDepth(old_bucket_idx, split_local_depth - 1);
        }
        bucket_guard.Drop();
        bucket_guard = bpm_->FetchPageWrite(split_bucket_page_id);
        bucket_page =
            bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        if (directory_mut->CanShrink()) {
            directory_mut->DecrGlobalDepth();
        }
    } while (bucket_page->IsEmpty());
    return removed;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>,
                                       RID,
                                       GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>,
                                       RID,
                                       GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>,
                                       RID,
                                       GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>,
                                       RID,
                                       GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>,
                                       RID,
                                       GenericComparator<64>>;
}  // namespace bustub
