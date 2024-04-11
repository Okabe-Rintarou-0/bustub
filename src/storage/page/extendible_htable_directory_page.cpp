//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
    max_depth_ = max_depth;
    global_depth_ = 0;
    size_t size = Size();
    for (size_t i = 0; i < size; i++) {
        bucket_page_ids_[i] = -1;
    }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const
    -> uint32_t {
    uint32_t mask = GetGlobalDepthMask();
    return hash & mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const
    -> page_id_t {
    return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx,
                                                    page_id_t bucket_page_id) {
    bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(
    uint32_t bucket_idx) const -> uint32_t {
    return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
    uint32_t mask = 0;
    mask = ~((~mask) << global_depth_);
    return mask;
}
auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const
    -> uint32_t {
    uint32_t mask = 0;
    mask = ~((~mask) << local_depths_[bucket_idx]);
    return mask;
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
    return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
    size_t prev_size = Size();
    size_t curr_size = prev_size << 1;
    uint32_t mask = 0;
    mask = ~((~mask) << global_depth_);
    global_depth_++;
    for (size_t i = prev_size; i <= curr_size; i++) {
        bucket_page_ids_[i] = bucket_page_ids_[i & mask];
        local_depths_[i] = local_depths_[i & mask];
    }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
    global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
    size_t size = Size();
    for (size_t i = 0; i < size; i++) {
        if (local_depths_[i] == global_depth_) {
            return false;
        }
    }
    return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
    return 1 << global_depth_;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
    return 1 << max_depth_;
}
auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const
    -> uint32_t {
    return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx,
                                                  uint8_t local_depth) {
    local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
    local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
    local_depths_[bucket_idx]--;
}

}  // namespace bustub
