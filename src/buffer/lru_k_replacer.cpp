//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <functional>
#include <iostream>
#include <mutex>
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t fid) : fid_(fid) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() = default;

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t fid;
  for (auto iter = access_list_.rbegin(); iter != access_list_.rend(); iter++) {
    if (iter->is_evictable_) {
      fid = iter->fid_;
      if (frame_id != nullptr) {
        *frame_id = fid;
      }
      access_list_.erase(node_store_[fid]);
      node_store_.erase(fid);
      --curr_size_;
      return true;
    }
  }
  for (auto iter = cache_list_.rbegin(); iter != cache_list_.rend(); iter++) {
    if (iter->is_evictable_) {
      fid = iter->fid_;
      if (frame_id != nullptr) {
        *frame_id = fid;
      }
      cache_list_.erase(node_store_[fid]);
      node_store_.erase(fid);
      --curr_size_;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // We need a latch here
  std::lock_guard<std::mutex> guard(latch_);
  // Update timestamp
  current_timestamp_++;
  // If node does not exist, create and insert it into the list
  if (node_store_.count(frame_id) == 0) {
    // Evict or it will exceed the maximum size
    if (curr_size_ == replacer_size_) {
      Evict(nullptr);
    }

    access_list_.emplace_front(frame_id);
    auto node_iter = access_list_.begin();
    // Store the new node
    node_store_[frame_id] = node_iter;
    node_iter->k_timestamp_ = current_timestamp_;
    node_iter->history_.push_back(current_timestamp_);
  } else {
    auto node_iter = node_store_[frame_id];
    LRUKNode node = *node_iter;
    auto &history = node.history_;
    history.push_back(current_timestamp_);
    size_t history_len = history.size();
    if (history_len >= k_) {
      size_t index = history_len - k_;
      auto iter = history.begin();
      for (size_t i = 0; i < index; i++) {
        iter++;
      }
      node.k_timestamp_ = *iter;
      if (history_len == k_) {
        access_list_.erase(node_iter);
      } else {
        cache_list_.erase(node_iter);
      }
      auto insert_point =
          std::upper_bound(cache_list_.begin(), cache_list_.end(), node,
                           [](const LRUKNode &n1, const LRUKNode &n2) { return n1.k_timestamp_ > n2.k_timestamp_; });
      node_iter = cache_list_.insert(insert_point, node);
      node_store_[frame_id] = node_iter;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  // If node does not even exist, do nothing
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  auto &node_iter = node_store_[frame_id];
  bool origin_evictable = node_iter->is_evictable_;
  // No effect, do nothing
  if (origin_evictable == set_evictable) {
    return;
  }
  node_iter->is_evictable_ = set_evictable;
  if (set_evictable) {
    ++curr_size_;
  } else {
    --curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto node_iter = node_store_[frame_id];
  if (node_iter->is_evictable_) {
    curr_size_--;
  }
  node_store_.erase(frame_id);
  if (node_iter->history_.size() >= k_) {
    cache_list_.erase(node_iter);
  } else {
    access_list_.erase(node_iter);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }
}  // namespace bustub