//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <mutex>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager* disk_manager,
                                     size_t replacer_k,
                                     LogManager* log_manager)
    : pool_size_(pool_size),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
    // we allocate a consecutive memory space for the buffer pool
    pages_ = new Page[pool_size_];
    replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

    // Initially, every page is in the free list.
    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_.emplace_back(static_cast<int>(i));
    }
}

BufferPoolManager::~BufferPoolManager() {
    delete[] pages_;
}

auto BufferPoolManager::NewPage(page_id_t* page_id) -> Page* {
    std::unique_lock<std::mutex> lock(latch_);
    frame_id_t fid;
    page_id_t old_pid;
    Page* page = nullptr;
    // If there exists some free pages
    if (!free_list_.empty()) {
        fid = free_list_.front();
        free_list_.pop_front();
        page = &pages_[fid];
    }
    // Otherwise, Check if there exists some evictable page
    else if (replacer_->Evict(&fid)) {
        page = &pages_[fid];
        old_pid = page->page_id_;
        // Flush the evicted page if it's dirty
        if (page->is_dirty_) {
            FlushPageInner(old_pid);
        }
        // Old page has been evicted
        page_table_.erase(old_pid);
    } else {
        *page_id = INVALID_PAGE_ID;
        return nullptr;
    }

    page_id_t new_pid = AllocatePage();
    page_table_[new_pid] = fid;
    page->ResetMemory();
    page->page_id_ = new_pid;
    page->pin_count_ = 1;
    *page_id = new_pid;
    // Mark as not evicatable
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id,
                                  [[maybe_unused]] AccessType access_type)
    -> Page* {
    if (page_id == INVALID_PAGE_ID) {
        return nullptr;
    }
    std::unique_lock<std::mutex> lock(latch_);
    // If target page is cached in buffer pool now
    frame_id_t fid = -1;
    Page* page = nullptr;
    if (page_table_.count(page_id) > 0) {
        fid = page_table_[page_id];
        replacer_->RecordAccess(fid);
        replacer_->SetEvictable(fid, false);
        page = &pages_[fid];
        page->pin_count_ += 1;
        return page;
    }

    if (!free_list_.empty()) {
        fid = free_list_.front();
        free_list_.pop_front();
        page = &pages_[fid];
    }
    // Otherwise, Check if there exists some evictable page
    else if (replacer_->Evict(&fid)) {
        page = &pages_[fid];
        page_id_t old_pid = page->page_id_;
        // Flush the evicted page if it's dirty
        if (page->is_dirty_) {
            FlushPageInner(old_pid);
        }
        // Old page has been evicted
        page_table_.erase(old_pid);
    } else {
        return nullptr;
    }
    page_table_[page_id] = fid;

    // Mark as not evicatable
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);

    DiskRequest r;
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    r.data_ = page->data_;

    r.is_write_ = false;
    r.page_id_ = page_id;
    // Read into page's data
    r.callback_ = disk_scheduler_->CreatePromise();
    auto future = r.callback_.get_future();

    // Schedule disk request
    disk_scheduler_->Schedule(std::move(r));
    future.wait();
    return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id,
                                  bool is_dirty,
                                  [[maybe_unused]] AccessType access_type)
    -> bool {
    std::lock_guard<std::mutex> guard(latch_);
    // No such page, return false
    if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0) {
        return false;
    }
    // Otherwise, get the page
    frame_id_t fid = page_table_[page_id];
    Page* page = &pages_[fid];
    size_t pin_count = page->pin_count_;
    // If the page's pin count is already 0, return false
    if (pin_count == 0) {
        return false;
    }

    // Mark whether it's dirty;
    page->is_dirty_ |= is_dirty;
    page->pin_count_--;
    if (page->pin_count_ == 0) {
        // It should be evictable
        // We can flush it when we evict it if it's dirty, but not now.
        replacer_->SetEvictable(fid, true);
    }
    return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
    std::lock_guard<std::mutex> guard(latch_);
    return FlushPageInner(page_id);
}

auto BufferPoolManager::FlushPageInner(page_id_t page_id) -> bool {
    // No such page, return false
    if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0) {
        return false;
    }
    frame_id_t fid = page_table_[page_id];
    Page* page = &pages_[fid];
    // Write into disk
    DiskRequest r;
    page->is_dirty_ = false;
    r.data_ = page->data_;
    r.is_write_ = true;
    r.page_id_ = page_id;
    r.callback_ = disk_scheduler_->CreatePromise();
    auto future = r.callback_.get_future();

    disk_scheduler_->Schedule(std::move(r));
    future.wait();
    return true;
}

void BufferPoolManager::FlushAllPages() {
    std::lock_guard<std::mutex> guard(latch_);
    for (const auto& [pid, _] : page_table_) {
        FlushPageInner(pid);
    }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
    std::lock_guard<std::mutex> guard(latch_);
    // No such page, return false
    if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0) {
        return false;
    }
    frame_id_t fid = page_table_[page_id];
    Page* page = &pages_[fid];
    page->page_id_ = INVALID_PAGE_ID;
    page->ResetMemory();
    // It's pinned, cannot be deleted
    if (page->pin_count_ > 0) {
        return false;
    }

    // Replace don't need to track it
    replacer_->Remove(fid);
    // Add to free list
    free_list_.push_back(fid);
    DeallocatePage(page_id);
    return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
    return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
    return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
    return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
    return {this, FetchPage(page_id)};
}

auto BufferPoolManager::NewPageGuarded(page_id_t* page_id) -> BasicPageGuard {
    return {this, NewPage(page_id)};
}

}  // namespace bustub
