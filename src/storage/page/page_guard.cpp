#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard&& that) noexcept
    : BasicPageGuard(that.bpm_, that.page_) {}

void BasicPageGuard::Drop() {
    if (page_ != nullptr) {
        bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        page_ = nullptr;
    }
}

auto BasicPageGuard::operator=(BasicPageGuard&& that) noexcept
    -> BasicPageGuard& {
    Drop();
    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
    return *this;
}

BasicPageGuard::~BasicPageGuard() {
    if (page_ != nullptr) {
        Drop();
    }
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
    return {bpm_, page_};
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
    return {bpm_, page_};
}

ReadPageGuard::ReadPageGuard(BufferPoolManager* bpm, Page* page)
    : guard_(bpm, page) {
    if (page != nullptr) {
        page->RLatch();
    }
}

ReadPageGuard::ReadPageGuard(ReadPageGuard&& that) noexcept
    : guard_(std::move(that.guard_)){};

auto ReadPageGuard::operator=(ReadPageGuard&& that) noexcept -> ReadPageGuard& {
    this->guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
    if (guard_.page_ != nullptr) {
        guard_.page_->RUnlatch();
        guard_.Drop();
    }
}

ReadPageGuard::~ReadPageGuard() {
    if (guard_.page_ != nullptr) {
        Drop();
    }
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager* bpm, Page* page)
    : guard_(bpm, page) {
    if (page != nullptr) {
        page->WLatch();
    }
}

WritePageGuard::WritePageGuard(WritePageGuard&& that) noexcept
    : guard_(std::move(that.guard_)){};

auto WritePageGuard::operator=(WritePageGuard&& that) noexcept
    -> WritePageGuard& {
    this->guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    if (guard_.page_ != nullptr) {
        guard_.page_->WUnlatch();
        guard_.Drop();
    }
}

WritePageGuard::~WritePageGuard() {
    if (guard_.page_ != nullptr) {
        Drop();
    }
}  // NOLINT

}  // namespace bustub
