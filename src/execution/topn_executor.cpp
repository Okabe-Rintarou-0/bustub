#include "execution/executors/topn_executor.h"
#include <algorithm>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext* exec_ctx,
                           const TopNPlanNode* plan,
                           std::unique_ptr<AbstractExecutor>&& child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
    Tuple tuple;
    RID rid;
    child_executor_->Init();
    index_ = 0;
    if (sorted_) {
        return;
    }
    sorted_ = true;
    const auto& cmp = [this](const Tuple& t1, const Tuple& t2) {
        return TupleCmp(t1, t2);
    };
    while (child_executor_->Next(&tuple, &rid)) {
        if (heap_.size() < plan_->n_) {
            heap_.push_back(tuple);
            std::push_heap(heap_.begin(), heap_.end(), cmp);
            continue;
        }

        if (TupleCmp(tuple, heap_[0])) {
            std::pop_heap(heap_.begin(), heap_.end(), cmp);
            heap_.pop_back();
            heap_.push_back(tuple);
            std::push_heap(heap_.begin(), heap_.end(), cmp);
        }
    }
    size_t n = GetNumInHeap();
    for (size_t i = 0; i < n; i++) {
        std::pop_heap(heap_.begin(), heap_.end(), cmp);
        tuples_.push_back(*heap_.rbegin());
        heap_.pop_back();
    }
    std::reverse(tuples_.begin(), tuples_.end());
}

auto TopNExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (index_ == tuples_.size()) {
        return false;
    }
    *tuple = tuples_[index_];
    *rid = tuple->GetRid();
    index_++;
    return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return heap_.size();
};

}  // namespace bustub
