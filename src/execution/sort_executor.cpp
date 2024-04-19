#include "execution/executors/sort_executor.h"
#include <ios>
#include "execution/expressions/comparison_expression.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext* exec_ctx,
                           const SortPlanNode* plan,
                           std::unique_ptr<AbstractExecutor>&& child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    index_ = 0;
    if (sorted_) {
        return;
    }
    sorted_ = true;
    while (child_executor_->Next(&tuple, &rid)) {
        tuples_.push_back(tuple);
    }
    std::sort(tuples_.begin(), tuples_.end(),
              [&](const Tuple& t1, const Tuple& t2) {
                  Value v1;
                  Value v2;
                  OrderByType type;
                  for (const auto& group_by : plan_->GetOrderBy()) {
                      type = group_by.first;
                      const auto& expr = group_by.second;
                      v1 = expr->Evaluate(&t1, plan_->OutputSchema());
                      v2 = expr->Evaluate(&t2, plan_->OutputSchema());
                      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
                          continue;
                      }
                      if (type == OrderByType::DESC) {
                          return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
                      }
                      return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
                  }
                  return false;
              });
}

auto SortExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (index_ == static_cast<int>(tuples_.size())) {
        return false;
    }
    *tuple = tuples_[index_];
    *rid = tuple->GetRid();
    ++index_;
    return true;
}

}  // namespace bustub
