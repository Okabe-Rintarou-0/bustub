//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(
    ExecutorContext* exec_ctx,
    const AggregationPlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
    child_executor_->Init();
    Tuple child_tuple;
    RID rid;
    size_t count = 0;
    aht_.Clear();
    while (child_executor_->Next(&child_tuple, &rid)) {
        aht_.InsertCombine(MakeAggregateKey(&child_tuple),
                           MakeAggregateValue(&child_tuple));
        auto key = MakeAggregateKey(&child_tuple);
        count++;
    }
    if (plan_->GetGroupBys().empty() && count == 0) {
        aht_.InsertEmpty(MakeAggregateKey(&child_tuple));
    }
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (aht_iterator_ == aht_.End()) {
        return false;
    }
    std::vector<Value> values = aht_iterator_.Key().group_bys_;
    for (const auto& agg : aht_iterator_.Val().aggregates_) {
        values.push_back(agg);
    }
    Tuple out_tuple{values, &GetOutputSchema()};
    *tuple = out_tuple;
    ++aht_iterator_;
    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor* {
    return child_executor_.get();
}

}  // namespace bustub
