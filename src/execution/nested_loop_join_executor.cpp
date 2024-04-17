//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    ExecutorContext* exec_ctx,
    const NestedLoopJoinPlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& left_executor,
    std::unique_ptr<AbstractExecutor>&& right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
    if (plan->GetJoinType() != JoinType::LEFT &&
        plan->GetJoinType() != JoinType::INNER) {
        // Note for 2023 Fall: You ONLY need to implement left join and inner
        // join.
        throw bustub::NotImplementedException(
            fmt::format("join type {} not supported", plan->GetJoinType()));
    }
}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    RID rid;
    has_left_ = left_executor_->Next(&left_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (!has_left_) {
        return false;
    }
    Tuple right_tuple;
    RID right_rid;
    RID left_rid;
    const auto& predicate = plan_->predicate_;
    do {
        while (right_executor_->Next(&right_tuple, &right_rid)) {
            Value eval_result = predicate->EvaluateJoin(
                &left_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                &right_tuple, plan_->GetRightPlan()->OutputSchema());
            if (eval_result.CompareEquals({TypeId::BOOLEAN, 1}) ==
                CmpBool::CmpTrue) {
                *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);
                *rid = tuple->GetRid();
                left_joined_ = true;
                return true;
            }
        }
        if (plan_->GetJoinType() == JoinType::LEFT && !left_joined_) {
            *tuple = LeftJoinTuple(&left_tuple_, &right_tuple);
            *rid = tuple->GetRid();
            right_executor_->Init();
            has_left_ = left_executor_->Next(&left_tuple_, &left_rid);
            return true;
        }
        right_executor_->Init();
        has_left_ = left_executor_->Next(&left_tuple_, &left_rid);
        left_joined_ = false;
    } while (has_left_);
    return false;
}

auto NestedLoopJoinExecutor::InnerJoinTuple(Tuple* left, Tuple* right)
    -> Tuple {
    std::vector<Value> values;
    for (uint32_t idx = 0;
         idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(
            left->GetValue(&left_executor_->GetOutputSchema(), idx));
    }
    for (uint32_t idx = 0;
         idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(
            right->GetValue(&right_executor_->GetOutputSchema(), idx));
    }
    return Tuple{values, &GetOutputSchema()};
}
}  // namespace bustub

auto bustub::NestedLoopJoinExecutor::LeftJoinTuple(Tuple* left, Tuple* right)
    -> Tuple {
    std::vector<Value> values;
    for (uint32_t idx = 0;
         idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(
            left->GetValue(&left_executor_->GetOutputSchema(), idx));
    }
    for (uint32_t idx = 0;
         idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(ValueFactory::GetNullValueByType(
            right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
    }
    return Tuple{values, &GetOutputSchema()};
}
