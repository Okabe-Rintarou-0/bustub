//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <ios>
#include <ostream>
#include "binder/table_ref/bound_join_ref.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(
    ExecutorContext* exec_ctx,
    const HashJoinPlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& left_child,
    std::unique_ptr<AbstractExecutor>&& right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      left_hjt_(plan->left_key_expressions_),
      right_hjt_(plan->right_key_expressions_),
      left_iter_(left_hjt_.Begin()) {
    if (plan->GetJoinType() != JoinType::LEFT &&
        plan->GetJoinType() != JoinType::INNER) {
        // Note for 2023 Fall: You ONLY need to implement left join and inner
        // join.
        throw bustub::NotImplementedException(
            fmt::format("join type {} not supported", plan->GetJoinType()));
    }
}

void HashJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();

    Tuple left_tuple;
    Tuple right_tuple;
    RID left_rid;
    RID right_rid;
    while (left_executor_->Next(&left_tuple, &left_rid)) {
        left_hjt_.InsertTuple(left_tuple, plan_->GetLeftPlan()->OutputSchema());
    }
    while (right_executor_->Next(&right_tuple, &right_rid)) {
        right_hjt_.InsertTuple(right_tuple,
                               plan_->GetRightPlan()->OutputSchema());
    }
    left_iter_ = left_hjt_.Begin();
    left_tuple_idx_ = right_tuple_idx_ = 0;
}

auto HashJoinExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (left_iter_ == left_hjt_.End()) {
        return false;
    }

    bool meet_right_end =
        (right_tuples_ != nullptr && right_tuple_idx_ == right_tuples_->size());
    bool right_not_exists = !right_hjt_.Exists(left_iter_.Key());
    while (right_not_exists || meet_right_end) {
        const auto left_tuples = left_iter_.Val().values_;
        if (right_not_exists && plan_->join_type_ == JoinType::LEFT &&
            left_tuple_idx_ < left_tuples.size()) {
            const auto& left_tuple = left_tuples[left_tuple_idx_];
            *tuple = LeftJoinTuple(&left_tuple);
            *rid = tuple->GetRid();
            ++left_tuple_idx_;
            return true;
        }
        if (left_tuple_idx_ + 1 == left_tuples.size() || right_not_exists) {
            ++left_iter_;
            left_tuple_idx_ = 0;
            right_tuple_idx_ = 0;
            meet_right_end = false;
            if (left_iter_ == left_hjt_.End()) {
                return false;
            }
            right_not_exists = !right_hjt_.Exists(left_iter_.Key());
        } else {
            meet_right_end = false;
            right_tuple_idx_ = 0;
            left_tuple_idx_++;
        }
    }
    right_tuples_ = &right_hjt_.Get(left_iter_.Key()).values_;
    const auto& left_tuple = left_iter_.Val().values_[left_tuple_idx_];
    const auto& right_tuple = (*right_tuples_)[right_tuple_idx_];
    right_tuple_idx_++;
    *tuple = InnerJoinTuple(&left_tuple, &right_tuple);
    *rid = tuple->GetRid();
    return true;
}

auto HashJoinExecutor::LeftJoinTuple(const Tuple* left) -> Tuple {
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

auto HashJoinExecutor::InnerJoinTuple(const Tuple* left, const Tuple* right)
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
void SimpleHashJoinTable::InsertTuple(const Tuple& tuple,
                                      const Schema& schema) {
    HashJoinKey key;
    for (const auto& expr : key_exprs_) {
        key.keys_.push_back(expr->Evaluate(&tuple, schema));
    }
    if (ht_.count(key) == 0) {
        ht_.insert({key, {}});
    }
    ht_[key].values_.push_back(tuple);
}
}  // namespace bustub
