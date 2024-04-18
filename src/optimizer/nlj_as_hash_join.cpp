#include <algorithm>
#include <memory>
#include <optional>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef& plan)
    -> AbstractPlanNodeRef {
    // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
    // Note for 2023 Fall: You should support join keys of any number of
    // conjunction of equi-condistions: E.g. <column expr> = <column expr> AND
    // <column expr> = <column expr> AND ...
    std::vector<AbstractPlanNodeRef> children;
    for (const auto& child : plan->GetChildren()) {
        children.emplace_back(OptimizeNLJAsHashJoin(child));
    }
    auto optimized_plan = plan->CloneWithChildren(std::move(children));
    if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
        return optimized_plan;
    }
    const auto& nlj_plan =
        static_cast<NestedLoopJoinPlanNode*>(optimized_plan.get());
    auto extract_result = ExtractHashJoinKeyExpressions(nlj_plan);
    if (!extract_result.has_value()) {
        return optimized_plan;
    }
    const auto& left_exprs = extract_result->first;
    const auto& right_exprs = extract_result->second;
    return std::make_shared<HashJoinPlanNode>(
        nlj_plan->output_schema_, nlj_plan->GetLeftPlan(),
        nlj_plan->GetRightPlan(), left_exprs, right_exprs,
        nlj_plan->GetJoinType());
}

auto Optimizer::ExtractHashJoinKeyExpressions(
    const NestedLoopJoinPlanNode* plan)
    -> std::optional<std::pair<std::vector<AbstractExpressionRef>,
                               std::vector<AbstractExpressionRef>>> {
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    if (plan->predicate_ == nullptr ||
        !ExtractHashJoinKeyExpressions(plan->predicate_, left_exprs,
                                       right_exprs)) {
        return std::nullopt;
    }
    return std::make_optional(std::make_pair(left_exprs, right_exprs));
}

auto Optimizer::ExtractHashJoinKeyExpressions(
    const AbstractExpressionRef& expr,
    std::vector<AbstractExpressionRef>& left_exprs,
    std::vector<AbstractExpressionRef>& right_exprs) -> bool {
    if (auto cmp_expr = dynamic_cast<const ComparisonExpression*>(expr.get());
        cmp_expr != nullptr) {
        if (cmp_expr->comp_type_ != ComparisonType::Equal) {
            return false;
        }
        auto left_column_expr = dynamic_cast<const ColumnValueExpression*>(
            cmp_expr->children_[0].get());
        auto right_column_expr = dynamic_cast<const ColumnValueExpression*>(
            cmp_expr->children_[1].get());
        if (left_column_expr == nullptr || right_column_expr == nullptr) {
            return false;
        }
        if (left_column_expr->GetTupleIdx() == 0) {
            left_exprs.push_back(cmp_expr->children_[0]);
            right_exprs.push_back(cmp_expr->children_[1]);
        } else {
            left_exprs.push_back(cmp_expr->children_[1]);
            right_exprs.push_back(cmp_expr->children_[0]);
        }
        return true;
    }
    if (auto logic_expr = dynamic_cast<const LogicExpression*>(expr.get());
        logic_expr != nullptr) {
        if (logic_expr->logic_type_ != LogicType::And) {
            return false;
        }
        for (const auto& expr : logic_expr->children_) {
            if (!ExtractHashJoinKeyExpressions(expr, left_exprs, right_exprs)) {
                return false;
            }
        }
        return true;
    }

    return false;
}
}  // namespace bustub
