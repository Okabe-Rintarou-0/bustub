#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(
    const bustub::AbstractPlanNodeRef& plan) -> AbstractPlanNodeRef {
    // TODO(student): implement seq scan with predicate -> index scan optimizer
    // rule The Filter Predicate Pushdown has been enabled for you in
    // optimizer.cpp when forcing starter rule
    std::vector<AbstractPlanNodeRef> children;
    for (const auto& child : plan->GetChildren()) {
        children.emplace_back(OptimizeSeqScanAsIndexScan(child));
    }

    auto optimized_plan = plan->CloneWithChildren(std::move(children));
    if (optimized_plan->GetType() == PlanType::SeqScan) {
        auto seq_scan_plan =
            dynamic_cast<const SeqScanPlanNode*>(optimized_plan.get());
        auto predicate = seq_scan_plan->filter_predicate_;
        if (predicate == nullptr) {
            return optimized_plan;
        }

        const ColumnValueExpression* column_expr{};
        std::vector<AbstractExpressionRef> pred_keys;
        if (!GetColumnAndPredKeys(predicate, &column_expr, pred_keys)) {
            return optimized_plan;
        }

        TableInfo* table_info = catalog_.GetTable(seq_scan_plan->table_oid_);
        auto match_result =
            MatchIndex(table_info->name_, column_expr->GetColIdx());
        if (match_result.has_value()) {
            auto index = match_result.value();
            return std::make_shared<IndexScanPlanNode>(
                seq_scan_plan->output_schema_, table_info->oid_,
                std::get<0>(index), predicate, pred_keys);
        }
    }
    return optimized_plan;
}

auto bustub::Optimizer::GetColumnAndPredKeys(
    const AbstractExpressionRef& expr,
    const ColumnValueExpression** column_expr_ptr,
    std::vector<AbstractExpressionRef>& pred_keys) -> bool {
    if (auto cmp_expr = dynamic_cast<const ComparisonExpression*>(expr.get());
        cmp_expr != nullptr) {
        if (cmp_expr->comp_type_ != ComparisonType::Equal) {
            return false;
        }
        auto expr_pair = GetColumnEqualExpressionPair(cmp_expr);
        if (!expr_pair.has_value()) {
            return false;
        }
        auto column_expr = expr_pair->first;
        auto value_expr = expr_pair->second;
        const auto const_expr =
            dynamic_cast<const ConstantValueExpression*>(value_expr.get());
        if (const_expr == nullptr) {
            return false;
        }
        Schema dummy({});
        bool existed{};
        for (const auto& pred_key : pred_keys) {
            if (pred_key->Evaluate(nullptr, dummy)
                    .CompareEquals(const_expr->Evaluate(nullptr, dummy)) ==
                CmpBool::CmpTrue) {
                existed = true;
                break;
            }
        }
        if (!existed) {
            pred_keys.push_back(expr_pair->second);
        }
        if (*column_expr_ptr == nullptr) {
            *column_expr_ptr = column_expr;
        } else if ((*column_expr_ptr)->GetColIdx() !=
                   column_expr->GetColIdx()) {
            return false;
        }
        return true;
    }
    if (auto logic_expr = dynamic_cast<const LogicExpression*>(expr.get());
        logic_expr != nullptr) {
        if (logic_expr->logic_type_ != LogicType::Or) {
            return false;
        }

        for (const auto& child : logic_expr->children_) {
            if (!GetColumnAndPredKeys(child, column_expr_ptr, pred_keys)) {
                return false;
            }
        }
        return true;
    }
    return false;
}

auto bustub::Optimizer::GetColumnEqualExpressionPair(
    const ComparisonExpression* cmp_expr)
    -> std::optional<
        std::pair<const ColumnValueExpression*, AbstractExpressionRef>> {
    const ColumnValueExpression* column_expr{};
    AbstractExpressionRef value_expr{};
    auto child_left = dynamic_cast<const ColumnValueExpression*>(
        cmp_expr->children_[0].get());
    auto child_right = dynamic_cast<const ColumnValueExpression*>(
        cmp_expr->children_[1].get());
    if (child_left != nullptr && child_right != nullptr) {
        return std::nullopt;
    }

    if (child_left != nullptr) {
        column_expr = child_left;
        value_expr = cmp_expr->children_[1];
    } else if (child_right != nullptr) {
        column_expr = child_right;
        value_expr = cmp_expr->children_[0];
    } else {
        return std::nullopt;
    }

    return std::make_optional(std::make_pair(column_expr, value_expr));
}
}  // namespace bustub
