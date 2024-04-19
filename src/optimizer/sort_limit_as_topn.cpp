#include <memory>
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef& plan)
    -> AbstractPlanNodeRef {
    // TODO(student): implement sort + limit -> top N optimizer rule
    std::vector<AbstractPlanNodeRef> children;
    for (const auto& child : plan->GetChildren()) {
        children.emplace_back(OptimizeSortLimitAsTopN(child));
    }
    auto optimized_plan = plan->CloneWithChildren(std::move(children));

    auto limit_plan = dynamic_cast<const LimitPlanNode*>(plan.get());
    if (limit_plan == nullptr) {
        return optimized_plan;
    }

    auto sort_plan =
        dynamic_cast<const SortPlanNode*>(limit_plan->children_[0].get());
    if (limit_plan == nullptr) {
        return optimized_plan;
    }

    const auto& child = OptimizeSortLimitAsTopN(limit_plan->children_[0]);
    return std::make_shared<TopNPlanNode>(
        plan->output_schema_, child, sort_plan->order_bys_, limit_plan->limit_);
}
}  // namespace bustub
