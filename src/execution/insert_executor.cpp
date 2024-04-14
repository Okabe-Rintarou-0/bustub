//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "execution/executors/values_executor.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(
    ExecutorContext* exec_ctx,
    const InsertPlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    table_oid_t table_id = plan_->GetTableOid();
    Catalog* catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(table_id);
    child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
    if (insert_finish_) {
        return false;
    }
    auto values_plan =
        static_cast<const ValuesPlanNode*>(plan_->GetChildPlan().get());
    auto raw_values = values_plan->GetValues();
    int32_t n_inserted = 0;
    std::vector<IndexInfo*> indexes =
        exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    while (child_executor_->Next(tuple, rid)) {
        TupleMeta meta{0, false};
        std::optional<RID> rid = table_info_->table_->InsertTuple(meta, *tuple);
        if (!rid.has_value()) {
            continue;
        }

        n_inserted++;
        for (auto index : indexes) {
            index->index_->InsertEntry(*tuple, rid.value(), nullptr);
        }
    }
    insert_finish_ = true;
    std::vector<Value> values{Value(TypeId::INTEGER, n_inserted)};
    *tuple = Tuple(values, plan_->output_schema_.get());
    return true;
}

}  // namespace bustub
