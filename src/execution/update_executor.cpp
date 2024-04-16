//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(
    ExecutorContext* exec_ctx,
    const UpdatePlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {
    // As of Fall 2022, you DON'T need to implement update executor to have
    // perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
    child_executor_->Init();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
    if (update_finish_) {
        return false;
    }
    int32_t n_updated = 0;
    std::vector<IndexInfo*> indexes =
        exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    Transaction* tx = GetExecutorContext()->GetTransaction();
    while (child_executor_->Next(tuple, rid)) {
        TupleMeta meta{tx->GetTransactionTempTs(), false};
        std::vector<Value> updated_values{};
        for (const auto& expression : plan_->target_expressions_) {
            Value v = expression->Evaluate(tuple, table_info_->schema_);
            updated_values.push_back(v);
        }
        Tuple updated_tuple{updated_values, &table_info_->schema_};
        bool updated =
            table_info_->table_->UpdateTupleInPlace(meta, updated_tuple, *rid);
        if (!updated) {
            continue;
        }
        n_updated++;
        for (auto index : indexes) {
            Tuple old_key =
                tuple->KeyFromTuple(table_info_->schema_, index->key_schema_,
                                    index->index_->GetKeyAttrs());
            Tuple new_key = updated_tuple.KeyFromTuple(
                table_info_->schema_, index->key_schema_,
                index->index_->GetKeyAttrs());
            index->index_->DeleteEntry(old_key, *rid, tx);
            index->index_->InsertEntry(new_key, *rid, tx);
        }
    }
    update_finish_ = true;
    std::vector<Value> values{Value(TypeId::INTEGER, n_updated)};
    *tuple = Tuple(values, plan_->output_schema_.get());
    return true;
}

}  // namespace bustub
