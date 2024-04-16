//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(
    ExecutorContext* exec_ctx,
    const DeletePlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
    if (delete_finish_) {
        return false;
    }
    int32_t n_deleted = 0;
    std::vector<IndexInfo*> indexes =
        exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    Transaction* tx = GetExecutorContext()->GetTransaction();
    Tuple child_tuple;
    while (child_executor_->Next(&child_tuple, rid)) {
        table_info_->table_->UpdateTupleMeta(
            TupleMeta{tx->GetTransactionTempTs(), true}, *rid);
        n_deleted++;
        for (auto index : indexes) {
            Tuple key = child_tuple.KeyFromTuple(table_info_->schema_,
                                                 index->key_schema_,
                                                 index->index_->GetKeyAttrs());
            index->index_->DeleteEntry(key, *rid, tx);
        }
    }
    delete_finish_ = true;
    std::vector<Value> values{Value(TypeId::INTEGER, n_deleted)};
    *tuple = Tuple(values, plan_->output_schema_.get());
    return true;
}

}  // namespace bustub
