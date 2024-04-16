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

#include "execution/executors/filter_executor.h"
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
    Transaction* tx = GetExecutorContext()->GetTransaction();
    int32_t n_inserted = 0;
    std::vector<IndexInfo*> indexes =
        exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    Tuple child_tuple;
    while (child_executor_->Next(&child_tuple, rid)) {
        TupleMeta meta{0, false};
        std::optional<RID> rid_optional =
            table_info_->table_->InsertTuple(meta, child_tuple);
        if (!rid_optional.has_value()) {
            continue;
        }
        *rid = rid_optional.value();
        n_inserted++;
        for (auto index : indexes) {
            Tuple key = child_tuple.KeyFromTuple(table_info_->schema_,
                                                 index->key_schema_,
                                                 index->index_->GetKeyAttrs());
            index->index_->InsertEntry(key, *rid, tx);
        }
    }
    insert_finish_ = true;
    std::vector<Value> values{Value(TypeId::INTEGER, n_inserted)};
    *tuple = Tuple(values, plan_->output_schema_.get());
    return true;
}

}  // namespace bustub
