//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext* exec_ctx,
                                 const SeqScanPlanNode* plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    table_oid_t table_id = plan_->GetTableOid();
    Catalog* catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(table_id);
    table_iter_ =
        std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (table_iter_->IsEnd()) {
        return false;
    }

    bool match_predicate{};
    bool has_next{};
    TupleMeta meta;
    do {
        auto tuple_pair = table_iter_->GetTuple();
        *tuple = tuple_pair.second;
        meta = tuple_pair.first;
        *rid = table_iter_->GetRID();
        ++(*table_iter_);
        match_predicate =
            !meta.is_deleted_ &&
            (plan_->filter_predicate_ == nullptr ||
             plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_)
                 .GetAs<bool>());
        has_next = !table_iter_->IsEnd();
    } while (has_next && !match_predicate);
    return match_predicate;
}

}  // namespace bustub
