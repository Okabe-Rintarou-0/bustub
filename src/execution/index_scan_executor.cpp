//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext* exec_ctx,
                                     const IndexScanPlanNode* plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn*>(
        index_info_->index_.get());
    Tuple tuple{};
    Transaction* tx = GetExecutorContext()->GetTransaction();
    for (const auto& value_expr : plan_->pred_keys_) {
        Value v = value_expr->Evaluate(nullptr, table_info_->schema_);
        index_info_->index_->ScanKey(
            Tuple{{v}, index_info_->index_->GetKeySchema()}, &rids_, tx);
    }
}

auto IndexScanExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    size_t n_rids = rids_.size();
    if (index_ == n_rids) {
        return false;
    }
    TupleMeta meta;

    bool meet_end;
    do {
        *rid = rids_[index_++];
        const auto& tuple_pair = table_info_->table_->GetTuple(*rid);
        meta = tuple_pair.first;
        *tuple = tuple_pair.second;
        meet_end = index_ < rids_.size();
    } while (meta.is_deleted_ && !meet_end);
    return !meta.is_deleted_;
}
}  // namespace bustub
