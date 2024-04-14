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
    TableInfo* table_info = catalog->GetTable(table_id);
    table_iter_ =
        std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (table_iter_->IsEnd()) {
        return false;
    }
    *tuple = table_iter_->GetTuple().second;
    *rid = table_iter_->GetRID();
    ++(*table_iter_);
    return true;
}

}  // namespace bustub
