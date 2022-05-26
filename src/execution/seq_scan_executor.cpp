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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_ite_ = std::make_unique<TableIterator>(table_info_->table_->Begin(GetExecutorContext()->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // First evaluate predicate. How?
  // We get the predicate expression from plan node, then feed tuple into it.
  // What value can we get? Tuple.
  // How to get rid? Tuple contains rid.
  while (*table_ite_ != table_info_->table_->End()) {
    auto old_tuple = **table_ite_;
    *rid = old_tuple.GetRid();
    *tuple = TransformTuple(&table_info_->schema_, &old_tuple, GetOutputSchema());
    (*table_ite_)++;
    if (plan_->GetPredicate()) {
      if (plan_->GetPredicate()->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
        return true;
      }
    } else {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
