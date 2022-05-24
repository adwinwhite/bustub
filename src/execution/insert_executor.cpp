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

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  raw_idx_ = 0;
  // Init child executor.
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}


bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
  // Insert one by one.
  // How to insert? Use TableHeap and update index.
  // How to insert with tableheap?
  // How to update index?
  // Match raw insert or from child.
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  if (plan_->IsRawInsert()) {
    if (raw_idx_ < plan_->RawValues().size()) {
      // Insert
      Tuple tu{plan_->RawValuesAt(raw_idx_), &table_info->schema_};
      RID ri{};
      table_info->table_->InsertTuple(tu, &ri, GetExecutorContext()->GetTransaction());
      for (auto idx : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_)) {
        idx->index_->InsertEntry(tu, ri, GetExecutorContext()->GetTransaction());
      }
      raw_idx_++;
      return true;
    } else {
      return false;
    }
  } else {
    // Get inserted value from child
    Tuple tu{};
    RID ri{};
    if (child_executor_->Next(&tu, &ri)) {
      table_info->table_->InsertTuple(tu, &ri, GetExecutorContext()->GetTransaction());
      for (auto idx : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_)) {
        idx->index_->InsertEntry(tu, ri, GetExecutorContext()->GetTransaction());
      }
      return true;
    } else {
      return false;
    }
  }
}

}  // namespace bustub
