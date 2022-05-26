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

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
  // Get inserted value from child
  auto table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  Tuple tu{};
  RID ri{};
  while (child_executor_->Next(&tu, &ri)) {
    table_info_->table_->MarkDelete(ri, GetExecutorContext()->GetTransaction());
    for (auto idx : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      idx->index_->DeleteEntry(TransformTuple(&table_info_->schema_, &tu, idx->index_->GetKeySchema()), ri, GetExecutorContext()->GetTransaction());
    }
    // return true;
  }   
  return false;
}

}  // namespace bustub
