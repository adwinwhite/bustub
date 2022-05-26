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

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Get inserted value from child
  // We shouldn't return anything according to test.
  Tuple tu{};
  RID ri{};
  while (child_executor_->Next(&tu, &ri)) {
    auto modified_tuple = GenerateUpdatedTuple(tu);
    table_info_->table_->UpdateTuple(modified_tuple, ri, GetExecutorContext()->GetTransaction());
    for (auto idx : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      idx->index_->DeleteEntry(TransformTuple(&table_info_->schema_, &tu, idx->index_->GetKeySchema()), ri, GetExecutorContext()->GetTransaction());
      idx->index_->InsertEntry(TransformTuple(&table_info_->schema_, &modified_tuple, idx->index_->GetKeySchema()), ri, GetExecutorContext()->GetTransaction());
    }
  }   
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
