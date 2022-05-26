//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {


HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  // Initialize children.
  left_child_->Init();
  right_child_->Init();

  // Build outer hash table 
  Tuple tuple{};
  RID rid{};
  while (left_child_->Next(&tuple, &rid)) {
    outer_hash_table_.emplace(plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema()), tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // Iterate over inner table.
  // First get saved inner tuple.
  RID inner_rid;
  // Check if inner tuple exists.
  if (inner_tuple_.GetLength() == 0) {
    // If not exists, load a new one.
    if (!right_child_->Next(&inner_tuple_, &inner_rid)) {
      // No more tuple.
      return false;
    }
  }

  // Get right join key.
  auto right_key = plan_->RightJoinKeyExpression()->Evaluate(&inner_tuple_, right_child_->GetOutputSchema());
  // Check if inner iterator runs to end.
  if (inner_ite_table_[inner_tuple_] == outer_hash_table_.equal_range(right_key).second) {
    // Load one new inner tuple and update iterator in inner_ite_table_.
    if (!right_child_->Next(&inner_tuple_, &inner_rid)) {
      // No more tuple.
      return false;
    }
    auto new_right_key = plan_->RightJoinKeyExpression()->Evaluate(&inner_tuple_, right_child_->GetOutputSchema());
    inner_ite_table_.insert({inner_tuple_, outer_hash_table_.equal_range(right_key).first});
  }

  // Now we have valid inner tuple.
  right_key = plan_->RightJoinKeyExpression()->Evaluate(&inner_tuple_, right_child_->GetOutputSchema());
  auto outer_tuple_ite = outer_hash_table_.equal_range(right_key).first;
  *tuple = ConcatenateTuple(outer_tuple_ite->second, left_child_->GetOutputSchema(), inner_tuple_, right_child_->GetOutputSchema());
  // Will this really increase the interator? Yes, just believe so.
  // Update inner_ite_table_. 
  outer_tuple_ite++;
  inner_ite_table_[inner_tuple_] = outer_tuple_ite;
  return true;
}

Tuple HashJoinExecutor::ConcatenateTuple(const Tuple &tuple1, const Schema *schema1, const Tuple &tuple2, const Schema *schema2) {
  std::vector<Value> values;
  for (uint32_t i = 0; i < schema1->GetColumnCount(); i++) {
    values.push_back(tuple1.GetValue(schema1, i));
  }
  for (uint32_t i = 0; i < schema2->GetColumnCount(); i++) {
    values.push_back(tuple2.GetValue(schema2, i));
  }
  return Tuple{values, GetOutputSchema()};
}

}  // namespace bustub
