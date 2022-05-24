//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) { 
  // How can I iterate inner table multiple times? Get all tuples in Init()? Or use children's Init() to reset iterator?
  // By the way, how to determine inner vs outer table? We don't know sizes. Get all tuples of both tables in Init()?
  // Use children's Init() to reset.
  // But still no way to determine size? Whatever.
  // Loop inner iterator until predicate satisfied. Advance outer iterator if end.
  //
  // Outer tuple may not be initialized.
  if (outer_rid_.GetPageId() == INVALID_PAGE_ID) {
    if (!left_executor_->Next(&outer_tuple_, &outer_rid_)) {
      return false;
    }
  }
  Tuple inner_tuple{};
  RID inner_rid{};
  while (true) {
    // Check whether inner iterator reaches end
    if (!right_executor_->Next(&inner_tuple, &inner_rid)) {
      // Advance outer iterator.
      if (!left_executor_->Next(&outer_tuple_, &outer_rid_)) {
        // Outer iterator also reaches end. No more tuple to emit.
        return false;
      }
      // Reset inner iterator.
      right_executor_->Init();
      // Load inner tuple
      if (!right_executor_->Next(&inner_tuple, &inner_rid)) {
        // It means right_executor has 0 size.
        return false;
      }
    }
    // Now we have valid inner tuple and outer tuple. 
    // It's time to check predicate.
    if (plan_->Predicate()->EvaluateJoin(&outer_tuple_, left_executor_->GetOutputSchema(), &inner_tuple, right_executor_->GetOutputSchema()).GetAs<bool>()) {
      // Where to specify the common key
      *tuple = ConcatenateTuple(outer_tuple_, *left_executor_->GetOutputSchema(), inner_tuple, *right_executor_->GetOutputSchema());
      return true;
    }
  }
}

Tuple NestedLoopJoinExecutor::ConcatenateTuple(const Tuple &tuple1, const Schema &schema1, const Tuple &tuple2, const Schema &schema2) {
  std::vector<Value> values;
  for (uint32_t i = 0; i < schema1.GetColumnCount(); i++) {
    values.push_back(tuple1.GetValue(&schema1, i));
  }
  for (uint32_t i = 0; i < schema2.GetColumnCount(); i++) {
    values.push_back(tuple2.GetValue(&schema2, i));
  }
  return Tuple{values, GetOutputSchema()};
}

}  // namespace bustub
