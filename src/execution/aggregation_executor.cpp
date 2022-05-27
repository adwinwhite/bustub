//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan->GetAggregates(), plan->GetAggregateTypes()), aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // Initialize the child.
  child_->Init();

  // Build the multimap if group_by_col is not empty.
  Tuple child_tuple;
  RID child_rid;
  while (child_->Next(&child_tuple, &child_rid)) {
    auto key = MakeAggregateKey(&child_tuple);
    auto value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
  }
  // Update table iterator.
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  // Check whether iterator reaches end.
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> output_values;
  auto group_bys = aht_iterator_.Key().group_bys_;
  auto aggregates = aht_iterator_.Val().aggregates_;
  ++aht_iterator_;
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    output_values.push_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
  }
  auto output_tuple = Tuple(output_values, GetOutputSchema());

  // Check having clause.
  if (plan_->GetHaving() == nullptr) {
    *tuple = output_tuple;
  } else {
    while (!plan_->GetHaving()->Evaluate(&output_tuple, GetOutputSchema()).GetAs<bool>()) {
      if (aht_iterator_ == aht_.End()) {
        return false;
      }
      output_values.clear();
      group_bys = aht_iterator_.Key().group_bys_;
      aggregates = aht_iterator_.Val().aggregates_;
      ++aht_iterator_;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        output_values.push_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
      }
      output_tuple = Tuple(output_values, GetOutputSchema());
    }
    *tuple = output_tuple;
  }
  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
