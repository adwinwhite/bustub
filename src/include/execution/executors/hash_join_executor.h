//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace std {
template <>
struct std::hash<bustub::Tuple> {
  size_t operator()(const bustub::Tuple &tuple) const {
    return bustub::HashUtil::HashBytes(tuple.GetData(), tuple.GetLength());
  }
};
}


namespace bustub {

// struct TupleKey {
  // Tuple tuple_;

  // bool operator==(const TupleKey &other) const {
    // if (tuple_.GetLength() != other.tuple_.GetLength()) {
      // return false;
    // }
    // for (uint32_t i = 0; i < tuple_.GetLength(); i++) {
      // if (tuple_.GetData()[i] != other.tuple_.GetData()[i]) {
        // return false;
      // }
    // }
    // return true;
  // }
// };


/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  Tuple ConcatenateTuple(const Tuple &tuple1, const Schema &scheme1, const Tuple &tuple2, const Schema &scheme2);
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::multimap<Value, Tuple> outer_hash_table_{};
  std::unordered_map<Tuple, std::multimap<Value, Tuple>::iterator> inner_ite_table_;
  Tuple inner_tuple_{};
};

}  // namespace bustub
//
