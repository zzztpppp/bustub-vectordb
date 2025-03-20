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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  bool found{false};
  if (iter_->IsEnd()) {
    return false;
  }
  auto schema = &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_);
  auto predicate = plan_->filter_predicate_;
  bool predicate_result = true;
  while (true) {
    auto [m, t] = iter_->GetTuple();
    if (predicate != nullptr) {
      predicate_result = predicate->Evaluate(&t, *schema).GetAs<bool>();
    }
    if (!m.is_deleted_ && predicate_result) {
      found = true;
      *tuple = t;
      *rid = iter_->GetRID();
      break;
    }

    ++(*iter_);
    if (iter_->IsEnd()) {
      return false;
    }
  }

  ++(*iter_);
  return found;
}


}  // namespace bustub
