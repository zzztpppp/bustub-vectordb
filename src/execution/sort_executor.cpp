#include "execution/executors/sort_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"


namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void SortExecutor::Init() {
  // Assume only one column
  child_executor_->Init();
  const std::pair<OrderByType, AbstractExpressionRef> orderby = plan_->GetOrderBy()[0];
  Schema child_schema = child_executor_->GetOutputSchema();
  auto lambda = [&orderby, &child_schema](const Tuple& lhs, const Tuple& rhs) {
    const auto& [orderby_type, orderby_expr] = orderby;
    Value lhs_v = orderby_expr->Evaluate(&lhs, child_schema);
    Value rhs_v = orderby_expr->Evaluate(&rhs, child_schema);
    bool cmp_result = lhs_v.CompareLessThan(rhs_v) == CmpBool::CmpTrue;
    return cmp_result;
  };
  std::vector<Tuple> tuples;
  Tuple t;
  RID r;
  while (true) {
    bool get = child_executor_->Next(&t, &r);
    if (!get) {
      break;
    }
    tuples.emplace_back(std::move(t));
  }
  std::sort(tuples.begin(), tuples.end(), lambda);
  sorted_tuples_ = std::move(tuples);
  tuple_iter_ = std::make_unique<std::vector<Tuple>::iterator>(sorted_tuples_.begin());
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*tuple_iter_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = **tuple_iter_;
  (*tuple_iter_)++;
  return true;
}

}  // namespace bustub
