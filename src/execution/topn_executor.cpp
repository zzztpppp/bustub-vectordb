#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void TopNExecutor::Init() {
  child_executor_->Init();
  const std::pair<OrderByType, AbstractExpressionRef> orderby = plan_->GetOrderBy()[0];
  Schema child_schema = child_executor_->GetOutputSchema();
  auto lambda = [&orderby, &child_schema](const Tuple& lhs, const Tuple& rhs) {
    const auto& [orderby_type, orderby_expr] = orderby;
    Value lhs_v = orderby_expr->Evaluate(&lhs, child_schema);
    Value rhs_v = orderby_expr->Evaluate(&rhs, child_schema);
    bool cmp_result = lhs_v.CompareLessThan(rhs_v) == CmpBool::CmpTrue;
    return !cmp_result;
  };
  std::vector<Tuple> tuples;
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(lambda)> pq(lambda);
  Tuple t;
  RID r;
  while (true) {
    bool get = child_executor_->Next(&t, &r);
    if (!get) {
      break;
    }
    pq.push(std::move(t));
  }
  for (size_t i = 0; i < plan_->GetN(); ++i) {
    if (pq.empty()) {
      break;
    }
    topn_.emplace_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (emitted_ >= plan_->GetN())  {
    return false;
  }
  if (emitted_ >= topn_.size()) {
    return false;
  }

  *tuple = topn_[emitted_++];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  return topn_.size();
}

}  // namespace bustub
