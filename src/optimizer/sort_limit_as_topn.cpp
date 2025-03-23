#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  if (plan->GetChildren().empty()) {
    return plan;
  }

  const auto* limit_plan = dynamic_cast<const LimitPlanNode*>(plan.get());
  bool single_child = plan->GetChildren().size() == 1;
  const auto* sort_plan_child = dynamic_cast<const SortPlanNode*>(plan->GetChildAt(0).get());

  if (limit_plan == nullptr || !single_child || sort_plan_child == nullptr) {
    std::vector<AbstractPlanNodeRef> optimized_children;
    for (const auto& child: plan->GetChildren()) {
      optimized_children.emplace_back(OptimizeSortLimitAsTopN(child));
    }
     return plan->CloneWithChildren(optimized_children);
  }

  return std::make_unique<TopNPlanNode>(plan->output_schema_, sort_plan_child->GetChildPlan(), sort_plan_child->GetOrderBy(), limit_plan->GetLimit());
}

}  // namespace bustub
