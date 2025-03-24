#include <memory>
#include <optional>
#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "concurrency/transaction.h"
#include "execution/expressions/array_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/vector_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/vector_index_scan_plan.h"
#include "fmt/core.h"
#include "optimizer/optimizer.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

auto MatchVectorIndex(const Catalog &catalog, table_oid_t table_oid, uint32_t col_idx, VectorExpressionType dist_fn,
                      const std::string &vector_index_match_method) -> const IndexInfo * {

  std::string table_name = catalog.GetTable(table_oid)->name_;
  const std::vector<IndexInfo*> table_indices = catalog.GetTableIndexes(table_name);
  const IndexInfo* vector_idx = nullptr;
  if (vector_index_match_method == "none") {
    return vector_idx;
  }

  for (const auto* idx: table_indices) {
    if (idx->index_type_ != IndexType::VectorHNSWIndex && idx->index_type_ != IndexType::VectorIVFFlatIndex) {
      continue;
    }
    bool match_method_unset = vector_index_match_method == "unset" || vector_index_match_method == "";
    bool match_on_hnsw = vector_index_match_method == "hnsw" || match_method_unset;
    if (idx->index_type_ == IndexType::VectorHNSWIndex && !match_on_hnsw) {
      continue;
    }
    bool match_on_ivfflat = vector_index_match_method == "ivfflat" || match_method_unset;
    if (idx->index_type_ == IndexType::VectorIVFFlatIndex && !match_on_ivfflat) {
      continue;
    }
    if (dynamic_cast<const VectorIndex&>(*(idx->index_)).distance_fn_ != dist_fn) {
      continue;
    }
    if (idx->index_->GetKeyAttrs()[0] == col_idx) {
      vector_idx = idx;
      break;
    }
  }
  return vector_idx;
}

auto Optimizer::OptimizeAsVectorIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeAsVectorIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  const auto* topn_node = dynamic_cast<const TopNPlanNode*>(plan.get());
  if (topn_node == nullptr || topn_node->GetOrderBy().size() > 1) {
    return optimized_plan;
  }
  // The topn operator must be based on a vector distance expression.
  const auto* vector_orderby = dynamic_cast<const VectorExpression*>(topn_node->GetOrderBy()[0].second.get());
  if (vector_orderby == nullptr) {
    return optimized_plan;
  }
  // TopN followed by seq-scan or by projection then seq-scan.
  const auto* scan_node = dynamic_cast<const SeqScanPlanNode*>(plan->GetChildAt(0).get());
  const auto* projection_node = dynamic_cast<const ProjectionPlanNode*>(plan->GetChildAt(0).get());
  if (projection_node != nullptr) {
     scan_node = dynamic_cast<const SeqScanPlanNode*>(projection_node->GetChildPlan().get());
  }
  if (scan_node == nullptr) {
    // Not a pattern to optimize.
    return optimized_plan;
  }
  // Check if the column has an associated vector index
  const std::string& table_name = scan_node->table_name_;
  const std::vector<IndexInfo*> table_indices = catalog_.GetTableIndexes(table_name);
  // The first child of the vector expression should be a array expression and
  // the second a column value expression.
  const auto& vector_val_expr = dynamic_cast<const ArrayExpression&>(*vector_orderby->GetChildAt(0));
  const auto& vector_col_expr = dynamic_cast<const ColumnValueExpression&>(*vector_orderby->GetChildAt(1));
  // Find the vector column idx in the table.
  size_t target_vector_col_idx = vector_col_expr.GetColIdx();
  if (projection_node != nullptr) {
    const auto& col_expr = dynamic_cast<const ColumnValueExpression&>(
        *projection_node->GetExpressions()[target_vector_col_idx]);
    target_vector_col_idx = col_expr.GetColIdx();
  }

  const IndexInfo* vector_idx = MatchVectorIndex(
      catalog_,
      scan_node->table_oid_,
      target_vector_col_idx,
      vector_orderby->expr_type_,
      vector_index_match_method_
      );

  if (vector_idx == nullptr) {
    // No index create on the column
    return optimized_plan;
  }

  auto array_expr = std::make_unique<const ArrayExpression>(vector_val_expr);
  optimized_plan = std::make_unique<VectorIndexScanPlanNode>(plan->output_schema_,
                                             scan_node->table_oid_,
                                             scan_node->table_name_,
                                             vector_idx->index_oid_,
                                             vector_idx->name_,
                                             std::move(array_expr),
                                                 topn_node->GetN());
  // Consider projection
  if (projection_node != nullptr) {
    optimized_plan = std::make_unique<ProjectionPlanNode>(plan->output_schema_,
                                                          projection_node->GetExpressions(),
                                                          std::move(optimized_plan));
  }

  return optimized_plan;
}

}  // namespace bustub
