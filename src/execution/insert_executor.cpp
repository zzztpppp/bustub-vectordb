//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (emitted_) {
    return false;
  }
  Tuple t;
  RID r;
  TupleMeta meta{0, false};
  int count = 0;
  Catalog *catalog = exec_ctx_->GetCatalog();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  TableHeap &table_heap = *table_info->table_;
  auto table_indexes = catalog->GetTableIndexes(table_info->name_);
  while (true) {
    bool get = child_executor_->Next(&t, &r);
    if (!get) {
      emitted_ = true;
      break;
    }
    ++count;
    r = table_heap.InsertTuple(meta, t).value();
    // Construct index-key tuple

    for (auto &idx : table_indexes) {
      IndexMetadata *idx_meta = idx->index_->GetMetadata();
      const std::vector<uint32_t> &key_attr = idx_meta->GetKeyAttrs();
      std::vector<Value> key_values;
      key_values.reserve(key_attr.size());
      for (auto i : key_attr) {
        key_values.emplace_back(t.GetValue(&table_info->schema_, i));
      }
      Tuple key_tuple(key_values, idx_meta->GetKeySchema());
      auto* vector_idx = dynamic_cast<VectorIndex*>(idx->index_.get());
      if (vector_idx != nullptr) {
        vector_idx->InsertVectorEntry(key_values[0].GetVector(), r);
      } else {
        idx->index_->InsertEntry(key_tuple, r, exec_ctx_->GetTransaction());
      }
    }
  }
  // A tuple contains an integer value indicates how many tuples were inserted.
  Schema output_schema(std::vector<Column>{{"output", TypeId::INTEGER}});
  Tuple output_tuple(std::vector<Value>{Value(TypeId::INTEGER, count)}, &output_schema);

  *tuple = output_tuple;
  return true;
}

}  // namespace bustub
