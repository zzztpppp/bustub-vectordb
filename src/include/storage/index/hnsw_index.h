
#pragma once

#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {

class HNSWIndex : public VectorIndex {
 public:
  HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
            VectorExpressionType distance_fn, std::vector<std::pair<std::string, int>> options);

  ~HNSWIndex() override = default;

  void BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) override;
  auto ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> override;
  void InsertVectorEntry(const std::vector<double> &key, RID rid) override;

  std::unique_ptr<IndexMetadata> metadata_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
