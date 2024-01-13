
#pragma once

#include <unordered_map>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {

struct NSW {
  using Vector = std::vector<double>;
  const std::vector<Vector> &vertices_;
  VectorExpressionType dist_fn_;
  std::unordered_map<size_t, std::vector<size_t>> edges_{};
  std::vector<size_t> in_vertices_{};
  auto FindNearestNeighbors(const std::vector<double> &base_vector, size_t limit,
                            const std::vector<size_t> &entry_points) -> std::vector<size_t>;
  auto Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m);
  auto SampleEntryPoints(size_t num_elements) -> std::vector<size_t>;
  auto AddVertex(size_t vertex_id);
  void Connect(size_t vertex_a, size_t vertex_b);
};

class HNSWIndex : public VectorIndex {
 public:
  HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
            VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options);

  ~HNSWIndex() override = default;

  void BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) override;
  auto ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> override;
  void InsertVectorEntry(const std::vector<double> &key, RID rid) override;

  auto AddVertex(const std::vector<double> &vec, RID rid) -> size_t;

  using Vector = std::vector<double>;
  std::unique_ptr<std::vector<Vector>> vertices_;
  std::vector<RID> rids_;
  NSW layer_;
  int m_;
  int ef_construction_;
  int ef_search_;
};

}  // namespace bustub
