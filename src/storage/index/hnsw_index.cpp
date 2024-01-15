#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/macros.h"
#include "execution/expressions/vector_expression.h"
#include "fmt/format.h"
#include "fmt/std.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
HNSWIndex::HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                     VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn),
      vertices_(std::make_unique<std::vector<Vector>>()),
      layer_{*vertices_, distance_fn} {
  std::optional<size_t> m;
  std::optional<size_t> ef_construction;
  std::optional<size_t> ef_search;
  for (const auto &[k, v] : options) {
    if (k == "m") {
      m = v;
    } else if (k == "ef_construction") {
      ef_construction = v;
    } else if (k == "ef_search") {
      ef_search = v;
    }
  }
  if (!m.has_value() || !ef_construction.has_value() || !ef_search.has_value()) {
    throw Exception("missing options: m / ef_construction / ef_search for hnsw index");
  }
  ef_construction_ = *ef_construction;
  m_ = *m;
  ef_search_ = *ef_search;
}

auto NSW::FindNearestNeighbors(const std::vector<double> &base_vector, size_t limit,
                               const std::vector<size_t> &entry_points) -> std::vector<size_t> {
  BUSTUB_ASSERT(limit > 0, "limit > 0");
  return {};
}

auto NSW::AddVertex(size_t vertex_id) { in_vertices_.push_back(vertex_id); }

auto NSW::Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m) {
  auto neighbors = FindNearestNeighbors(vec, m, SampleEntryPoints(ef_construction));
  for (size_t i = 0; i < m && i < neighbors.size(); i++) {
    Connect(vertex_id, neighbors[i]);
  }
  AddVertex(vertex_id);
}

void NSW::Connect(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

auto NSW::SampleEntryPoints(size_t num_elements) -> std::vector<size_t> { return {}; }

auto HNSWIndex::AddVertex(const std::vector<double> &vec, RID rid) -> size_t {
  auto id = vertices_->size();
  vertices_->emplace_back(vec);
  rids_.emplace_back(rid);
  return id;
}

void HNSWIndex::BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) {}

auto HNSWIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> { return {}; }

void HNSWIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {}

}  // namespace bustub
