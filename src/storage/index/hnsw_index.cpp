#include <cmath>
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
      layers_{{*vertices_, distance_fn}} {
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
  m_max_ = m_;
  m_max_0_ = m_ * m_;
  layers_[0].m_max_ = m_max_0_;
  m_l_ = 1.0 / std::log(m_);
  std::random_device rand_dev;
  generator_ = std::mt19937(rand_dev());
}

auto SelectNeighbors(const std::vector<double> &vec, const std::vector<size_t> &vertex_ids,
                     const std::vector<std::vector<double>> &vertices, size_t m, VectorExpressionType dist_fn)
    -> std::vector<size_t> {
  using Node  = std::pair<double, size_t>;
  auto cmp = [](const Node& n1, const Node& n2) {return n1.first > n2.first;};
  std::vector<Node> node_distances;
  node_distances.reserve(vertex_ids.size());
  for (const auto& vid: vertex_ids)  {
    double d = ComputeDistance(vec, vertices[vid], dist_fn);
    node_distances.emplace_back(d, vid);
  }
  std::priority_queue<Node, std::vector<Node>, decltype(cmp)> node_distance_pq(node_distances.begin(), node_distances.end(), cmp);
  std::vector<size_t> result;
  result.reserve(m);
  for (size_t i = 0; i < m; ++i) {
    if (node_distance_pq.empty()) {
      break;
    }
    result.emplace_back(node_distance_pq.top().second);
    node_distance_pq.pop();
  }
  return result;
}

auto NSW::SearchLayer(const std::vector<double> &base_vector, size_t limit, const std::vector<size_t> &entry_points)
    -> std::vector<size_t> {
  using Node = std::pair<double, size_t>;
  auto min_cmp = [](const Node& n1, const Node& n2) {return n1.first > n2.first;};
  std::priority_queue<Node, std::vector<Node>, decltype(min_cmp)> candidates(min_cmp);
  auto max_cmp = [](const Node& n1, const Node& n2) {return n1.first < n2.first;};
  std::priority_queue<Node, std::vector<Node>, decltype(max_cmp)> results(max_cmp);
  std::unordered_set visited(entry_points.begin(), entry_points.end());
  for (const auto& p: entry_points) {
    double distance = ComputeDistance(base_vector, vertices_[p], dist_fn_);
    candidates.emplace(distance, p);
    results.emplace(distance, p);
    visited.emplace(p);
  }
  while (true) {
    if (candidates.empty()) {
      break;
    }
    const auto [distance, processing] = candidates.top();
    if (distance > results.top().first) {
      break;
    }
    candidates.pop();
    // Add all neighbors.
    for (const auto& neighbor: edges_[processing]) {
      if (visited.find(neighbor) == visited.end()) {
        double neighbor_distance = ComputeDistance(base_vector, vertices_[neighbor], dist_fn_);
        candidates.emplace(neighbor_distance, neighbor);
        results.emplace(neighbor_distance, neighbor);
        visited.emplace(neighbor);
      }
    }
    // Retain `limit` number of results
    while (results.size() > limit) {
      results.pop();
    }
  }
  std::vector<size_t> result_vertices;
  while (!results.empty()) {
    const auto& [_, v] = results.top();
    result_vertices.push_back(v);
    results.pop();
  }
  std::reverse(result_vertices.begin(), result_vertices.end());
  return result_vertices;
}

auto NSW::AddVertex(size_t vertex_id) { in_vertices_.push_back(vertex_id); }

auto NSW::Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m) {
  // IMPLEMENT ME
  InsertUnderEntries(vec, vertex_id, in_vertices_, m);
}
void NSW::InsertUnderEntries(const std::vector<double> &vec, size_t vertex_id, const std::vector<size_t> &entry_points, size_t m) {
  const std::vector<size_t>& neighbors = SelectNeighbors(vec, entry_points, vertices_, m, dist_fn_);
  AddVertex(vertex_id);
  for (const auto& v: neighbors) {
    Connect(vertex_id, v);
  }

  // Prune connections down below m_max
  for (const auto& v: neighbors) {
    if (edges_[v].size() <= m_max_) {
      continue;
    }
    // Remove a connection with max distance.
    double max_distance = 0.0;
    size_t to_remove = -1;
    for (const auto& nv: edges_[v]) {
      double distance = ComputeDistance(vertices_[v], vertices_[nv], dist_fn_);
      if (distance > max_distance) {
        max_distance = distance;
        to_remove = nv;
      }
    }
    // Remove the edge pointing to the neighbor
    auto pos = std::find(edges_[v].begin(), edges_[v].end(), to_remove);
    edges_[v].erase(pos);
    // Remove the edge pointed from the neighbor.
    auto pos_n = std::find(edges_[to_remove].begin(), edges_[to_remove].end(), v);
    edges_[to_remove].erase(pos_n);
  }
}

void NSW::Connect(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

auto HNSWIndex::AddVertex(const std::vector<double> &vec, RID rid) -> size_t {
  auto id = vertices_->size();
  vertices_->emplace_back(vec);
  rids_.emplace_back(rid);
  return id;
}

void HNSWIndex::BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) {
  std::shuffle(initial_data.begin(), initial_data.end(), generator_);
  for (const auto &[vec, rid] : initial_data) {
    InsertVectorEntry(vec, rid);
  }
}

auto HNSWIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
  auto vertex_ids = layers_[0].SearchLayer(base_vector, limit, {layers_[0].DefaultEntryPoint()});
  std::vector<RID> result;
  result.reserve(vertex_ids.size());
  for (const auto &id : vertex_ids) {
    result.push_back(rids_[id]);
  }
  return result;
}

void HNSWIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  auto id = AddVertex(key, rid);
  // Decide which layer and below to insert
  std::uniform_real_distribution<double> dist(0.0, 1.0);
  size_t begin_l = std::floor(-std::log(dist(generator_)) * m_l_);
  std::vector<size_t> ep{layers_[layers_.size() - 1].DefaultEntryPoint()};
  for (size_t l = m_l_ - 1; l > begin_l; --l) {
    ep = layers_[l].SearchLayer(key, 1, ep);
  }
  for (size_t l = begin_l; l >= 0; --l) {
    layers_[l].Insert(key, id, ef_construction_, m_);
  }
}

}  // namespace bustub
