#include "storage/index/ivfflat_index.h"
#include <algorithm>
#include <optional>
#include <random>
#include "common/exception.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
using Vector = std::vector<double>;

IVFFlatIndex::IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                           VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn) {
  std::optional<size_t> lists;
  std::optional<size_t> probe_lists;
  for (const auto &[k, v] : options) {
    if (k == "lists") {
      lists = v;
    } else if (k == "probe_lists") {
      probe_lists = v;
    }
  }
  if (!lists.has_value() || !probe_lists.has_value()) {
    throw Exception("missing options: lists / probe_lists for ivfflat index");
  }
  lists_ = *lists;
  probe_lists_ = *probe_lists;
}

void VectorAdd(Vector &a, const Vector &b) {
  for (size_t i = 0; i < a.size(); i++) {
    a[i] += b[i];
  }
}

void VectorScalarDiv(Vector &a, double x) {
  for (auto &y : a) {
    y /= x;
  }
}

// Find the nearest centroid to the base vector in all centroids
auto FindCentroid(const Vector &vec, const std::vector<Vector> &centroids, VectorExpressionType dist_fn) -> size_t {
  size_t cent_idx = -1;
  double min_distance = std::numeric_limits<double>::infinity();
  for (size_t i = 0; i < centroids.size(); ++i) {
    double distance = ComputeDistance(vec, centroids[i], dist_fn);
    if (distance < min_distance) {
        cent_idx = i;
        min_distance = distance;
    }
  }
  return cent_idx;
}

// Compute new centroids based on the original centroids.
auto FindCentroids(const std::vector<std::pair<Vector, RID>> &data, const std::vector<Vector> &centroids,
                   VectorExpressionType dist_fn) -> std::vector<Vector> {
  size_t vector_dim = centroids[0].size();
  std::vector<Vector> new_centroids(centroids.size(), Vector(vector_dim, 0));
  std::vector<double> group_size(centroids.size(), 0);
  for (const auto& [v, _]: data) {
    size_t assigned_cent = FindCentroid(v, centroids, dist_fn);
    VectorAdd(new_centroids[assigned_cent], v);
    group_size[assigned_cent] += 1.0;
  }
  for (size_t i = 0; i < centroids.size(); ++i) {
    VectorScalarDiv(new_centroids[i], group_size[i]);
  }
  return new_centroids;
}

void IVFFlatIndex::BuildIndex(std::vector<std::pair<Vector, RID>> initial_data) {
  if (initial_data.empty()) {
    return;
  }

  // IMPLEMENT ME
  // Iterate for 500 times.
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> sampler(0, initial_data.size() - 1);
  for (size_t t = 0; t < lists_; ++t) {
    size_t choice = sampler(gen);
    centroids_.emplace_back(initial_data[choice].first);
  }
  for (size_t t = 0; t < 500; ++t) {
    centroids_ = FindCentroids(initial_data, centroids_, distance_fn_);
  }
  // Assign data to the final centroids result.
  centroids_buckets_ = std::vector<std::vector<std::pair<Vector, RID>>>(lists_);
  for (auto& [v, r]: initial_data) {
    InsertVectorEntry(v, r);
  }
}

void IVFFlatIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  std::cerr << "Insertion\n";
  size_t assignment = FindCentroid(key, centroids_, distance_fn_);
  centroids_buckets_[assignment].emplace_back(key, rid);
}

auto IVFFlatIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
  using CentroidDistance = std::pair<double, size_t>;
  auto cmp =  [](CentroidDistance& t1, CentroidDistance& t2) {return t1.first > t2.first;};
  std::priority_queue<CentroidDistance, std::vector<CentroidDistance>, decltype(cmp)> centroid_pq(cmp);
  std::vector<std::pair<Vector, RID>>  points_to_lookup;
  for (size_t i = 0; i < lists_; ++i) {
    double distance = ComputeDistance(base_vector, centroids_[i], distance_fn_);
    for (auto& v: centroids_[i]) {
      std::cerr << v << " ";
    }
    std::cerr << "\n";
    centroid_pq.emplace(distance, i);
  }
  for (size_t i = 0; i < probe_lists_; ++i) {
    const auto& [_, centroid_id] = centroid_pq.top();
    centroid_pq.pop();
    for (auto& point: centroids_buckets_[centroid_id]) {
      points_to_lookup.emplace_back(point);
    }
  }
  // Find the approximate knn
  using PointDistance = std::pair<double, RID>;
  auto cmp2 =  [](PointDistance & t1, PointDistance & t2) {return t1.first > t2.first;};
  std::priority_queue<PointDistance, std::vector<PointDistance>, decltype(cmp2)> point_pq(cmp2);
  for (auto& [point, r]: points_to_lookup) {
    for (auto& v: point) {
      std::cerr << v << " ";
    }
    std::cerr << "\n";
    point_pq.emplace(ComputeDistance(base_vector, point, distance_fn_), r);
  }
  std::vector<RID> result;
  for (size_t i = 0; i < limit; ++i) {
    if (point_pq.empty()) {
      break;
    }
    result.push_back(point_pq.top().second);
    point_pq.pop();
  }
  return result;
}

}  // namespace bustub
