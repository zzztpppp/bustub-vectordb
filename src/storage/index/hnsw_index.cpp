#include "storage/index/hnsw_index.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
HNSWIndex::HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                     VectorExpressionType distance_fn, std::vector<std::pair<std::string, int>> options)
    : VectorIndex(std::move(metadata), distance_fn) {}

void HNSWIndex::BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) {}
auto HNSWIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> { return {}; }
void HNSWIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {}

}  // namespace bustub
