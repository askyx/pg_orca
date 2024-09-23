#pragma once

#include <unordered_map>

namespace gpdxl {

struct TranslateContextBaseTable {
  uint64_t rel_oid;
  uint32_t rte_index;
  std::unordered_map<uint32_t, int> colid_to_attno_map;
};

}  // namespace gpdxl
