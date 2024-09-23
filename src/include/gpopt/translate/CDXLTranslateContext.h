#pragma once

#include <unordered_map>

extern "C" {
#include <postgres.h>

#include <nodes/plannodes.h>
}

#include "gpopt/translate/CMappingElementColIdParamId.h"
#include "gpos/base.h"

struct TargetEntry;
struct Query;

namespace gpdxl {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDXLTranslateContext
//
//	@doc:
//		Class providing access to translation context, such as mappings between
//		ColIds and target entries
//
//---------------------------------------------------------------------------
class CDXLTranslateContext {
 private:
  // mappings ColId->TargetEntry used for intermediate DXL nodes
  std::unordered_map<uint32_t, TargetEntry *> m_colid_to_target_entry_map;

  // mappings ColId->ParamId used for outer refs in subplans
  std::unordered_map<uint32_t, CMappingElementColIdParamId *> m_colid_to_paramid_map;

  // is the node for which this context is built a child of an aggregate node
  // This is used to assign 0 instead of OUTER for the varno value of columns
  // in an Agg node, as expected in GPDB
  // TODO: antovl - Jan 26, 2011; remove this when Agg node in GPDB is fixed
  // to use OUTER instead of 0 for Var::varno in Agg target lists (MPP-12034)
  bool m_is_child_agg_node;

  const Query *m_query{nullptr};

 public:
  CDXLTranslateContext(const CDXLTranslateContext &) = delete;

  // ctor/dtor
  CDXLTranslateContext(bool is_child_agg_node, const Query *query)
      : m_is_child_agg_node(is_child_agg_node), m_query(query) {}

  CDXLTranslateContext(bool is_child_agg_node, std::unordered_map<uint32_t, CMappingElementColIdParamId *> &original)
      : m_is_child_agg_node(is_child_agg_node) {
    for (auto [k, v] : original)
      m_colid_to_paramid_map[k] = v->Copy();
  }

  ~CDXLTranslateContext() {
    for (auto [_, v] : m_colid_to_paramid_map)
      delete v;
  }

  // is parent an aggregate node
  bool IsParentAggNode() const { return m_is_child_agg_node; }

  // return the params hashmap
  auto &GetColIdToParamIdMap() { return m_colid_to_paramid_map; }

  const Query *GetQuery() { return m_query; }

  // return the target entry corresponding to the given ColId
  const TargetEntry *GetTargetEntry(uint32_t colid) const {
    if (m_colid_to_target_entry_map.contains(colid))
      return m_colid_to_target_entry_map.at(colid);
    return nullptr;
  }

  // return the param id corresponding to the given ColId
  const CMappingElementColIdParamId *GetParamIdMappingElement(uint32_t colid) const {
    if (m_colid_to_paramid_map.contains(colid))
      return m_colid_to_paramid_map.at(colid);
    return nullptr;
  }

  // store the mapping of the given column id and target entry
  void InsertMapping(uint32_t colid, TargetEntry *target_entry) { m_colid_to_target_entry_map[colid] = target_entry; }

  // store the mapping of the given column id and param id
  void FInsertParamMapping(uint32_t colid, CMappingElementColIdParamId *pmecolidparamid) {
    m_colid_to_paramid_map[colid] = pmecolidparamid;
  }
};

// array of dxl translation context
using CDXLTranslationContextArray = CDynamicPtrArray<const CDXLTranslateContext, CleanupNULL>;
}  // namespace gpdxl
