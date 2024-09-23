#pragma once

#include <vector>

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpos/base.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLScalarIdent.h"

extern "C" {
#include <postgres.h>

#include <nodes/plannodes.h>
}

#include <unordered_map>

namespace gpdxl {

class CContextDXLToPlStmt {
 public:
  // counter for generating plan ids
  uint32_t m_plan_id_counter{0};

  // counter for generating unique param ids
  uint32_t m_param_id_counter{0};

  List *m_param_types_list{nullptr};

  // list of all rtable entries
  List *m_rtable_entries_list{nullptr};

  // list of all subplan entries
  List *m_subplan_entries_list{nullptr};

  // index of the target relation in the rtable or 0 if not a DML statement
  uint32_t m_result_relation_index{0};

  // hash map of the queryid (of DML query) and the target relation index
  std::unordered_map<uint32_t, uint32_t> m_used_rte_indexes;

  // retrieve the next plan id
  uint32_t GetNextPlanId();

  // retrieve the current parameter type list
  List *GetParamTypes();

  // retrieve the next parameter id
  uint32_t GetNextParamId(OID typeoid);

  // return list of range table entries
  List *GetRTableEntriesList() const { return m_rtable_entries_list; }

  List *GetSubplanEntriesList() const { return m_subplan_entries_list; }

  // index of result relation in the rtable
  uint32_t GetResultRelationIndex() const { return m_result_relation_index; }

  // add a range table entry
  void AddRTE(RangeTblEntry *rte, bool is_result_relation = false);

  // used by internal GPDB functions to build the RelOptInfo when creating foreign scans
  Query *m_orig_query;

  // get rte from m_rtable_entries_list by given index
  RangeTblEntry *GetRTEByIndex(Index index);

  Index GetRTEIndexByAssignedQueryId(uint32_t assigned_query_id_for_target_rel, bool *is_rte_exists);
};

}  // namespace gpdxl
