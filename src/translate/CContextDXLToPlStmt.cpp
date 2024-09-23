extern "C" {
#include <postgres.h>

#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
#include <utils/rel.h>
}

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpos/base.h"
#include "naucrates/exception.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetNextPlanId
//
//	@doc:
//		Get the next plan id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::GetNextPlanId() {
  return ++m_plan_id_counter;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetNextParamId
//
//	@doc:
//		Get the next param id, for a parameter of type 'typeoid'
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::GetNextParamId(OID typeoid) {
  m_param_types_list = gpdb::LAppendOid(m_param_types_list, typeoid);

  return ++m_param_id_counter;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::GetParamTypes
//
//	@doc:
//		Get the current param types list
//
//---------------------------------------------------------------------------
List *CContextDXLToPlStmt::GetParamTypes() {
  return m_param_types_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddRTE
//
//	@doc:
//		Add a RangeTableEntries
//
//---------------------------------------------------------------------------
void CContextDXLToPlStmt::AddRTE(RangeTblEntry *rte, bool is_result_relation) {
  // add rte to rtable entries list
  m_rtable_entries_list = gpdb::LAppend(m_rtable_entries_list, rte);

  rte->inFromCl = true;

  if (is_result_relation) {
    GPOS_ASSERT(0 == m_result_relation_index && "Only one result relation supported");
    rte->inFromCl = false;
    m_result_relation_index = gpdb::ListLength(m_rtable_entries_list);
  }
}

RangeTblEntry *CContextDXLToPlStmt::GetRTEByIndex(Index index) {
  return (RangeTblEntry *)gpdb::ListNth(m_rtable_entries_list, int(index - 1));
}

//---------------------------------------------------------------------------
//	@function: of associated
//		CContextDXLToPlStmt::GetRTEIndexByAssignedQueryId
//
//	@doc:
//
//		For given assigned query id, this function returns the index of rte in
//		m_rtable_entries_list for further processing and sets is_rte_exists
//		flag that rte was processed.
//
//		assigned query id is a "tag" of a table descriptor. It marks the query
//		structure that contains the result relation. If two table descriptors
//		have the same assigned query id, these two table descriptors point to
//		the same result relation in `ModifyTable` operation. If the assigned
//		query id is positive, it indicates the query is an INSERT/UPDATE/DELETE
//		operation (`ModifyTable` operation). If the assigned query id zero
// 		(UNASSIGNED_QUERYID), usually it indicates the operation is not
// 		INSERT/UPDATE/DELETE, and doesn't have a result relation. Or sometimes,
// 		the operation is INSERT/UPDATE/DELETE, but the table descriptor tagged
//		with the assigned query id doesn't point to a result relation.
//
//		m_used_rte_indexes is a hash map. It maps the assigned query id (key)
//		to the rte index (value) as in m_rtable_entries_list. The hash map only
//		stores positive id's, because id 0 is reserved for operations that
//		don't have a result relation.
//
//		To look up the rte index, we may encounter three scenarios:
//
//		1. If assigned query id == 0, since the hash map only stores positive
//		id's, the rte index isn't stored in the hash map. In this case, we
//		return rtable entries list length+1. This means we will add a new rte
//		to the rte list.
//
//		2. If assigned query id > 0, it indicates the operation is
//		INSERT/UPDATE/DELETE. We look for its index in the hash map. If the
//		index is found, we return the index. This means we can reuse the rte
//		that's already in the list.
//
// 		3. If assigned query id > 0, but the index isn't found in the hash map,
// 		it means the id hasn't been processed. We return rtable entries list
// 		length+1. This means we will add a new rte to the rte list. At the same
// 		time, we will add the assigned query id --> list_length+1 key-value pair
// 		to the hash map for future look ups.
//
//		Here's an example
//		```sql
//		create table b (i int, j int);
//		create table c (i int);
//		insert into b(i,j) values (1,2), (2,3), (3,4);
//		insert into c(i) values (1), (2);
//		delete from b where i in (select i from c);
//		```
//
//		`b` is a result relation. Table descriptors for `b` will have the same
//		positive assigned query id. `c` is not a result relation. Table
//		descriptors for `c` will have zero assigned query id.
//---------------------------------------------------------------------------

Index CContextDXLToPlStmt::GetRTEIndexByAssignedQueryId(ULONG assigned_query_id_for_target_rel, bool *is_rte_exists) {
  *is_rte_exists = false;

  if (assigned_query_id_for_target_rel == UNASSIGNED_QUERYID) {
    return gpdb::ListLength(m_rtable_entries_list) + 1;
  }

  if (m_used_rte_indexes.contains(assigned_query_id_for_target_rel)) {
    *is_rte_exists = true;
    return m_used_rte_indexes[assigned_query_id_for_target_rel];
  }

  //	`assigned_query_id_for_target_rel` of table descriptor which points to
  //	result relation wasn't previously processed - create a new index.
  return gpdb::ListLength(m_rtable_entries_list) + 1;
}

// EOF
