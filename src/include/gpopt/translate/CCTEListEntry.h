//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CCTEListEntry.h
//
//	@doc:
//		Class representing the list of common table expression defined at a
//		query level
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CCTEListEntry_H
#define GPDXL_CCTEListEntry_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "naucrates/dxl/operators/CDXLNode.h"

// fwd declaration
struct Query;
struct List;
struct RangeTblEntry;
struct CommonTableExpr;

using namespace gpos;

namespace gpdxl {
// hash on character arrays
inline uint32_t HashStr(const char *str) {
  return gpos::HashByteArray((uint8_t *)str, clib::Strlen(str));
}

// equality on character arrays
inline bool StrEqual(const char *str_a, const char *str_b) {
  return (0 == clib::Strcmp(str_a, str_b));
}

//---------------------------------------------------------------------------
//	@class:
//		CCTEListEntry
//
//	@doc:
//		Class representing the list of common table expression defined at a
//		query level
//
//---------------------------------------------------------------------------
class CCTEListEntry : public CRefCount {
 private:
  // pair of DXL CTE producer and target list of the original CTE query
  struct SCTEProducerInfo {
    const CDXLNode *m_cte_producer;
    List *m_target_list;

    // ctor
    SCTEProducerInfo(const CDXLNode *cte_producer, List *target_list)
        : m_cte_producer(cte_producer), m_target_list(target_list) {}
  };

  // hash maps mapping char *->SCTEProducerInfo
  using HMSzCTEInfo = CHashMap<char, SCTEProducerInfo, HashStr, StrEqual, CleanupNULL, CleanupDelete>;

  // query level where the CTEs are defined
  uint32_t m_query_level;

  // CTE producers at that level indexed by their name
  HMSzCTEInfo *m_cte_info;

 public:
  // ctor: single CTE
  CCTEListEntry(CMemoryPool *mp, uint32_t query_level, CommonTableExpr *cte, CDXLNode *cte_producer);

  // ctor: multiple CTEs
  CCTEListEntry(CMemoryPool *mp, uint32_t query_level, List *cte_list, CDXLNodeArray *dxlnodes);

  // dtor
  ~CCTEListEntry() override { m_cte_info->Release(); };

  // the query level
  uint32_t GetQueryLevel() const { return m_query_level; }

  // lookup CTE producer by its name
  const CDXLNode *GetCTEProducer(const char *cte_str) const;

  // lookup CTE producer target list by its name
  List *GetCTEProducerTargetList(const char *cte_str) const;

  // add a new CTE producer for this level
  void AddCTEProducer(CMemoryPool *mp, CommonTableExpr *cte, const CDXLNode *cte_producer);
};

// hash maps mapping uint32_t -> CCTEListEntry
using HMUlCTEListEntry = CHashMap<uint32_t, CCTEListEntry, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                  CleanupDelete<uint32_t>, CleanupRelease>;

// iterator
using HMIterUlCTEListEntry = CHashMapIter<uint32_t, CCTEListEntry, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                          CleanupDelete<uint32_t>, CleanupRelease>;

}  // namespace gpdxl
#endif  // !GPDXL_CCTEListEntry_H

// EOF
