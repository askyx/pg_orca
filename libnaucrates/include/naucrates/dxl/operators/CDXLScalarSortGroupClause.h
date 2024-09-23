//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSortGroupClause.h
//
//	@doc:
//		Class for representing sort group clause.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSortGroupClause_H
#define GPDXL_CDXLScalarSortGroupClause_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl {
using namespace gpmd;
using namespace gpos;

// fwd decl

class CDXLScalarSortGroupClause;

// arrays of column references
using CDXLScalarSortGroupClauseArray = CDynamicPtrArray<CDXLScalarSortGroupClause, CleanupRelease>;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarSortGroupClause
//
//	@doc:
//		Class for representing references to columns in DXL trees
//
//---------------------------------------------------------------------------
class CDXLScalarSortGroupClause : public CDXLScalar {
 private:
  int32_t m_tle_sort_group_ref;
  int32_t m_eqop;
  int32_t m_sortop;
  bool m_nulls_first;
  bool m_hashable;

 public:
  CDXLScalarSortGroupClause(const CDXLScalarSortGroupClause &) = delete;

  // ctor/dtor
  CDXLScalarSortGroupClause(CMemoryPool *mp, int32_t tle_sort_group_ref, int32_t eqop, int32_t sortop, bool nulls_first,
                            bool hashable)
      : CDXLScalar(mp),
        m_tle_sort_group_ref(tle_sort_group_ref),
        m_eqop(eqop),
        m_sortop(sortop),
        m_nulls_first(nulls_first),
        m_hashable(hashable) {}

  // accessors
  int32_t Index() const { return m_tle_sort_group_ref; }

  int32_t EqOp() const { return m_eqop; }

  int32_t SortOp() const { return m_sortop; }

  bool NullsFirst() const { return m_nulls_first; }

  bool IsHashable() const { return m_hashable; }

  static CDXLScalarSortGroupClause *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopScalarSortGroupClause == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLScalarSortGroupClause *>(dxl_op);
  }

  // does the operator return a boolean result
  bool HasBoolResult(CMDAccessor *) const override { return false; }

#ifdef GPOS_DEBUG
  void AssertValid(const CDXLNode *, bool) const override {}
#endif  // GPOS_DEBUG

  Edxlopid GetDXLOperator() const override { return EdxlopScalarSortGroupClause; }

  const CWStringConst *GetOpNameStr() const override {
    return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSortGroupClause);
  }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLScalarSortGroupClause_H

// EOF
