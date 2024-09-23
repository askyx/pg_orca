//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalAppend.cpp
//
//	@doc:
//		Implementation of DXL physical Append operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalAppend.h"

#include "gpos/common/CBitSetIter.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::CDXLPhysicalAppend
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalAppend::CDXLPhysicalAppend(CMemoryPool *mp, bool fIsTarget, bool fIsZapped)
    : CDXLPhysical(mp), m_used_in_upd_del(fIsTarget), m_is_zapped(fIsZapped) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalAppend::GetDXLOperator() const {
  return EdxlopPhysicalAppend;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalAppend::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalAppend);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::IsUsedInUpdDel
//
//	@doc:
//		Is the append node updating a target relation
//
//---------------------------------------------------------------------------
bool CDXLPhysicalAppend::IsUsedInUpdDel() const {
  return m_used_in_upd_del;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::IsZapped
//
//	@doc:
//		Is the append node zapped
//
//---------------------------------------------------------------------------
bool CDXLPhysicalAppend::IsZapped() const {
  return m_is_zapped;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalAppend::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  // assert proj list and filter are valid
  CDXLPhysical::AssertValid(dxlnode, validate_children);

  const uint32_t ulChildren = dxlnode->Arity();
  for (uint32_t ul = EdxlappendIndexFirstChild; ul < ulChildren; ul++) {
    CDXLNode *child_dxlnode = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypePhysical == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
