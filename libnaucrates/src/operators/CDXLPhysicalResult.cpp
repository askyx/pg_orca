//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalResult.cpp
//
//	@doc:
//		Implementation of DXL physical result operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalResult.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::CDXLPhysicalResult
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalResult::CDXLPhysicalResult(CMemoryPool *mp) : CDXLPhysical(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalResult::GetDXLOperator() const {
  return EdxlopPhysicalResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalResult::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalResult);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalResult::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  GPOS_ASSERT(EdxlresultIndexSentinel >= dxlnode->Arity());

  // check that one time filter is valid
  CDXLNode *one_time_filter = (*dxlnode)[EdxlresultIndexOneTimeFilter];
  GPOS_ASSERT(EdxlopScalarOneTimeFilter == one_time_filter->GetOperator()->GetDXLOperator());

  if (validate_children) {
    one_time_filter->GetOperator()->AssertValid(one_time_filter, validate_children);
  }

  if (EdxlresultIndexSentinel == dxlnode->Arity()) {
    CDXLNode *child_dxlnode = (*dxlnode)[EdxlresultIndexChild];
    GPOS_ASSERT(EdxloptypePhysical == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
