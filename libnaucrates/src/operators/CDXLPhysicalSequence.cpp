//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalSequence.cpp
//
//	@doc:
//		Implementation of DXL physical sequence operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalSequence.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::CDXLPhysicalSequence
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSequence::CDXLPhysicalSequence(CMemoryPool *mp) : CDXLPhysical(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::~CDXLPhysicalSequence
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSequence::~CDXLPhysicalSequence() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalSequence::GetDXLOperator() const {
  return EdxlopPhysicalSequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalSequence::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalSequence);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalSequence::AssertValid(const CDXLNode *dxlnode, BOOL validate_children) const {
  const ULONG arity = dxlnode->Arity();
  GPOS_ASSERT(1 < arity);

  for (ULONG ul = 1; ul < arity; ul++) {
    CDXLNode *child_dxlnode = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypePhysical == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
