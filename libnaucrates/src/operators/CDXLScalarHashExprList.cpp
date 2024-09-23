//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarHashExprList.cpp
//
//	@doc:
//		Implementation of DXL hash expression lists for redistribute operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarHashExprList.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::CDXLScalarHashExprList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarHashExprList::CDXLScalarHashExprList(CMemoryPool *mp) : CDXLScalar(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarHashExprList::GetDXLOperator() const {
  return EdxlopScalarHashExprList;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarHashExprList::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarHashExprList);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarHashExprList::AssertValid(const CDXLNode *node, bool validate_children) const {
  const uint32_t arity = node->Arity();
  GPOS_ASSERT(1 <= arity);

  for (uint32_t ul = 0; ul < arity; ul++) {
    CDXLNode *child_dxlnode = (*node)[ul];
    GPOS_ASSERT(EdxlopScalarHashExpr == child_dxlnode->GetOperator()->GetDXLOperator());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}

#endif  // GPOS_DEBUG

// EOF
