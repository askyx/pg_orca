//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarLimitOffset.cpp
//
//	@doc:
//		Implementation of DXL Scalar Limit Offset
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarLimitOffset.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitOffset::CDXLScalarLimitOffset
//
//	@doc:
//		Constructs a scalar Limit Offset node
//
//---------------------------------------------------------------------------
CDXLScalarLimitOffset::CDXLScalarLimitOffset(CMemoryPool *mp) : CDXLScalar(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitOffset::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarLimitOffset::GetDXLOperator() const {
  return EdxlopScalarLimitOffset;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitOffset::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarLimitOffset::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarLimitOffset);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitOffset::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarLimitOffset::AssertValid(const CDXLNode *node, bool validate_children) const {
  const uint32_t arity = node->Arity();
  GPOS_ASSERT(1 >= arity);

  for (uint32_t ul = 0; ul < arity; ++ul) {
    CDXLNode *dxlnode_arg = (*node)[ul];
    GPOS_ASSERT(EdxloptypeScalar == dxlnode_arg->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      dxlnode_arg->GetOperator()->AssertValid(dxlnode_arg, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
