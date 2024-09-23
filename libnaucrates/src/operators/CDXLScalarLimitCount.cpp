//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarLimitCount.cpp
//
//	@doc:
//		Implementation of DXL Scalar Limit Count
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarLimitCount.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::CDXLScalarLimitCount
//
//	@doc:
//		Constructs a scalar Limit Count node
//
//---------------------------------------------------------------------------
CDXLScalarLimitCount::CDXLScalarLimitCount(CMemoryPool *mp) : CDXLScalar(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarLimitCount::GetDXLOperator() const {
  return EdxlopScalarLimitCount;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarLimitCount::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarLimitCount);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarLimitCount::AssertValid(const CDXLNode *node, bool validate_children) const {
  const uint32_t arity = node->Arity();
  GPOS_ASSERT(1 >= arity);

  for (uint32_t idx = 0; idx < arity; ++idx) {
    CDXLNode *dxlnode_arg = (*node)[idx];
    GPOS_ASSERT(EdxloptypeScalar == dxlnode_arg->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      dxlnode_arg->GetOperator()->AssertValid(dxlnode_arg, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
