//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarSwitchCase.cpp
//
//	@doc:
//		Implementation of DXL Switch case
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarSwitchCase.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitchCase::CDXLScalarSwitchCase
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarSwitchCase::CDXLScalarSwitchCase(CMemoryPool *mp) : CDXLScalar(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitchCase::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarSwitchCase::GetDXLOperator() const {
  return EdxlopScalarSwitchCase;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitchCase::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarSwitchCase::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSwitchCase);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitchCase::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarSwitchCase::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  const uint32_t arity = dxlnode->Arity();
  GPOS_ASSERT(2 == arity);

  for (uint32_t idx = 0; idx < arity; ++idx) {
    CDXLNode *dxlnode_arg = (*dxlnode)[idx];
    GPOS_ASSERT(EdxloptypeScalar == dxlnode_arg->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      dxlnode_arg->GetOperator()->AssertValid(dxlnode_arg, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
