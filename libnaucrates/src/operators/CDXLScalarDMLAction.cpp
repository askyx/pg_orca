//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarDMLAction.cpp
//
//	@doc:
//		Implementation of DXL DML action expression
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarDMLAction.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::CDXLScalarDMLAction
//
//	@doc:
//		Constructs an action expression
//
//---------------------------------------------------------------------------
CDXLScalarDMLAction::CDXLScalarDMLAction(CMemoryPool *mp) : CDXLScalar(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarDMLAction::GetDXLOperator() const {
  return EdxlopScalarDMLAction;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarDMLAction::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarDMLAction);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::HasBoolResult
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
bool CDXLScalarDMLAction::HasBoolResult(CMDAccessor *  // md_accessor
) const {
  return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarDMLAction::AssertValid(const CDXLNode *node,
                                      bool  // validate_children
) const {
  GPOS_ASSERT(0 == node->Arity());
}
#endif  // GPOS_DEBUG

// EOF
