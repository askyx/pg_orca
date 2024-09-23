//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryNotExists.cpp
//
//	@doc:
//		Implementation of NOT EXISTS subqueries
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarSubqueryNotExists.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::CDXLScalarSubqueryNotExists
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryNotExists::CDXLScalarSubqueryNotExists(CMemoryPool *mp) : CDXLScalar(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::~CDXLScalarSubqueryNotExists
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryNotExists::~CDXLScalarSubqueryNotExists() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarSubqueryNotExists::GetDXLOperator() const {
  return EdxlopScalarSubqueryNotExists;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarSubqueryNotExists::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubqueryNotExists);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarSubqueryNotExists::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  GPOS_ASSERT(1 == dxlnode->Arity());

  CDXLNode *child_dxlnode = (*dxlnode)[0];
  GPOS_ASSERT(EdxloptypeLogical == child_dxlnode->GetOperator()->GetDXLOperatorType());

  dxlnode->AssertValid(validate_children);
}
#endif  // GPOS_DEBUG

// EOF
