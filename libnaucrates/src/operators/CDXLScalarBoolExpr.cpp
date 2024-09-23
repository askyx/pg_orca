//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarBoolExpr.cpp
//
//	@doc:
//		Implementation of DXL BoolExpr
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::CDXLScalarBoolExpr
//
//	@doc:
//		Constructs a BoolExpr node
//
//---------------------------------------------------------------------------
CDXLScalarBoolExpr::CDXLScalarBoolExpr(CMemoryPool *mp, const EdxlBoolExprType bool_type)
    : CDXLScalar(mp), m_bool_type(bool_type) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarBoolExpr::GetDXLOperator() const {
  return EdxlopScalarBoolExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::GetDxlBoolTypeStr
//
//	@doc:
//		Boolean expression type
//
//---------------------------------------------------------------------------
EdxlBoolExprType CDXLScalarBoolExpr::GetDxlBoolTypeStr() const {
  return m_bool_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarBoolExpr::GetOpNameStr() const {
  switch (m_bool_type) {
    case Edxland:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarBoolAnd);
    case Edxlor:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarBoolOr);
    case Edxlnot:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarBoolNot);
    default:
      return nullptr;
  }
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarBoolExpr::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  EdxlBoolExprType dxl_bool_type = ((CDXLScalarBoolExpr *)dxlnode->GetOperator())->GetDxlBoolTypeStr();

  GPOS_ASSERT((dxl_bool_type == Edxlnot) || (dxl_bool_type == Edxlor) || (dxl_bool_type == Edxland));

  const uint32_t arity = dxlnode->Arity();
  if (dxl_bool_type == Edxlnot) {
    GPOS_ASSERT(1 == arity);
  } else {
    GPOS_ASSERT(2 <= arity);
  }

  for (uint32_t ul = 0; ul < arity; ++ul) {
    CDXLNode *dxlnode_arg = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypeScalar == dxlnode_arg->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      dxlnode_arg->GetOperator()->AssertValid(dxlnode_arg, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
