//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarArrayComp.cpp
//
//	@doc:
//		Implementation of DXL scalar array comparison
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayComp.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::CDXLScalarArrayComp
//
//	@doc:
//		Constructs a ScalarArrayComp node
//
//---------------------------------------------------------------------------
CDXLScalarArrayComp::CDXLScalarArrayComp(CMemoryPool *mp, IMDId *mdid_op, const CWStringConst *str_opname,
                                         EdxlArrayCompType comparison_type)
    : CDXLScalarComp(mp, mdid_op, str_opname), m_comparison_type(comparison_type) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarArrayComp::GetDXLOperator() const {
  return EdxlopScalarArrayComp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::GetDXLArrayCmpType
//
//	@doc:
//	 	Returns the array comparison operation type (ALL/ANY)
//
//---------------------------------------------------------------------------
EdxlArrayCompType CDXLScalarArrayComp::GetDXLArrayCmpType() const {
  return m_comparison_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::PstrArrayCompType
//
//	@doc:
//		AggRef AggStage
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarArrayComp::GetDXLStrArrayCmpType() const {
  switch (m_comparison_type) {
    case Edxlarraycomptypeany:
      return CDXLTokens::GetDXLTokenStr(EdxltokenOpTypeAny);
    case Edxlarraycomptypeall:
      return CDXLTokens::GetDXLTokenStr(EdxltokenOpTypeAll);
    default:
      GPOS_ASSERT(!"Unrecognized array operation type");
      return nullptr;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarArrayComp::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarArrayComp);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarArrayComp::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  const uint32_t arity = dxlnode->Arity();
  GPOS_ASSERT(2 == arity);

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
