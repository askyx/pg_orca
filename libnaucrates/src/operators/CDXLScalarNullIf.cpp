//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarNullIf.cpp
//
//	@doc:
//		Implementation of DXL NullIf operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarNullIf.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::CDXLScalarNullIf
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarNullIf::CDXLScalarNullIf(CMemoryPool *mp, IMDId *mdid_op, IMDId *mdid_type)
    : CDXLScalar(mp), m_mdid_op(mdid_op), m_mdid_type(mdid_type) {
  GPOS_ASSERT(mdid_op->IsValid());
  GPOS_ASSERT(mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::~CDXLScalarNullIf
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarNullIf::~CDXLScalarNullIf() {
  m_mdid_op->Release();
  m_mdid_type->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarNullIf::GetDXLOperator() const {
  return EdxlopScalarNullIf;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::MdIdOp
//
//	@doc:
//		Operator id
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarNullIf::MdIdOp() const {
  return m_mdid_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::MdidType
//
//	@doc:
//		Return type
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarNullIf::MdidType() const {
  return m_mdid_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarNullIf::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarNullIf);
  ;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::HasBoolResult
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
BOOL CDXLScalarNullIf::HasBoolResult(CMDAccessor *md_accessor) const {
  return (IMDType::EtiBool == md_accessor->RetrieveType(m_mdid_type)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarNullIf::AssertValid(const CDXLNode *dxlnode, BOOL validate_children) const {
  const ULONG arity = dxlnode->Arity();
  GPOS_ASSERT(2 == arity);

  for (ULONG idx = 0; idx < arity; ++idx) {
    CDXLNode *child_dxlnode = (*dxlnode)[idx];
    GPOS_ASSERT(EdxloptypeScalar == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
