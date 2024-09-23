//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarSwitch.cpp
//
//	@doc:
//		Implementation of DXL Switch
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarSwitch.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::CDXLScalarSwitch
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarSwitch::CDXLScalarSwitch(CMemoryPool *mp, IMDId *mdid_type) : CDXLScalar(mp), m_mdid_type(mdid_type) {
  GPOS_ASSERT(m_mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::~CDXLScalarSwitch
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarSwitch::~CDXLScalarSwitch() {
  m_mdid_type->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarSwitch::GetDXLOperator() const {
  return EdxlopScalarSwitch;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarSwitch::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSwitch);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::MdidType
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarSwitch::MdidType() const {
  return m_mdid_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::HasBoolResult
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
bool CDXLScalarSwitch::HasBoolResult(CMDAccessor *md_accessor) const {
  return (IMDType::EtiBool == md_accessor->RetrieveType(m_mdid_type)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarSwitch::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  const uint32_t arity = dxlnode->Arity();
  GPOS_ASSERT(1 < arity);

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
