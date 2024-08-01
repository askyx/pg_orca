//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarArrayRef.cpp
//
//	@doc:
//		Implementation of DXL arrayrefs
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayRef.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::CDXLScalarArrayRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArrayRef::CDXLScalarArrayRef(CMemoryPool *mp, IMDId *elem_type_mdid, INT type_modifier,
                                       IMDId *array_type_mdid, IMDId *return_type_mdid)
    : CDXLScalar(mp),
      m_elem_type_mdid(elem_type_mdid),
      m_type_modifier(type_modifier),
      m_array_type_mdid(array_type_mdid),
      m_return_type_mdid(return_type_mdid) {
  GPOS_ASSERT(m_elem_type_mdid->IsValid());
  GPOS_ASSERT(m_array_type_mdid->IsValid());
  GPOS_ASSERT(m_return_type_mdid->IsValid());
  GPOS_ASSERT(m_return_type_mdid->Equals(m_elem_type_mdid) || m_return_type_mdid->Equals(m_array_type_mdid));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::~CDXLScalarArrayRef
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarArrayRef::~CDXLScalarArrayRef() {
  m_elem_type_mdid->Release();
  m_array_type_mdid->Release();
  m_return_type_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarArrayRef::GetDXLOperator() const {
  return EdxlopScalarArrayRef;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarArrayRef::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarArrayRef);
}

INT CDXLScalarArrayRef::TypeModifier() const {
  return m_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::HasBoolResult
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
BOOL CDXLScalarArrayRef::HasBoolResult(CMDAccessor *md_accessor) const {
  return (IMDType::EtiBool == md_accessor->RetrieveType(m_return_type_mdid)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarArrayRef::AssertValid(const CDXLNode *dxlnode, BOOL validate_children) const {
  const ULONG arity = dxlnode->Arity();
  for (ULONG ul = 0; ul < arity; ++ul) {
    CDXLNode *child_dxlnode = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypeScalar == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
