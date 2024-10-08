//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarCoerceBase.cpp
//
//	@doc:
//		Implementation of DXL scalar coerce base class
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCoerceBase.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceBase::CDXLScalarCoerceBase
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCoerceBase::CDXLScalarCoerceBase(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier,
                                           EdxlCoercionForm dxl_coerce_format, int32_t location)
    : CDXLScalar(mp),
      m_result_type_mdid(mdid_type),
      m_type_modifier(type_modifier),
      m_dxl_coerce_format(dxl_coerce_format),
      m_location(location) {
  GPOS_ASSERT(nullptr != mdid_type);
  GPOS_ASSERT(mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceBase::~CDXLScalarCoerceBase
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarCoerceBase::~CDXLScalarCoerceBase() {
  m_result_type_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceBase::HasBoolResult
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
bool CDXLScalarCoerceBase::HasBoolResult(CMDAccessor *md_accessor) const {
  return (IMDType::EtiBool == md_accessor->RetrieveType(m_result_type_mdid)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceBase::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarCoerceBase::AssertValid(const CDXLNode *node, bool validate_children) const {
  GPOS_ASSERT(1 == node->Arity());

  CDXLNode *child_dxlnode = (*node)[0];
  GPOS_ASSERT(EdxloptypeScalar == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}
#endif  // GPOS_DEBUG

// EOF
