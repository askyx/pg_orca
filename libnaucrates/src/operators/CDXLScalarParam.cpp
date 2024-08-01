//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 Broadcom
//
//	@filename:
//		CDXLScalarParam.cpp
//
//	@doc:
//		Implementation of DXL scalar parameters
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarParam.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarParam::CDXLScalarParam
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarParam::CDXLScalarParam(CMemoryPool *mp, ULONG id, IMDId *mdid_type, INT type_modifer)
    : CDXLScalar(mp), m_id(id), m_mdid_type(mdid_type), m_type_modifer(type_modifer) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarParam::~CDXLScalarParam
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarParam::~CDXLScalarParam() {
  m_mdid_type->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarParam::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarParam::GetDXLOperator() const {
  return EdxlopScalarParam;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarParam::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarParam::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarParam);
}

ULONG
CDXLScalarParam::GetId() const {
  return m_id;
}

IMDId *CDXLScalarParam::GetMDIdType() const {
  return m_mdid_type;
}

INT CDXLScalarParam::GetTypeModifier() const {
  return m_type_modifer;
}
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarParam::HasBoolResult
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
BOOL CDXLScalarParam::HasBoolResult(CMDAccessor *md_accessor) const {
  return (IMDType::EtiBool == md_accessor->RetrieveType(m_mdid_type)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarParam::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarParam::AssertValid(const CDXLNode *node,
                                  BOOL  // validate_children
) const {
  GPOS_ASSERT(0 == node->Arity());
  GPOS_ASSERT(nullptr != m_mdid_type);
}
#endif  // GPOS_DEBUG

// EOF
