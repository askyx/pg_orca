//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarCaseTest.cpp
//
//	@doc:
//		Implementation of DXL case test
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCaseTest.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::CDXLScalarCaseTest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCaseTest::CDXLScalarCaseTest(CMemoryPool *mp, IMDId *mdid_type) : CDXLScalar(mp), m_mdid_type(mdid_type) {
  GPOS_ASSERT(m_mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::~CDXLScalarCaseTest
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarCaseTest::~CDXLScalarCaseTest() {
  m_mdid_type->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarCaseTest::GetDXLOperator() const {
  return EdxlopScalarCaseTest;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarCaseTest::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarCaseTest);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::MdidType
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarCaseTest::MdidType() const {
  return m_mdid_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::HasBoolResult
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
bool CDXLScalarCaseTest::HasBoolResult(CMDAccessor *md_accessor) const {
  return (IMDType::EtiBool == md_accessor->RetrieveType(m_mdid_type)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarCaseTest::AssertValid(const CDXLNode *dxlnode,
                                     bool  // validate_children
) const {
  GPOS_ASSERT(0 == dxlnode->Arity());
  GPOS_ASSERT(m_mdid_type->IsValid());
}
#endif  // GPOS_DEBUG

// EOF
