//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarFuncExpr.cpp
//
//	@doc:
//		Implementation of DXL Scalar FuncExpr
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarFuncExpr.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::CDXLScalarFuncExpr
//
//	@doc:
//		Constructs a scalar FuncExpr node
//
//---------------------------------------------------------------------------
CDXLScalarFuncExpr::CDXLScalarFuncExpr(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type,
                                       int32_t return_type_modifier, bool fRetSet, bool funcvariadic)
    : CDXLScalar(mp),
      m_func_mdid(mdid_func),
      m_return_type_mdid(mdid_return_type),
      m_return_type_modifier(return_type_modifier),
      m_returns_set(fRetSet),
      m_funcvariadic(funcvariadic) {
  GPOS_ASSERT(m_func_mdid->IsValid());
  GPOS_ASSERT(m_return_type_mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::~CDXLScalarFuncExpr
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarFuncExpr::~CDXLScalarFuncExpr() {
  m_func_mdid->Release();
  m_return_type_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarFuncExpr::GetDXLOperator() const {
  return EdxlopScalarFuncExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarFuncExpr::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarFuncExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::FuncMdId
//
//	@doc:
//		Returns function id
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarFuncExpr::FuncMdId() const {
  return m_func_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::ReturnTypeMdId
//
//	@doc:
//		Return type
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarFuncExpr::ReturnTypeMdId() const {
  return m_return_type_mdid;
}

int32_t CDXLScalarFuncExpr::TypeModifier() const {
  return m_return_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::ReturnsSet
//
//	@doc:
//		Returns whether the function returns a set
//
//---------------------------------------------------------------------------
bool CDXLScalarFuncExpr::ReturnsSet() const {
  return m_returns_set;
}
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::IsFuncVariadic
//
//	@doc:
//		Returns whether the function is variadic
//
//---------------------------------------------------------------------------
bool CDXLScalarFuncExpr::IsFuncVariadic() const {
  return m_funcvariadic;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::HasBoolResult
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
bool CDXLScalarFuncExpr::HasBoolResult(CMDAccessor *md_accessor) const {
  IMDId *mdid = md_accessor->RetrieveFunc(m_func_mdid)->GetResultTypeMdid();
  return (IMDType::EtiBool == md_accessor->RetrieveType(mdid)->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFuncExpr::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarFuncExpr::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  for (uint32_t ul = 0; ul < dxlnode->Arity(); ++ul) {
    CDXLNode *dxlnode_arg = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypeScalar == dxlnode_arg->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      dxlnode_arg->GetOperator()->AssertValid(dxlnode_arg, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
