//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSubPlan.cpp
//
//	@doc:
//		Implementation of DXL Scalar SubPlan operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarSubPlan.h"

#include "gpopt/base/COptCtxt.h"
#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::CDXLScalarSubPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarSubPlan::CDXLScalarSubPlan(CMemoryPool *mp, IMDId *first_col_type_mdid, CDXLColRefArray *dxl_colref_array,
                                     EdxlSubPlanType dxl_subplan_type, CDXLNode *dxlnode_test_expr, BOOL outer_param)
    : CDXLScalar(mp),
      m_first_col_type_mdid(first_col_type_mdid),
      m_dxl_colref_array(dxl_colref_array),
      m_dxl_subplan_type(dxl_subplan_type),
      m_dxlnode_test_expr(dxlnode_test_expr),
      m_outer_param(outer_param) {
  GPOS_ASSERT(EdxlSubPlanTypeSentinel > dxl_subplan_type);
  GPOS_ASSERT_IMP(EdxlSubPlanTypeAny == dxl_subplan_type || EdxlSubPlanTypeAll == dxl_subplan_type,
                  nullptr != dxlnode_test_expr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::~CDXLScalarSubPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarSubPlan::~CDXLScalarSubPlan() {
  m_first_col_type_mdid->Release();
  m_dxl_colref_array->Release();
  CRefCount::SafeRelease(m_dxlnode_test_expr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarSubPlan::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubPlan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::GetFirstColTypeMdId
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarSubPlan::GetFirstColTypeMdId() const {
  return m_first_col_type_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::HasBoolResult
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL CDXLScalarSubPlan::HasBoolResult(CMDAccessor *md_accessor) const {
  return (IMDType::EtiBool == md_accessor->RetrieveType(m_first_col_type_mdid)->GetDatumType());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::GetSubplanTypeStr
//
//	@doc:
//		Return a string representation of Subplan type
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarSubPlan::GetSubplanTypeStr() const {
  switch (m_dxl_subplan_type) {
    case EdxlSubPlanTypeScalar:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubPlanTypeScalar);

    case EdxlSubPlanTypeExists:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubPlanTypeExists);

    case EdxlSubPlanTypeNotExists:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubPlanTypeNotExists);

    case EdxlSubPlanTypeAny:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubPlanTypeAny);

    case EdxlSubPlanTypeAll:
      return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubPlanTypeAll);

    default:
      GPOS_ASSERT(!"Unrecognized subplan type");
      return nullptr;
  }
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarSubPlan::AssertValid(const CDXLNode *dxlnode, BOOL validate_children) const {
  GPOS_ASSERT(EdxlSubPlanIndexSentinel == dxlnode->Arity());

  // assert child plan is a physical plan and is valid

  CDXLNode *child_dxlnode = (*dxlnode)[EdxlSubPlanIndexChildPlan];
  GPOS_ASSERT(nullptr != child_dxlnode);
  GPOS_ASSERT(EdxloptypePhysical == child_dxlnode->GetOperator()->GetDXLOperatorType());
  GPOS_ASSERT_IMP(nullptr != m_dxlnode_test_expr,
                  EdxloptypeScalar == m_dxlnode_test_expr->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}
#endif  // GPOS_DEBUG

// EOF
