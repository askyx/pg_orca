//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalProject.cpp
//
//	@doc:
//		Implementation of DXL logical project operator
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalProject.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::CDXLLogicalProject
//
//	@doc:
//		Construct a DXL Logical project node
//
//---------------------------------------------------------------------------
CDXLLogicalProject::CDXLLogicalProject(CMemoryPool *mp) : CDXLLogical(mp), m_mdname_alias(nullptr) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalProject::GetDXLOperator() const {
  return EdxlopLogicalProject;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::MdName
//
//	@doc:
//		Returns alias name
//
//---------------------------------------------------------------------------
const CMDName *CDXLLogicalProject::MdName() const {
  return m_mdname_alias;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::SetAliasName
//
//	@doc:
//		Set alias name
//
//---------------------------------------------------------------------------
void CDXLLogicalProject::SetAliasName(CMDName *mdname) {
  GPOS_ASSERT(nullptr == m_mdname_alias);
  GPOS_ASSERT(nullptr != mdname);

  m_mdname_alias = mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalProject::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalProject);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalProject::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  GPOS_ASSERT(2 == dxlnode->Arity());

  CDXLNode *proj_list_dxlnode = (*dxlnode)[0];
  CDXLNode *child_dxlnode = (*dxlnode)[1];

  GPOS_ASSERT(EdxlopScalarProjectList == proj_list_dxlnode->GetOperator()->GetDXLOperator());
  GPOS_ASSERT(EdxloptypeLogical == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    proj_list_dxlnode->GetOperator()->AssertValid(proj_list_dxlnode, validate_children);
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }

  const uint32_t arity = proj_list_dxlnode->Arity();
  for (uint32_t ul = 0; ul < arity; ++ul) {
    CDXLNode *pdxlnPrEl = (*proj_list_dxlnode)[ul];
    GPOS_ASSERT(EdxlopScalarIdent != pdxlnPrEl->GetOperator()->GetDXLOperator());
  }
}
#endif  // GPOS_DEBUG

// EOF
