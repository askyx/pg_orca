//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarProjElem.cpp
//
//	@doc:
//		Implementation of DXL projection list element operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarProjElem.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjElem::CDXLScalarProjElem
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarProjElem::CDXLScalarProjElem(CMemoryPool *mp, uint32_t id, const CMDName *mdname)
    : CDXLScalar(mp), m_id(id), m_mdname(mdname) {
  GPOS_ASSERT(nullptr != mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjElem::~CDXLScalarProjElem
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarProjElem::~CDXLScalarProjElem() {
  GPOS_DELETE(m_mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjElem::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarProjElem::GetDXLOperator() const {
  return EdxlopScalarProjectElem;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjElem::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarProjElem::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarProjElem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjElem::Id
//
//	@doc:
//		Col id for this project element
//
//---------------------------------------------------------------------------
uint32_t CDXLScalarProjElem::Id() const {
  return m_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjElem::GetMdNameAlias
//
//	@doc:
//		Alias
//
//---------------------------------------------------------------------------
const CMDName *CDXLScalarProjElem::GetMdNameAlias() const {
  return m_mdname;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjElem::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarProjElem::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  GPOS_ASSERT(1 == dxlnode->Arity());
  CDXLNode *child_dxlnode = (*dxlnode)[0];

  GPOS_ASSERT(EdxloptypeScalar == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}

#endif  // GPOS_DEBUG

// EOF
