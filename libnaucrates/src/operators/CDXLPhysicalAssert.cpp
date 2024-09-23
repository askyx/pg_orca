//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalAssert.cpp
//
//	@doc:
//		Implementation of DXL physical assert operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalAssert.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAssert::CDXLPhysicalAssert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalAssert::CDXLPhysicalAssert(CMemoryPool *mp, const char *sql_state) : CDXLPhysical(mp) {
  GPOS_ASSERT(nullptr != sql_state);
  GPOS_ASSERT(GPOS_SQLSTATE_LENGTH == clib::Strlen(sql_state));
  clib::Strncpy(m_sql_state, sql_state, GPOS_SQLSTATE_LENGTH);
  m_sql_state[GPOS_SQLSTATE_LENGTH] = '\0';
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAssert::~CDXLPhysicalAssert
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalAssert::~CDXLPhysicalAssert() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAssert::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalAssert::GetDXLOperator() const {
  return EdxlopPhysicalAssert;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAssert::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalAssert::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalAssert);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAssert::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalAssert::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  GPOS_ASSERT(3 == dxlnode->Arity());

  CDXLNode *proj_list_dxlnode = (*dxlnode)[EdxlassertIndexProjList];
  GPOS_ASSERT(EdxlopScalarProjectList == proj_list_dxlnode->GetOperator()->GetDXLOperator());

  CDXLNode *predicate_dxlnode = (*dxlnode)[EdxlassertIndexFilter];
  GPOS_ASSERT(EdxlopScalarAssertConstraintList == predicate_dxlnode->GetOperator()->GetDXLOperator());

  CDXLNode *physical_child_dxlnode = (*dxlnode)[EdxlassertIndexChild];
  GPOS_ASSERT(EdxloptypePhysical == physical_child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    for (uint32_t ul = 0; ul < 3; ul++) {
      CDXLNode *child_dxlnode = (*dxlnode)[ul];
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
