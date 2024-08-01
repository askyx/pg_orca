//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalWindow.cpp
//
//	@doc:
//		Implementation of DXL logical window operator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalWindow.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::CDXLLogicalWindow
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalWindow::CDXLLogicalWindow(CMemoryPool *mp, CDXLWindowSpecArray *window_spec_array)
    : CDXLLogical(mp), m_window_spec_array(window_spec_array) {
  GPOS_ASSERT(nullptr != m_window_spec_array);
  GPOS_ASSERT(0 < m_window_spec_array->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::~CDXLLogicalWindow
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalWindow::~CDXLLogicalWindow() {
  m_window_spec_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalWindow::GetDXLOperator() const {
  return EdxlopLogicalWindow;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalWindow::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalWindow);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::GetWindowKeyAt
//
//	@doc:
//		Return the window specification at a given position
//
//---------------------------------------------------------------------------
CDXLWindowSpec *CDXLLogicalWindow::GetWindowKeyAt(ULONG idx) const {
  GPOS_ASSERT(idx <= m_window_spec_array->Size());
  return (*m_window_spec_array)[idx];
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalWindow::AssertValid(const CDXLNode *node, BOOL validate_children) const {
  GPOS_ASSERT(2 == node->Arity());

  CDXLNode *proj_list_dxlnode = (*node)[0];
  CDXLNode *child_dxlnode = (*node)[1];

  GPOS_ASSERT(EdxlopScalarProjectList == proj_list_dxlnode->GetOperator()->GetDXLOperator());
  GPOS_ASSERT(EdxloptypeLogical == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    proj_list_dxlnode->GetOperator()->AssertValid(proj_list_dxlnode, validate_children);
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }

  const ULONG arity = proj_list_dxlnode->Arity();
  for (ULONG idx = 0; idx < arity; ++idx) {
    CDXLNode *proj_elem = (*proj_list_dxlnode)[idx];
    GPOS_ASSERT(EdxlopScalarIdent != proj_elem->GetOperator()->GetDXLOperator());
  }

  GPOS_ASSERT(nullptr != m_window_spec_array);
  GPOS_ASSERT(0 < m_window_spec_array->Size());
}

#endif  // GPOS_DEBUG

// EOF
