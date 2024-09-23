//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalWindow.cpp
//
//	@doc:
//		Implementation of DXL physical window operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalWindow.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::CDXLPhysicalWindow
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalWindow::CDXLPhysicalWindow(CMemoryPool *mp, ULongPtrArray *part_by_colid_array,
                                       CDXLWindowKeyArray *window_key_array)
    : CDXLPhysical(mp), m_part_by_colid_array(part_by_colid_array), m_dxl_window_key_array(window_key_array) {
  GPOS_ASSERT(nullptr != m_part_by_colid_array);
  GPOS_ASSERT(nullptr != m_dxl_window_key_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::~CDXLPhysicalWindow
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalWindow::~CDXLPhysicalWindow() {
  m_part_by_colid_array->Release();
  m_dxl_window_key_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalWindow::GetDXLOperator() const {
  return EdxlopPhysicalWindow;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalWindow::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalWindow);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::UlPartCols
//
//	@doc:
//		Returns the number of partition columns
//
//---------------------------------------------------------------------------
uint32_t CDXLPhysicalWindow::PartByColsCount() const {
  return m_part_by_colid_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::UlWindowKeys
//
//	@doc:
//		Returns the number of window keys
//
//---------------------------------------------------------------------------
uint32_t CDXLPhysicalWindow::WindowKeysCount() const {
  return m_dxl_window_key_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::PdxlWindowKey
//
//	@doc:
//		Return the window key at a given position
//
//---------------------------------------------------------------------------
CDXLWindowKey *CDXLPhysicalWindow::GetDXLWindowKeyAt(uint32_t position) const {
  GPOS_ASSERT(position <= m_dxl_window_key_array->Size());
  return (*m_dxl_window_key_array)[position];
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalWindow::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  // assert proj list and filter are valid
  CDXLPhysical::AssertValid(dxlnode, validate_children);
  GPOS_ASSERT(nullptr != m_part_by_colid_array);
  GPOS_ASSERT(nullptr != m_dxl_window_key_array);
  GPOS_ASSERT(EdxlwindowIndexSentinel == dxlnode->Arity());
  CDXLNode *child_dxlnode = (*dxlnode)[EdxlwindowIndexChild];
  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}
#endif  // GPOS_DEBUG

// EOF
