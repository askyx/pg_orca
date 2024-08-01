//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowKey.cpp
//
//	@doc:
//		Implementation of DXL window key
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLWindowKey.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::CDXLWindowKey
//
//	@doc:
//		Constructs a scalar window key node
//
//---------------------------------------------------------------------------
CDXLWindowKey::CDXLWindowKey()

    = default;

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::~CDXLWindowKey
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLWindowKey::~CDXLWindowKey() {
  CRefCount::SafeRelease(m_window_frame_dxl);
  CRefCount::SafeRelease(m_sort_col_list_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::SetWindowFrame
//
//	@doc:
//		Set window frame
//
//---------------------------------------------------------------------------
void CDXLWindowKey::SetWindowFrame(CDXLWindowFrame *window_frame) {
  // allow setting window frame only once
  GPOS_ASSERT(nullptr == m_window_frame_dxl);
  m_window_frame_dxl = window_frame;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::SetSortColList
//
//	@doc:
//		Set sort column list
//
//---------------------------------------------------------------------------
void CDXLWindowKey::SetSortColList(CDXLNode *sort_col_list_dxlnode) {
  // allow setting window frame only once
  GPOS_ASSERT(nullptr == m_sort_col_list_dxlnode);
  m_sort_col_list_dxlnode = sort_col_list_dxlnode;
}
