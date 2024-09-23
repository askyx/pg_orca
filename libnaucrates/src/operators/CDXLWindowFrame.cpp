//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowFrame.cpp
//
//	@doc:
//		Implementation of DXL Window Frame
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLWindowFrame.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::CDXLWindowFrame
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLWindowFrame::CDXLWindowFrame(EdxlFrameSpec edxlfs, EdxlFrameExclusionStrategy frame_exc_strategy,
                                 CDXLNode *dxlnode_leading, CDXLNode *dxlnode_trailing, OID start_in_range_func,
                                 OID end_in_range_func, OID in_range_coll, bool in_range_asc, bool in_range_nulls_first)
    : m_dxl_win_frame_spec(edxlfs),
      m_dxl_frame_exclusion_strategy(frame_exc_strategy),
      m_dxlnode_leading(dxlnode_leading),
      m_dxlnode_trailing(dxlnode_trailing),
      m_start_in_range_func(start_in_range_func),
      m_end_in_range_func(end_in_range_func),
      m_in_range_coll(in_range_coll),
      m_in_range_asc(in_range_asc),
      m_in_range_nulls_first(in_range_nulls_first) {
  GPOS_ASSERT(EdxlfsSentinel > m_dxl_win_frame_spec);
  GPOS_ASSERT(EdxlfesSentinel > m_dxl_frame_exclusion_strategy);
  GPOS_ASSERT(nullptr != dxlnode_leading);
  GPOS_ASSERT(nullptr != dxlnode_trailing);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::~CDXLWindowFrame
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLWindowFrame::~CDXLWindowFrame() {
  m_dxlnode_leading->Release();
  m_dxlnode_trailing->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::PstrES
//
//	@doc:
//		Return the string representation of the window frame exclusion strategy
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLWindowFrame::PstrES(EdxlFrameExclusionStrategy edxles) {
  GPOS_ASSERT(EdxlfesSentinel > edxles);
  uint32_t window_frame_boundary_to_frame_boundary_mapping[][2] = {{EdxlfesNone, EdxltokenWindowESNone},
                                                                   {EdxlfesNulls, EdxltokenWindowESNulls},
                                                                   {EdxlfesCurrentRow, EdxltokenWindowESCurrentRow},
                                                                   {EdxlfesGroup, EdxltokenWindowESGroup},
                                                                   {EdxlfesTies, EdxltokenWindowESTies}};

  const uint32_t arity = GPOS_ARRAY_SIZE(window_frame_boundary_to_frame_boundary_mapping);
  for (uint32_t ul = 0; ul < arity; ul++) {
    uint32_t *pulElem = window_frame_boundary_to_frame_boundary_mapping[ul];
    if ((uint32_t)edxles == pulElem[0]) {
      Edxltoken edxltk = (Edxltoken)pulElem[1];
      return CDXLTokens::GetDXLTokenStr(edxltk);
      break;
    }
  }

  GPOS_ASSERT(!"Unrecognized window frame exclusion strategy");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::PstrFS
//
//	@doc:
//		Return the string representation of the window frame specification
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLWindowFrame::PstrFS(EdxlFrameSpec edxlfs) {
  GPOS_ASSERT(EdxlfsSentinel > edxlfs && "Unrecognized window frame specification");

  if (EdxlfsRow == edxlfs) {
    return CDXLTokens::GetDXLTokenStr(EdxltokenWindowFSRow);
  } else if (EdxlfsGroups == edxlfs) {
    return CDXLTokens::GetDXLTokenStr(EdxltokenWindowFSGroups);
  }

  return CDXLTokens::GetDXLTokenStr(EdxltokenWindowFSRange);
}
