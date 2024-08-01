//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalAgg.cpp
//
//	@doc:
//		Implementation of DXL physical aggregate operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalAgg.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::CDXLPhysicalAgg
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalAgg::CDXLPhysicalAgg(CMemoryPool *mp, EdxlAggStrategy dxl_agg_strategy, BOOL stream_safe)
    : CDXLPhysical(mp),
      m_grouping_colids_array(nullptr),
      m_dxl_agg_strategy(dxl_agg_strategy),
      m_stream_safe(stream_safe) {
  GPOS_ASSERT_IMP(stream_safe, (EdxlaggstrategyHashed == dxl_agg_strategy));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::~CDXLPhysicalAgg
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalAgg::~CDXLPhysicalAgg() {
  CRefCount::SafeRelease(m_grouping_colids_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalAgg::GetDXLOperator() const {
  return EdxlopPhysicalAgg;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::GetAggStrategy
//
//	@doc:
//		Aggregation strategy
//
//---------------------------------------------------------------------------
EdxlAggStrategy CDXLPhysicalAgg::GetAggStrategy() const {
  return m_dxl_agg_strategy;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalAgg::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalAggregate);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::GetAggStrategyNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalAgg::GetAggStrategyNameStr() const {
  switch (m_dxl_agg_strategy) {
    case EdxlaggstrategyPlain:
      return CDXLTokens::GetDXLTokenStr(EdxltokenAggStrategyPlain);
    case EdxlaggstrategySorted:
      return CDXLTokens::GetDXLTokenStr(EdxltokenAggStrategySorted);
    case EdxlaggstrategyHashed:
      return CDXLTokens::GetDXLTokenStr(EdxltokenAggStrategyHashed);
    default:
      GPOS_ASSERT(!"Unrecognized aggregation strategy");
      return nullptr;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::GetGroupingColidArray
//
//	@doc:
//		Grouping column indices
//
//---------------------------------------------------------------------------
const ULongPtrArray *CDXLPhysicalAgg::GetGroupingColidArray() const {
  return m_grouping_colids_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::SetGroupingCols
//
//	@doc:
//		Sets array of grouping columns
//
//---------------------------------------------------------------------------
void CDXLPhysicalAgg::SetGroupingCols(ULongPtrArray *grouping_colids_array) {
  GPOS_ASSERT(nullptr != grouping_colids_array);
  m_grouping_colids_array = grouping_colids_array;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalAgg::AssertValid(const CDXLNode *node, BOOL validate_children) const {
  // assert proj list and filter are valid
  CDXLPhysical::AssertValid(node, validate_children);

  GPOS_ASSERT((EdxlaggstrategySentinel > m_dxl_agg_strategy) && (EdxlaggstrategyPlain <= m_dxl_agg_strategy));

  GPOS_ASSERT(EdxlaggIndexSentinel == node->Arity());
  GPOS_ASSERT(nullptr != m_grouping_colids_array);

  CDXLNode *child_dxlnode = (*node)[EdxlaggIndexChild];
  GPOS_ASSERT(EdxloptypePhysical == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}
#endif  // GPOS_DEBUG

// EOF
