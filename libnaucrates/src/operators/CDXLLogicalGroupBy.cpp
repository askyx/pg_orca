//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalGroupBy.cpp
//
//	@doc:
//		Implementation of DXL logical group by operator
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalGroupBy.h"

#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::CDXLLogicalGroupBy
//
//	@doc:
//		Construct a DXL Logical group by node
//
//---------------------------------------------------------------------------
CDXLLogicalGroupBy::CDXLLogicalGroupBy(CMemoryPool *mp) : CDXLLogical(mp), m_grouping_colid_array(nullptr) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::CDXLLogicalGroupBy
//
//	@doc:
//		Construct a DXL Logical group by node
//
//---------------------------------------------------------------------------
CDXLLogicalGroupBy::CDXLLogicalGroupBy(CMemoryPool *mp, ULongPtrArray *pdrgpulGrpColIds)
    : CDXLLogical(mp), m_grouping_colid_array(pdrgpulGrpColIds) {
  GPOS_ASSERT(nullptr != pdrgpulGrpColIds);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::~CDXLLogicalGroupBy
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLLogicalGroupBy::~CDXLLogicalGroupBy() {
  CRefCount::SafeRelease(m_grouping_colid_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalGroupBy::GetDXLOperator() const {
  return EdxlopLogicalGrpBy;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalGroupBy::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalGrpBy);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::SetGroupingColumns
//
//	@doc:
//		Sets array of grouping columns
//
//---------------------------------------------------------------------------
void CDXLLogicalGroupBy::SetGroupingColumns(ULongPtrArray *grouping_colid_array) {
  GPOS_ASSERT(nullptr != grouping_colid_array);
  m_grouping_colid_array = grouping_colid_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::GetGroupingColidArray
//
//	@doc:
//		Grouping column indices
//
//---------------------------------------------------------------------------
const ULongPtrArray *CDXLLogicalGroupBy::GetGroupingColidArray() const {
  return m_grouping_colid_array;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalGroupBy::AssertValid(const CDXLNode *node, bool validate_children) const {
  // 1 Child node
  // 1 Group By project list

  const uint32_t num_of_child = node->Arity();
  GPOS_ASSERT(2 == num_of_child);

  CDXLNode *proj_list = (*node)[0];
  GPOS_ASSERT(EdxlopScalarProjectList == proj_list->GetOperator()->GetDXLOperator());

  CDXLNode *dxl_op_type = (*node)[1];
  GPOS_ASSERT(EdxloptypeLogical == dxl_op_type->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    for (uint32_t idx = 0; idx < num_of_child; idx++) {
      CDXLNode *child_dxlnode = (*node)[idx];
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }

  const uint32_t num_of_proj_elem = proj_list->Arity();
  for (uint32_t idx = 0; idx < num_of_proj_elem; ++idx) {
    CDXLNode *proj_elem = (*proj_list)[idx];
    GPOS_ASSERT(EdxlopScalarIdent != proj_elem->GetOperator()->GetDXLOperator());
  }
}
#endif  // GPOS_DEBUG

// EOF
