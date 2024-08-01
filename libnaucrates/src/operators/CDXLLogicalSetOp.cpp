//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalSetOp.cpp
//
//	@doc:
//		Implementation of DXL logical set operator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalSetOp.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::CDXLLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalSetOp::CDXLLogicalSetOp(CMemoryPool *mp, EdxlSetOpType edxlsetoptype, CDXLColDescrArray *col_descr_array,
                                   ULongPtr2dArray *input_colids_arrays, BOOL fCastAcrossInputs)
    : CDXLLogical(mp),
      m_set_operation_dxl_type(edxlsetoptype),
      m_col_descr_array(col_descr_array),
      m_input_colids_arrays(input_colids_arrays),
      m_cast_across_input_req(fCastAcrossInputs) {
  GPOS_ASSERT(nullptr != m_col_descr_array);
  GPOS_ASSERT(nullptr != m_input_colids_arrays);
  GPOS_ASSERT(EdxlsetopSentinel > edxlsetoptype);

#ifdef GPOS_DEBUG
  const ULONG num_of_cols = m_col_descr_array->Size();
  const ULONG length = m_input_colids_arrays->Size();
  for (ULONG idx = 0; idx < length; idx++) {
    ULongPtrArray *input_colids_array = (*m_input_colids_arrays)[idx];
    GPOS_ASSERT(num_of_cols == input_colids_array->Size());
  }

#endif
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::~CDXLLogicalSetOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalSetOp::~CDXLLogicalSetOp() {
  m_col_descr_array->Release();
  m_input_colids_arrays->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalSetOp::GetDXLOperator() const {
  return EdxlopLogicalSetOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalSetOp::GetOpNameStr() const {
  switch (m_set_operation_dxl_type) {
    case EdxlsetopUnion:
      return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalUnion);

    case EdxlsetopUnionAll:
      return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalUnionAll);

    case EdxlsetopIntersect:
      return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalIntersect);

    case EdxlsetopIntersectAll:
      return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalIntersectAll);

    case EdxlsetopDifference:
      return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalDifference);

    case EdxlsetopDifferenceAll:
      return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalDifferenceAll);

    default:
      GPOS_ASSERT(!"Unrecognized set operator type");
      return nullptr;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::IsColDefined
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
BOOL CDXLLogicalSetOp::IsColDefined(ULONG colid) const {
  const ULONG size = Arity();
  for (ULONG descr_id = 0; descr_id < size; descr_id++) {
    ULONG id = GetColumnDescrAt(descr_id)->Id();
    if (id == colid) {
      return true;
    }
  }

  return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalSetOp::AssertValid(const CDXLNode *node, BOOL validate_children) const {
  GPOS_ASSERT(2 <= node->Arity());
  GPOS_ASSERT(nullptr != m_col_descr_array);

  // validate output columns
  const ULONG num_of_output_cols = m_col_descr_array->Size();
  GPOS_ASSERT(0 < num_of_output_cols);

  // validate children
  const ULONG num_of_child = node->Arity();
  for (ULONG idx = 0; idx < num_of_child; ++idx) {
    CDXLNode *child_dxlnode = (*node)[idx];
    GPOS_ASSERT(EdxloptypeLogical == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}

#endif  // GPOS_DEBUG

// EOF
