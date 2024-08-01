//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalSplit.cpp
//
//	@doc:
//		Implementation of DXL physical Split operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalSplit.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::CDXLPhysicalSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalSplit::CDXLPhysicalSplit(CMemoryPool *mp, ULongPtrArray *delete_colid_array,
                                     ULongPtrArray *insert_colid_array, ULONG action_colid, ULONG ctid_colid,
                                     ULONG segid_colid)
    : CDXLPhysical(mp),
      m_deletion_colid_array(delete_colid_array),
      m_insert_colid_array(insert_colid_array),
      m_action_colid(action_colid),
      m_ctid_colid(ctid_colid),
      m_segid_colid(segid_colid) {
  GPOS_ASSERT(nullptr != delete_colid_array);
  GPOS_ASSERT(nullptr != insert_colid_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::~CDXLPhysicalSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalSplit::~CDXLPhysicalSplit() {
  m_deletion_colid_array->Release();
  m_insert_colid_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalSplit::GetDXLOperator() const {
  return EdxlopPhysicalSplit;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalSplit::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalSplit);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalSplit::AssertValid(const CDXLNode *dxlnode, BOOL validate_children) const {
  GPOS_ASSERT(2 == dxlnode->Arity());
  CDXLNode *child_dxlnode = (*dxlnode)[1];
  GPOS_ASSERT(EdxloptypePhysical == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}

#endif  // GPOS_DEBUG

// EOF
