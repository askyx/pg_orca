//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalUpdate.cpp
//
//	@doc:
//		Implementation of DXL logical update operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalUpdate.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::CDXLLogicalUpdate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalUpdate::CDXLLogicalUpdate(CMemoryPool *mp, CDXLTableDescr *table_descr, uint32_t ctid_colid,
                                     uint32_t segid_colid, ULongPtrArray *delete_colid_array,
                                     ULongPtrArray *insert_colid_array)
    : CDXLLogical(mp),
      m_dxl_table_descr(table_descr),
      m_ctid_colid(ctid_colid),
      m_segid_colid(segid_colid),
      m_deletion_colid_array(delete_colid_array),
      m_insert_colid_array(insert_colid_array) {
  GPOS_ASSERT(nullptr != table_descr);
  GPOS_ASSERT(nullptr != delete_colid_array);
  GPOS_ASSERT(nullptr != insert_colid_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::~CDXLLogicalUpdate
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalUpdate::~CDXLLogicalUpdate() {
  m_dxl_table_descr->Release();
  m_deletion_colid_array->Release();
  m_insert_colid_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalUpdate::GetDXLOperator() const {
  return EdxlopLogicalUpdate;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalUpdate::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalUpdate);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalUpdate::AssertValid(const CDXLNode *node, bool validate_children) const {
  GPOS_ASSERT(1 == node->Arity());

  CDXLNode *child_dxlnode = (*node)[0];
  GPOS_ASSERT(EdxloptypeLogical == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}

#endif  // GPOS_DEBUG

// EOF
