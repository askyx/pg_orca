//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDML.cpp
//
//	@doc:
//		Implementation of DXL physical DML operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalDML.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::CDXLPhysicalDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalDML::CDXLPhysicalDML(CMemoryPool *mp, const EdxlDmlType dxl_dml_type, CDXLTableDescr *table_descr,
                                 ULongPtrArray *src_colids_array, ULONG action_colid, ULONG ctid_colid,
                                 ULONG segid_colid)
    : CDXLPhysical(mp),
      m_dxl_dml_type(dxl_dml_type),
      m_dxl_table_descr(table_descr),
      m_src_colids_array(src_colids_array),
      m_action_colid(action_colid),
      m_ctid_colid(ctid_colid),
      m_segid_colid(segid_colid) {
  GPOS_ASSERT(EdxldmlSentinel > dxl_dml_type);
  GPOS_ASSERT(nullptr != table_descr);
  GPOS_ASSERT(nullptr != src_colids_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::~CDXLPhysicalDML
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalDML::~CDXLPhysicalDML() {
  m_dxl_table_descr->Release();
  m_src_colids_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalDML::GetDXLOperator() const {
  return EdxlopPhysicalDML;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalDML::GetOpNameStr() const {
  switch (m_dxl_dml_type) {
    case Edxldmlinsert:
      return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalDMLInsert);
    case Edxldmldelete:
      return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalDMLDelete);
    case Edxldmlupdate:
      return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalDMLUpdate);
    default:
      return nullptr;
  }
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalDML::AssertValid(const CDXLNode *node, BOOL validate_children) const {
  GPOS_ASSERT(2 == node->Arity());
  CDXLNode *child_dxlnode = (*node)[1];
  GPOS_ASSERT(EdxloptypePhysical == child_dxlnode->GetOperator()->GetDXLOperatorType());

  if (validate_children) {
    child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
  }
}

#endif  // GPOS_DEBUG

// EOF
