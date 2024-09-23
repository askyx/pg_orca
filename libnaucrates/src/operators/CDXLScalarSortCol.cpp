//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSortCol.cpp
//
//	@doc:
//		Implementation of DXL sorting columns for sort and motion operator nodes
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarSortCol.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::CDXLScalarSortCol
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSortCol::CDXLScalarSortCol(CMemoryPool *mp, uint32_t colid, IMDId *mdid_sort_op,
                                     CWStringConst *sort_op_name_str, bool sort_nulls_first)
    : CDXLScalar(mp),
      m_colid(colid),
      m_mdid_sort_op(mdid_sort_op),
      m_sort_op_name_str(sort_op_name_str),
      m_must_sort_nulls_first(sort_nulls_first) {
  GPOS_ASSERT(m_mdid_sort_op->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::~CDXLScalarSortCol
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSortCol::~CDXLScalarSortCol() {
  m_mdid_sort_op->Release();
  GPOS_DELETE(m_sort_op_name_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarSortCol::GetDXLOperator() const {
  return EdxlopScalarSortCol;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarSortCol::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSortCol);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::GetColId
//
//	@doc:
//		Id of the sorting column
//
//---------------------------------------------------------------------------
uint32_t CDXLScalarSortCol::GetColId() const {
  return m_colid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::GetMdIdSortOp
//
//	@doc:
//		Oid of the sorting operator for the column from the catalog
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarSortCol::GetMdIdSortOp() const {
  return m_mdid_sort_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::IsSortedNullsFirst
//
//	@doc:
//		Whether nulls are sorted before other values
//
//---------------------------------------------------------------------------
bool CDXLScalarSortCol::IsSortedNullsFirst() const {
  return m_must_sort_nulls_first;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarSortCol::AssertValid(const CDXLNode *dxlnode,
                                    bool  // validate_children
) const {
  GPOS_ASSERT(0 == dxlnode->Arity());
}
#endif  // GPOS_DEBUG

// EOF
