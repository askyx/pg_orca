//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalTableScan.cpp
//
//	@doc:
//		Implementation of DXL physical table scan operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalTableScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::CDXLPhysicalTableScan
//
//	@doc:
//		Construct a table scan node with uninitialized table descriptor
//
//---------------------------------------------------------------------------
CDXLPhysicalTableScan::CDXLPhysicalTableScan(CMemoryPool *mp) : CDXLPhysical(mp), m_dxl_table_descr(nullptr) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::CDXLPhysicalTableScan
//
//	@doc:
//		Construct a table scan node given its table descriptor
//
//---------------------------------------------------------------------------
CDXLPhysicalTableScan::CDXLPhysicalTableScan(CMemoryPool *mp, CDXLTableDescr *table_descr)
    : CDXLPhysical(mp), m_dxl_table_descr(table_descr) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::~CDXLPhysicalTableScan
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalTableScan::~CDXLPhysicalTableScan() {
  CRefCount::SafeRelease(m_dxl_table_descr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::SetTableDescriptor
//
//	@doc:
//		Set table descriptor
//
//---------------------------------------------------------------------------
void CDXLPhysicalTableScan::SetTableDescriptor(CDXLTableDescr *table_descr) {
  // allow setting table descriptor only once
  GPOS_ASSERT(nullptr == m_dxl_table_descr);

  m_dxl_table_descr = table_descr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalTableScan::GetDXLOperator() const {
  return EdxlopPhysicalTableScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalTableScan::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::GetDXLTableDescr
//
//	@doc:
//		Table descriptor for the table scan
//
//---------------------------------------------------------------------------
const CDXLTableDescr *CDXLPhysicalTableScan::GetDXLTableDescr() {
  return m_dxl_table_descr;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalTableScan::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  // assert proj list and filter are valid
  CDXLPhysical::AssertValid(dxlnode, validate_children);

  // table scan has only 2 children
  GPOS_ASSERT(2 == dxlnode->Arity());

  // assert validity of table descriptor
  GPOS_ASSERT(nullptr != m_dxl_table_descr);
  GPOS_ASSERT(nullptr != m_dxl_table_descr->MdName());
  GPOS_ASSERT(m_dxl_table_descr->MdName()->GetMDName()->IsValid());
}
#endif  // GPOS_DEBUG

// EOF
