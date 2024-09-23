//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLPhysicalPartitionSelector.cpp
//
//	@doc:
//		Implementation of DXL physical partition selector
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalPartitionSelector.h"

#include "gpos/common/CBitSetIter.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::CDXLPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalPartitionSelector::CDXLPhysicalPartitionSelector(CMemoryPool *mp, IMDId *mdid_rel, uint32_t selector_id,
                                                             uint32_t scan_id, ULongPtrArray *parts)
    : CDXLPhysical(mp), m_rel_mdid(mdid_rel), m_selector_id(selector_id), m_scan_id(scan_id), m_parts(parts) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::~CDXLPhysicalPartitionSelector
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalPartitionSelector::~CDXLPhysicalPartitionSelector() {
  CRefCount::SafeRelease(m_parts);
  CRefCount::SafeRelease(m_rel_mdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLPhysicalPartitionSelector::GetDXLOperator() const {
  return EdxlopPhysicalPartitionSelector;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLPhysicalPartitionSelector::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalPartitionSelector);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLPhysicalPartitionSelector::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  const uint32_t arity = dxlnode->Arity();
  GPOS_ASSERT(6 == arity || 7 == arity);
  for (uint32_t idx = 0; idx < arity; ++idx) {
    CDXLNode *child_dxlnode = (*dxlnode)[idx];
    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
