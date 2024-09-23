//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarHashCondList.cpp
//
//	@doc:
//		Implementation of DXL hash condition lists for hash join operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarHashCondList.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashCondList::CDXLScalarHashCondList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarHashCondList::CDXLScalarHashCondList(CMemoryPool *mp) : CDXLScalar(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashCondList::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarHashCondList::GetDXLOperator() const {
  return EdxlopScalarHashCondList;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashCondList::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarHashCondList::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarHashCondList);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashCondList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarHashCondList::AssertValid(const CDXLNode *node, bool validate_children) const {
  GPOS_ASSERT(nullptr != node);

  const uint32_t arity = node->Arity();
  for (uint32_t ul = 0; ul < arity; ul++) {
    CDXLNode *child_dxlnode = (*node)[ul];
    GPOS_ASSERT(EdxloptypeScalar == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
