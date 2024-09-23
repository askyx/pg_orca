//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarValuesList.cpp
//
//	@doc:
//		Implementation of DXL value list operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarValuesList.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

// constructs a m_bytearray_value list node
CDXLScalarValuesList::CDXLScalarValuesList(CMemoryPool *mp) : CDXLScalar(mp) {}

// destructor
CDXLScalarValuesList::~CDXLScalarValuesList() = default;

// operator type
Edxlopid CDXLScalarValuesList::GetDXLOperator() const {
  return EdxlopScalarValuesList;
}

// operator name
const CWStringConst *CDXLScalarValuesList::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarValuesList);
}

// conversion function
CDXLScalarValuesList *CDXLScalarValuesList::Cast(CDXLOperator *dxl_op) {
  GPOS_ASSERT(nullptr != dxl_op);
  GPOS_ASSERT(EdxlopScalarValuesList == dxl_op->GetDXLOperator());

  return dynamic_cast<CDXLScalarValuesList *>(dxl_op);
}

// does the operator return a boolean result
bool CDXLScalarValuesList::HasBoolResult(CMDAccessor *  // md_accessor
) const {
  return false;
}

#ifdef GPOS_DEBUG

// checks whether operator node is well-structured
void CDXLScalarValuesList::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  const uint32_t arity = dxlnode->Arity();

  for (uint32_t idx = 0; idx < arity; ++idx) {
    CDXLNode *pdxlnConstVal = (*dxlnode)[idx];
    GPOS_ASSERT(EdxloptypeScalar == pdxlnConstVal->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      pdxlnConstVal->GetOperator()->AssertValid(pdxlnConstVal, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
