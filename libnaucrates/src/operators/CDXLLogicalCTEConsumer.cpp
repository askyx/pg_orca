//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of DXL logical CTE Consumer operator
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalCTEConsumer.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::CDXLLogicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalCTEConsumer::CDXLLogicalCTEConsumer(CMemoryPool *mp, uint32_t id, ULongPtrArray *output_colids_array)
    : CDXLLogical(mp), m_id(id), m_output_colids_array(output_colids_array) {
  GPOS_ASSERT(nullptr != output_colids_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::~CDXLLogicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalCTEConsumer::~CDXLLogicalCTEConsumer() {
  m_output_colids_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalCTEConsumer::GetDXLOperator() const {
  return EdxlopLogicalCTEConsumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalCTEConsumer::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalCTEConsumer);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::IsColDefined
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
bool CDXLLogicalCTEConsumer::IsColDefined(uint32_t colid) const {
  const uint32_t size = m_output_colids_array->Size();
  for (uint32_t idx = 0; idx < size; idx++) {
    uint32_t id = *((*m_output_colids_array)[idx]);
    if (id == colid) {
      return true;
    }
  }

  return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalCTEConsumer::AssertValid(const CDXLNode *dxlnode,
                                         bool  // validate_children
) const {
  GPOS_ASSERT(0 == dxlnode->Arity());
}
#endif  // GPOS_DEBUG

// EOF
