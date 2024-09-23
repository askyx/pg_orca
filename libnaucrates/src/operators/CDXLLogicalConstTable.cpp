//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDXLLogicalConstTable.cpp
//
//	@doc:
//		Implementation of DXL logical constant tables
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalConstTable.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::CDXLLogicalConstTable
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLLogicalConstTable::CDXLLogicalConstTable(CMemoryPool *mp, CDXLColDescrArray *col_descr_array,
                                             CDXLDatum2dArray *const_tuples_datum_array)
    : CDXLLogical(mp), m_col_descr_array(col_descr_array), m_const_tuples_datum_array(const_tuples_datum_array) {
  GPOS_ASSERT(nullptr != col_descr_array);
  GPOS_ASSERT(nullptr != const_tuples_datum_array);

#ifdef GPOS_DEBUG
  const uint32_t length = const_tuples_datum_array->Size();
  for (uint32_t idx = 0; idx < length; idx++) {
    CDXLDatumArray *pdrgpdxldatum = (*const_tuples_datum_array)[idx];
    GPOS_ASSERT(pdrgpdxldatum->Size() == col_descr_array->Size());
  }
#endif
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::~CDXLLogicalConstTable
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLLogicalConstTable::~CDXLLogicalConstTable() {
  m_col_descr_array->Release();
  m_const_tuples_datum_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalConstTable::GetDXLOperator() const {
  return EdxlopLogicalConstTable;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalConstTable::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalConstTable);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::GetColumnDescrAt
//
//	@doc:
//		Type of const table element at given position
//
//---------------------------------------------------------------------------
CDXLColDescr *CDXLLogicalConstTable::GetColumnDescrAt(uint32_t idx) const {
  GPOS_ASSERT(m_col_descr_array->Size() > idx);
  return (*m_col_descr_array)[idx];
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::Arity
//
//	@doc:
//		Const table arity
//
//---------------------------------------------------------------------------
uint32_t CDXLLogicalConstTable::Arity() const {
  return m_col_descr_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::IsColDefined
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
bool CDXLLogicalConstTable::IsColDefined(uint32_t colid) const {
  const uint32_t size = Arity();
  for (uint32_t descr_idx = 0; descr_idx < size; descr_idx++) {
    uint32_t id = GetColumnDescrAt(descr_idx)->Id();
    if (id == colid) {
      return true;
    }
  }

  return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalConstTable::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalConstTable::AssertValid(const CDXLNode *node,
                                        bool  // validate_children
) const {
  // assert validity of col descr
  GPOS_ASSERT(m_col_descr_array != nullptr);
  GPOS_ASSERT(0 < m_col_descr_array->Size());
  GPOS_ASSERT(0 == node->Arity());
}
#endif  // GPOS_DEBUG

// EOF
