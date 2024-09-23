//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalGet.cpp
//
//	@doc:
//		Implementation of DXL logical get operator
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalGet.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::CDXLLogicalGet
//
//	@doc:
//		Construct a logical get operator node given its table descriptor rtable entry
//
//---------------------------------------------------------------------------
CDXLLogicalGet::CDXLLogicalGet(CMemoryPool *mp, CDXLTableDescr *table_descr, bool hasSecurityQuals)
    : CDXLLogical(mp), m_dxl_table_descr(table_descr), m_has_security_quals(hasSecurityQuals) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::~CDXLLogicalGet
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLLogicalGet::~CDXLLogicalGet() {
  CRefCount::SafeRelease(m_dxl_table_descr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalGet::GetDXLOperator() const {
  return EdxlopLogicalGet;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalGet::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalGet);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::GetDXLTableDescr
//
//	@doc:
//		Table descriptor for the table scan
//
//---------------------------------------------------------------------------
CDXLTableDescr *CDXLLogicalGet::GetDXLTableDescr() const {
  return m_dxl_table_descr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::IsColDefined
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
bool CDXLLogicalGet::IsColDefined(uint32_t colid) const {
  const uint32_t size = m_dxl_table_descr->Arity();
  for (uint32_t descr_id = 0; descr_id < size; descr_id++) {
    uint32_t id = m_dxl_table_descr->GetColumnDescrAt(descr_id)->Id();
    if (id == colid) {
      return true;
    }
  }

  return false;
}

bool CDXLLogicalGet::HasSecurityQuals() const {
  return m_has_security_quals;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalGet::AssertValid(const CDXLNode *,  // dxlnode
                                 bool               // validate_children
) const {
  // assert validity of table descriptor
  GPOS_ASSERT(nullptr != m_dxl_table_descr);
  GPOS_ASSERT(nullptr != m_dxl_table_descr->MdName());
  GPOS_ASSERT(m_dxl_table_descr->MdName()->GetMDName()->IsValid());
}
#endif  // GPOS_DEBUG

// EOF
