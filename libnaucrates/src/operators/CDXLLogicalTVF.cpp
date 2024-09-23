//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalTVF.cpp
//
//	@doc:
//		Implementation of DXL table-valued function
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalTVF.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::CDXLLogicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalTVF::CDXLLogicalTVF(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type, CMDName *mdname,
                               CDXLColDescrArray *pdrgdxlcd)
    : CDXLLogical(mp),
      m_func_mdid(mdid_func),
      m_return_type_mdid(mdid_return_type),
      m_mdname(mdname),
      m_dxl_col_descr_array(pdrgdxlcd) {
  GPOS_ASSERT(m_return_type_mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::~CDXLLogicalTVF
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalTVF::~CDXLLogicalTVF() {
  m_dxl_col_descr_array->Release();
  m_func_mdid->Release();
  m_return_type_mdid->Release();
  GPOS_DELETE(m_mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLLogicalTVF::GetDXLOperator() const {
  return EdxlopLogicalTVF;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLLogicalTVF::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenLogicalTVF);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::Arity
//
//	@doc:
//		Return number of return columns
//
//---------------------------------------------------------------------------
uint32_t CDXLLogicalTVF::Arity() const {
  return m_dxl_col_descr_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::GetColumnDescrAt
//
//	@doc:
//		Get the column descriptor at the given position
//
//---------------------------------------------------------------------------
const CDXLColDescr *CDXLLogicalTVF::GetColumnDescrAt(uint32_t ul) const {
  return (*m_dxl_col_descr_array)[ul];
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::IsColDefined
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
bool CDXLLogicalTVF::IsColDefined(uint32_t colid) const {
  const uint32_t size = Arity();
  for (uint32_t ulDescr = 0; ulDescr < size; ulDescr++) {
    uint32_t id = GetColumnDescrAt(ulDescr)->Id();
    if (id == colid) {
      return true;
    }
  }

  return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLLogicalTVF::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  // assert validity of function id and return type
  GPOS_ASSERT(nullptr != m_func_mdid);
  GPOS_ASSERT(nullptr != m_return_type_mdid);

  const uint32_t arity = dxlnode->Arity();
  for (uint32_t ul = 0; ul < arity; ++ul) {
    CDXLNode *dxlnode_arg = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypeScalar == dxlnode_arg->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      dxlnode_arg->GetOperator()->AssertValid(dxlnode_arg, validate_children);
    }
  }
}

#endif  // GPOS_DEBUG

// EOF
