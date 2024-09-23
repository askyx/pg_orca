//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarArray.cpp
//
//	@doc:
//		Implementation of DXL arrays
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArray.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::CDXLScalarArray
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArray::CDXLScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid,
                                 bool multi_dimensional_array)
    : CDXLScalar(mp),
      m_elem_type_mdid(elem_type_mdid),
      m_array_type_mdid(array_type_mdid),
      m_multi_dimensional_array(multi_dimensional_array) {
  GPOS_ASSERT(m_elem_type_mdid->IsValid());
  GPOS_ASSERT(m_array_type_mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::~CDXLScalarArray
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarArray::~CDXLScalarArray() {
  m_elem_type_mdid->Release();
  m_array_type_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarArray::GetDXLOperator() const {
  return EdxlopScalarArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarArray::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarArray);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::PmdidElem
//
//	@doc:
//		Id of base element type
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarArray::ElementTypeMDid() const {
  return m_elem_type_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::PmdidArray
//
//	@doc:
//		Id of array type
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarArray::ArrayTypeMDid() const {
  return m_array_type_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::FMultiDimensional
//
//	@doc:
//		Is this a multi-dimensional array
//
//---------------------------------------------------------------------------
bool CDXLScalarArray::IsMultiDimensional() const {
  return m_multi_dimensional_array;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarArray::AssertValid(const CDXLNode *dxlnode, bool validate_children) const {
  const uint32_t arity = dxlnode->Arity();
  for (uint32_t ul = 0; ul < arity; ++ul) {
    CDXLNode *child_dxlnode = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypeScalar == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
