//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CDXLScalarFieldSelect.cpp
//
//	@doc:
//		Implementation of DXL Scalar FIELDSELECT operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarFieldSelect.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::CDXLScalarFieldSelect
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarFieldSelect::CDXLScalarFieldSelect(CMemoryPool *mp, IMDId *field_type, IMDId *field_collation,
                                             INT type_modifier, SINT field_number)
    : CDXLScalar(mp),
      m_dxl_field_type(field_type),
      m_dxl_field_collation(field_collation),
      m_dxl_type_modifier(type_modifier),
      m_dxl_field_number(field_number) {
  GPOS_ASSERT(m_dxl_field_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::~CDXLScalarFieldSelect
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarFieldSelect::~CDXLScalarFieldSelect() {
  m_dxl_field_type->Release();
  m_dxl_field_collation->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarFieldSelect::GetDXLOperator() const {
  return EdxlopScalarFieldSelect;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::GetDXLFieldType
//
//	@doc:
//		Returns mdid of the field
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarFieldSelect::GetDXLFieldType() const {
  return m_dxl_field_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::GetDXLFieldCollation
//
//	@doc:
//		Returns collation mdid of the field
//
//---------------------------------------------------------------------------
IMDId *CDXLScalarFieldSelect::GetDXLFieldCollation() const {
  return m_dxl_field_collation;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::GetDXLFieldTypeModifier
//
//	@doc:
//		Returns output type mode
//
//---------------------------------------------------------------------------
INT CDXLScalarFieldSelect::GetDXLTypeModifier() const {
  return m_dxl_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::GetDXLFieldNumber()
//
//	@doc:
//		Returns attribute number of the field to extract
//
//---------------------------------------------------------------------------
SINT CDXLScalarFieldSelect::GetDXLFieldNumber() const {
  return m_dxl_field_number;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarFieldSelect::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarFieldSelect);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFieldSelect::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void CDXLScalarFieldSelect::AssertValid(const CDXLNode *dxlnode, BOOL validate_children) const {
  const ULONG arity = dxlnode->Arity();
  for (ULONG ul = 0; ul < arity; ++ul) {
    CDXLNode *child_dxlnode = (*dxlnode)[ul];
    GPOS_ASSERT(EdxloptypeScalar == child_dxlnode->GetOperator()->GetDXLOperatorType());

    if (validate_children) {
      child_dxlnode->GetOperator()->AssertValid(child_dxlnode, validate_children);
    }
  }
}
#endif  // GPOS_DEBUG

// EOF
