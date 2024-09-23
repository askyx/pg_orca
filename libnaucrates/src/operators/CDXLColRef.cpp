//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColRef.cpp
//
//	@doc:
//		Implementation of DXL column references
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLColRef.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::CDXLColRef
//
//	@doc:
//		Constructs a column reference
//
//---------------------------------------------------------------------------
CDXLColRef::CDXLColRef(CMDName *mdname, uint32_t id, IMDId *mdid_type, int32_t type_modifier)
    : m_mdname(mdname), m_id(id), m_mdid_type(mdid_type), m_iTypeModifer(type_modifier) {
  GPOS_ASSERT(m_mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::~CDXLColRef
//
//	@doc:
//		Desctructor
//
//---------------------------------------------------------------------------
CDXLColRef::~CDXLColRef() {
  GPOS_DELETE(m_mdname);
  m_mdid_type->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::MdName
//
//	@doc:
//		Returns column's name
//
//---------------------------------------------------------------------------
const CMDName *CDXLColRef::MdName() const {
  return m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::MdidType
//
//	@doc:
//		Returns column's type md id
//
//---------------------------------------------------------------------------
IMDId *CDXLColRef::MdidType() const {
  return m_mdid_type;
}

int32_t CDXLColRef::TypeModifier() const {
  return m_iTypeModifer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::Id
//
//	@doc:
//		Returns column's id
//
//---------------------------------------------------------------------------
uint32_t CDXLColRef::Id() const {
  return m_id;
}

// EOF
