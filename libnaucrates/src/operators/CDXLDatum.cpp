//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatum.cpp
//
//	@doc:
//		Implementation of DXL datum with type information
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatum.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatum::CDXLDatum
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatum::CDXLDatum(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, bool is_null, uint32_t length)
    : m_mp(mp), m_mdid_type(mdid_type), m_type_modifier(type_modifier), m_is_null(is_null), m_length(length) {
  GPOS_ASSERT(m_mdid_type->IsValid());
}

int32_t CDXLDatum::TypeModifier() const {
  return m_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatum::IsNull
//
//	@doc:
//		Is the datum NULL
//
//---------------------------------------------------------------------------
bool CDXLDatum::IsNull() const {
  return m_is_null;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatum::Length
//
//	@doc:
//		Returns the size of the byte array
//
//---------------------------------------------------------------------------
uint32_t CDXLDatum::Length() const {
  return m_length;
}
