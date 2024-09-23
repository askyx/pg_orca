//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumOid.cpp
//
//	@doc:
//		Implementation of DXL datum of type oid
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumOid.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumOid::CDXLDatumOid
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumOid::CDXLDatumOid(CMemoryPool *mp, IMDId *mdid_type, bool is_null, OID oid_val)
    : CDXLDatum(mp, mdid_type, default_type_modifier, is_null, 4 /*length*/), m_oid_val(oid_val) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumOid::OidValue
//
//	@doc:
//		Return the oid value
//
//---------------------------------------------------------------------------
OID CDXLDatumOid::OidValue() const {
  return m_oid_val;
}
