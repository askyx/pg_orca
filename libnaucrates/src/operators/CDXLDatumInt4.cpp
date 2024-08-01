//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumInt4.cpp
//
//	@doc:
//		Implementation of DXL datum of type integer
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumInt4.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt4::CDXLDatumInt4
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumInt4::CDXLDatumInt4(CMemoryPool *mp, IMDId *mdid_type, BOOL is_null, INT val)
    : CDXLDatum(mp, mdid_type, default_type_modifier, is_null, 4 /*length*/), m_val(val) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt4::Value
//
//	@doc:
//		Return the integer value
//
//---------------------------------------------------------------------------
INT CDXLDatumInt4::Value() const {
  return m_val;
}
