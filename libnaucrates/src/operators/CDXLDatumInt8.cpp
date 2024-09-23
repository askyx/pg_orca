//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumInt8.cpp
//
//	@doc:
//		Implementation of DXL datum of type long int
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumInt8.h"

#include "gpos/string/CWStringDynamic.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt8::CDXLDatumInt8
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumInt8::CDXLDatumInt8(CMemoryPool *mp, IMDId *mdid_type, bool is_null, int64_t val)
    : CDXLDatum(mp, mdid_type, default_type_modifier, is_null, 8 /*length*/), m_val(val) {
  if (is_null) {
    m_val = 0;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt8::Value
//
//	@doc:
//		Return the long int value
//
//---------------------------------------------------------------------------
int64_t CDXLDatumInt8::Value() const {
  return m_val;
}
