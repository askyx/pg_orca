//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLDatumInt2.cpp
//
//	@doc:
//		Implementation of DXL datum of type short integer
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumInt2.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt2::CDXLDatumInt2
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumInt2::CDXLDatumInt2(CMemoryPool *mp, IMDId *mdid_type, bool is_null, int16_t val)
    : CDXLDatum(mp, mdid_type, default_type_modifier, is_null, 2 /*length*/), m_val(val) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt2::Value
//
//	@doc:
//		Return the short integer value
//
//---------------------------------------------------------------------------
int16_t CDXLDatumInt2::Value() const {
  return m_val;
}
