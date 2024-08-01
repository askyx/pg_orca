//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumBool.cpp
//
//	@doc:
//		Implementation of DXL datum of type boolean
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumBool.h"

#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumBool::CDXLDatumBool
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumBool::CDXLDatumBool(CMemoryPool *mp, IMDId *mdid_type, BOOL is_null, BOOL value)
    : CDXLDatum(mp, mdid_type, default_type_modifier, is_null, 1 /*length*/), m_value(value) {}
