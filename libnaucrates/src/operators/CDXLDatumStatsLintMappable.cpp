//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLDatumStatsLintMappable.cpp
//
//	@doc:
//		Implementation of DXL datum of types having LINT mapping
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"

#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumStatsLintMappable::CDXLDatumStatsLintMappable
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumStatsLintMappable::CDXLDatumStatsLintMappable(CMemoryPool *mp, IMDId *mdid_type, INT type_modifier,
                                                       BOOL is_null, BYTE *byte_array, ULONG length, LINT value)
    : CDXLDatumGeneric(mp, mdid_type, type_modifier, is_null, byte_array, length), m_val(value) {}
