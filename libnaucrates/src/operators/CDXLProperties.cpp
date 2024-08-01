//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLProperties.cpp
//
//	@doc:
//		Implementation of properties of DXL operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLProperties.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::CDXLProperties
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLProperties::CDXLProperties() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::~CDXLProperties
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLProperties::~CDXLProperties() {
  CRefCount::SafeRelease(m_dxl_stats_derived_relation);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::SetStats
//
//	@doc:
//		Set operator properties
//
//---------------------------------------------------------------------------
void CDXLProperties::SetStats(CDXLStatsDerivedRelation *dxl_stats_derived_relation) {
  // allow setting properties only once
  GPOS_ASSERT(nullptr == m_dxl_stats_derived_relation);
  GPOS_ASSERT(nullptr != dxl_stats_derived_relation);
  m_dxl_stats_derived_relation = dxl_stats_derived_relation;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::GetDxlStatsDrvdRelation
//
//	@doc:
//		Return operator's statistical information
//
//---------------------------------------------------------------------------
const CDXLStatsDerivedRelation *CDXLProperties::GetDxlStatsDrvdRelation() const {
  return m_dxl_stats_derived_relation;
}
