//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLDirectDispatchInfo.cpp
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

#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"

#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDirectDispatchInfo::CDXLDirectDispatchInfo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDirectDispatchInfo::CDXLDirectDispatchInfo(CDXLDatum2dArray *dispatch_identifer_datum_array,
                                               BOOL contains_raw_values)
    : m_dispatch_identifer_datum_array(dispatch_identifer_datum_array), m_contains_raw_values(contains_raw_values) {
  GPOS_ASSERT(nullptr != dispatch_identifer_datum_array);

#ifdef GPOS_DEBUG
  const ULONG length = dispatch_identifer_datum_array->Size();
  if (0 < length) {
    ULONG num_of_datums = ((*dispatch_identifer_datum_array)[0])->Size();
    for (ULONG idx = 1; idx < length; idx++) {
      GPOS_ASSERT(num_of_datums == ((*dispatch_identifer_datum_array)[idx])->Size());
    }
  }
#endif  // GPOS_DEBUG
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDirectDispatchInfo::~CDXLDirectDispatchInfo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLDirectDispatchInfo::~CDXLDirectDispatchInfo() {
  m_dispatch_identifer_datum_array->Release();
}
