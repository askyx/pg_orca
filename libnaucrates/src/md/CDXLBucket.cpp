//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLBucket.cpp
//
//	@doc:
//		Implementation of the class for representing buckets in DXL column stats
//---------------------------------------------------------------------------

#include "naucrates/md/CDXLBucket.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::CDXLBucket
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLBucket::CDXLBucket(CDXLDatum *dxl_datum_lower, CDXLDatum *dxl_datum_upper, BOOL is_lower_closed,
                       BOOL is_upper_closed, CDouble frequency, CDouble distinct)
    : m_lower_bound_dxl_datum(dxl_datum_lower),
      m_upper_bound_dxl_datum(dxl_datum_upper),
      m_is_lower_closed(is_lower_closed),
      m_is_upper_closed(is_upper_closed),
      m_frequency(frequency),
      m_distinct(distinct) {
  GPOS_ASSERT(nullptr != dxl_datum_lower);
  GPOS_ASSERT(nullptr != dxl_datum_upper);
  GPOS_ASSERT(m_frequency >= 0.0 && m_frequency <= 1.0);
  GPOS_ASSERT(m_distinct >= 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::~CDXLBucket
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLBucket::~CDXLBucket() {
  m_lower_bound_dxl_datum->Release();
  m_upper_bound_dxl_datum->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::GetDXLDatumLower
//
//	@doc:
//		Returns the lower bound for the bucket
//
//---------------------------------------------------------------------------
const CDXLDatum *CDXLBucket::GetDXLDatumLower() const {
  return m_lower_bound_dxl_datum;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::GetDXLDatumUpper
//
//	@doc:
//		Returns the upper bound for the bucket
//
//---------------------------------------------------------------------------
const CDXLDatum *CDXLBucket::GetDXLDatumUpper() const {
  return m_upper_bound_dxl_datum;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::GetFrequency
//
//	@doc:
//		Returns the frequency for this bucket
//
//---------------------------------------------------------------------------
CDouble CDXLBucket::GetFrequency() const {
  return m_frequency;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::GetNumDistinct
//
//	@doc:
//		Returns the number of distinct in this bucket
//
//---------------------------------------------------------------------------
CDouble CDXLBucket::GetNumDistinct() const {
  return m_distinct;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::DebugPrint
//
//	@doc:
//		Debug print of the bucket object
//
//---------------------------------------------------------------------------
void CDXLBucket::DebugPrint(IOstream &  // os
) const {
  // TODO:  - Feb 13, 2012; implement
}

#endif  // GPOS_DEBUG

// EOF
