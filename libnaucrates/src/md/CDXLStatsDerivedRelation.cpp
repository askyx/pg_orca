//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLStatsDerivedRelation.cpp
//
//	@doc:
//		Implementation of the class for representing DXL derived relation statistics
//---------------------------------------------------------------------------

#include "naucrates/md/CDXLStatsDerivedRelation.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::CDXLStatsDerivedRelation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLStatsDerivedRelation::CDXLStatsDerivedRelation(CDouble rows, BOOL is_empty,
                                                   CDXLStatsDerivedColumnArray *dxl_stats_derived_col_array)
    : m_rows(rows), m_empty(is_empty), m_dxl_stats_derived_col_array(dxl_stats_derived_col_array) {
  GPOS_ASSERT(nullptr != dxl_stats_derived_col_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::~CDXLStatsDerivedRelation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLStatsDerivedRelation::~CDXLStatsDerivedRelation() {
  m_dxl_stats_derived_col_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::GetDXLStatsDerivedColArray
//
//	@doc:
//		Returns the array of derived columns stats
//
//---------------------------------------------------------------------------
const CDXLStatsDerivedColumnArray *CDXLStatsDerivedRelation::GetDXLStatsDerivedColArray() const {
  return m_dxl_stats_derived_col_array;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::DebugPrint
//
//	@doc:
//		Debug print of the bucket object
//
//---------------------------------------------------------------------------
void CDXLStatsDerivedRelation::DebugPrint(IOstream &os) const {
  os << "Rows: " << Rows() << std::endl;

  os << "Empty: " << IsEmpty() << std::endl;
}

#endif  // GPOS_DEBUG

// EOF
