//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware Inc.
//
//	@filename:
//		CDXLExtStats.cpp
//
//	@doc:
//		Implementation of the class for representing extended stats in DXL
//---------------------------------------------------------------------------

#include "naucrates/md/CDXLExtStats.h"

#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

CDXLExtStats::CDXLExtStats(CMemoryPool *mp, IMDId *rel_stats_mdid, CMDName *mdname,
                           CMDDependencyArray *extstats_dependency_array, CMDNDistinctArray *ndistinct_array)
    : m_mp(mp),
      m_rel_stats_mdid(rel_stats_mdid),
      m_mdname(mdname),
      m_dependency_array(extstats_dependency_array),
      m_ndistinct_array(ndistinct_array) {
  GPOS_ASSERT(rel_stats_mdid->IsValid());
}

CDXLExtStats::~CDXLExtStats() {
  GPOS_DELETE(m_mdname);
  if (nullptr != m_dxl_str) {
    GPOS_DELETE(m_dxl_str);
  }
  m_rel_stats_mdid->Release();
  m_dependency_array->Release();
  m_ndistinct_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLExtStats::MDId
//
//	@doc:
//		Returns the metadata id of this extended stats object
//
//---------------------------------------------------------------------------
IMDId *CDXLExtStats::MDId() const {
  return m_rel_stats_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLExtStats::Mdname
//
//	@doc:
//		Returns the name of this relation
//
//---------------------------------------------------------------------------
CMDName CDXLExtStats::Mdname() const {
  return *m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLExtStats::CreateDXLDummyExtStats
//
//	@doc:
//		Dummy extended stats
//
//---------------------------------------------------------------------------
CDXLExtStats *CDXLExtStats::CreateDXLDummyExtStats(CMemoryPool *mp, IMDId *mdid) {
  CAutoP<CWStringDynamic> str;
  str = GPOS_NEW(mp) CWStringDynamic(mp, mdid->GetBuffer());
  CAutoP<CMDName> mdname;
  mdname = GPOS_NEW(mp) CMDName(mp, str.Value());
  CAutoRef<CDXLExtStats> ext_stats_dxl;

  CMDDependencyArray *extstats_dependency_array = GPOS_NEW(mp) CMDDependencyArray(mp);

  CMDNDistinctArray *extstats_ndistinct_array = GPOS_NEW(mp) CMDNDistinctArray(mp);

  ext_stats_dxl =
      GPOS_NEW(mp) CDXLExtStats(mp, mdid, mdname.Value(), extstats_dependency_array, extstats_ndistinct_array);
  mdname.Reset();
  return ext_stats_dxl.Reset();
}

// EOF
