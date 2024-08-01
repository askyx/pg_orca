//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware Inc.
//
//	@filename:
//		CDXLExtStatsInfo.cpp
//
//	@doc:
//		Implementation of the class for representing extended stats metadata in
//		DXL
//---------------------------------------------------------------------------

#include "naucrates/md/CDXLExtStatsInfo.h"

#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

CDXLExtStatsInfo::CDXLExtStatsInfo(CMemoryPool *mp, IMDId *mdid, CMDName *mdname,
                                   CMDExtStatsInfoArray *extstats_info_array)
    : m_mp(mp), m_mdid(mdid), m_mdname(mdname), m_extstats_info_array(extstats_info_array) {
  GPOS_ASSERT(mdid->IsValid());
}

CDXLExtStatsInfo::~CDXLExtStatsInfo() {
  GPOS_DELETE(m_mdname);
  if (nullptr != m_dxl_str) {
    GPOS_DELETE(m_dxl_str);
  }
  m_mdid->Release();
  m_extstats_info_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLExtStatsInfo::MDId
//
//	@doc:
//		Returns the metadata id of this extended stats metadata object
//
//---------------------------------------------------------------------------
IMDId *CDXLExtStatsInfo::MDId() const {
  return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLExtStatsInfo::Mdname
//
//	@doc:
//		Returns the name of this statistic
//
//---------------------------------------------------------------------------
CMDName CDXLExtStatsInfo::Mdname() const {
  return *m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLExtStatsInfo::CreateDXLDummyExtStatsInfo
//
//	@doc:
//		Dummy extended stats info
//
//---------------------------------------------------------------------------
CDXLExtStatsInfo *CDXLExtStatsInfo::CreateDXLDummyExtStatsInfo(CMemoryPool *mp, IMDId *mdid) {
  CAutoP<CWStringDynamic> str;
  str = GPOS_NEW(mp) CWStringDynamic(mp, mdid->GetBuffer());
  CAutoP<CMDName> mdname;
  mdname = GPOS_NEW(mp) CMDName(mp, str.Value());
  CAutoRef<CDXLExtStatsInfo> ext_stats_info_dxl;

  CMDExtStatsInfoArray *extstats_info_array = GPOS_NEW(mp) CMDExtStatsInfoArray(mp);

  ext_stats_info_dxl = GPOS_NEW(mp) CDXLExtStatsInfo(mp, mdid, mdname.Value(), extstats_info_array);
  mdname.Reset();
  return ext_stats_info_dxl.Reset();
}

// EOF
