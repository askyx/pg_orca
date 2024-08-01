//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware Inc.
//
//	@filename:
//		CMDExtStatsInfo.cpp
//
//	@doc:
//		Implementation of the class for representing metadata cache ext stats
//---------------------------------------------------------------------------

#include "naucrates/md/CMDExtStatsInfo.h"

#include "gpos/common/CBitSetIter.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

using namespace gpdxl;
using namespace gpmd;

CWStringDynamic *CMDExtStatsInfo::KeysToStr(CMemoryPool *mp) {
  CWStringDynamic *str = GPOS_NEW(mp) CWStringDynamic(mp);

  ULONG length = m_keys->Size();
  ULONG ul = 0;

  CBitSetIter bsi(*m_keys);
  while (bsi.Advance()) {
    const ULONG attno = bsi.Bit();
    if (ul == length - 1) {
      // last element: do not print a comma
      str->AppendFormat(GPOS_WSZ_LIT("%d"), attno);
    } else {
      str->AppendFormat(GPOS_WSZ_LIT("%d%ls"), attno, CDXLTokens::GetDXLTokenStr(EdxltokenComma)->GetBuffer());
    }
    ul += 1;
  }

  return str;
}

CWStringDynamic *CMDExtStatsInfo::KindToStr(CMemoryPool *mp) {
  CWStringDynamic *str = GPOS_NEW(mp) CWStringDynamic(mp);

  switch (m_kind) {
    case Estattype::EstatDependencies: {
      str->AppendFormat(GPOS_WSZ_LIT("MVDependency"));
      break;
    }
    case Estattype::EstatNDistinct: {
      str->AppendFormat(GPOS_WSZ_LIT("MVNDistinct"));
      break;
    }
    case Estattype::EstatMCV: {
      str->AppendFormat(GPOS_WSZ_LIT("MVMCV"));
      break;
    }
    default: {
      // unexpected type
      GPOS_ASSERT(false && "Unknown extended stat type");
      break;
    }
  }

  return str;
}
