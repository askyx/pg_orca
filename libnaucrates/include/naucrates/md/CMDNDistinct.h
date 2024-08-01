//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware Inc.
//
//	@filename:
//		CMDNDistinct.h
//
//	@doc:
//		Class representing MD extended stats multivariate n-distinct.
//
//		The structure mirrors MVNDistinct in statistics.h
//---------------------------------------------------------------------------

#ifndef GPMD_CMDNDistinct_H
#define GPMD_CMDNDistinct_H

#include "gpos/base.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/common/CDouble.h"
#include "gpos/string/CWStringDynamic.h"

namespace gpmd {
using namespace gpos;
using namespace gpdxl;

class CMDNDistinct : public CRefCount {
 private:
  // memory pool
  CMemoryPool *m_mp;

  CDouble m_ndistnct_value;

  CBitSet *m_attrs;

 public:
  CMDNDistinct(CMemoryPool *mp, DOUBLE ndistinct_value, CBitSet *attrs)
      : m_mp(mp), m_ndistnct_value(ndistinct_value), m_attrs(attrs) {}

  ~CMDNDistinct() override { m_attrs->Release(); }

  CWStringDynamic *AttrsToStr() {
    CWStringDynamic *str = GPOS_NEW(m_mp) CWStringDynamic(m_mp);
    CBitSetIter bsiter(*m_attrs);

    ULONG count = m_attrs->Size();
    while (bsiter.Advance()) {
      if (--count > 0) {
        str->AppendFormat(GPOS_WSZ_LIT("%d,"), bsiter.Bit());
      } else {
        str->AppendFormat(GPOS_WSZ_LIT("%d"), bsiter.Bit());
      }
    }

    return str;
  }

  CBitSet *GetAttrs() const { return m_attrs; }

  CDouble GetNDistinct() const { return m_ndistnct_value; }
};

using CMDNDistinctArray = CDynamicPtrArray<CMDNDistinct, CleanupRelease>;
}  // namespace gpmd

#endif  // !GPMD_CMDNDistinct_H

// EOF
