//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware Inc.
//
//	@filename:
//		CMDDependency.h
//
//	@doc:
//		Class representing MD extended stats depencency.
//
//		The structure mirrors MVDependency in statistics.h. One noticable
//		difference is that that in CMDDependency the attributes are split into
//		"from" and "to" attributes.  Whereas MVDependency it is a single array
//		where the last indexed attnum is infered to be the "to" attribute.
//---------------------------------------------------------------------------

#ifndef GPMD_CMDDependency_H
#define GPMD_CMDDependency_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/string/CWStringDynamic.h"

namespace gpmd {
using namespace gpos;
using namespace gpdxl;

class CMDDependency : public CRefCount {
 private:
  // memory pool
  CMemoryPool *m_mp;

  CDouble m_degree;

  IntPtrArray *m_from_attno;

  int32_t m_to_attno;

 public:
  CMDDependency(CMemoryPool *mp, double degree, IntPtrArray *from_attno, int32_t to_attno)
      : m_mp(mp), m_degree(degree), m_from_attno(from_attno), m_to_attno(to_attno) {}

  ~CMDDependency() override { m_from_attno->Release(); }

  CWStringDynamic *FromAttnosToStr() {
    CWStringDynamic *str = GPOS_NEW(m_mp) CWStringDynamic(m_mp);

    uint32_t size = m_from_attno->Size();
    for (uint32_t i = 0; i < size - 1; i++) {
      str->AppendFormat(GPOS_WSZ_LIT("%d,"), *(*m_from_attno)[i]);
    }
    str->AppendFormat(GPOS_WSZ_LIT("%d"), *(*m_from_attno)[size - 1]);

    return str;
  }

  IntPtrArray *GetFromAttno() const { return m_from_attno; }

  int32_t GetToAttno() const { return m_to_attno; }

  CDouble GetDegree() const { return m_degree; }

  uint32_t GetNAttributes() const

  {
    return m_from_attno->Size() + 1;
  }
};

using CMDDependencyArray = CDynamicPtrArray<CMDDependency, CleanupRelease>;
}  // namespace gpmd

#endif  // !GPMD_CMDDependency_H

// EOF
