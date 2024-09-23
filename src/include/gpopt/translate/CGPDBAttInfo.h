//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CGPDBAttInfo.h
//
//	@doc:
//		Class to uniquely identify a column in GPDB
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CGPDBAttInfo_H
#define GPDXL_CGPDBAttInfo_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/utils.h"
#include "naucrates/dxl/gpdb_types.h"

namespace gpdxl {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CGPDBAttInfo
//
//	@doc:
//		Class to uniquely identify a column in GPDB
//
//---------------------------------------------------------------------------
class CGPDBAttInfo : public CRefCount {
 private:
  // query level number
  uint32_t m_query_level;

  // varno in the rtable
  uint32_t m_varno;

  // attno
  int32_t m_attno;

 public:
  CGPDBAttInfo(const CGPDBAttInfo &) = delete;

  // ctor
  CGPDBAttInfo(uint32_t query_level, uint32_t var_no, int32_t attrnum)
      : m_query_level(query_level), m_varno(var_no), m_attno(attrnum) {}

  // d'tor
  ~CGPDBAttInfo() override = default;

  // accessor
  uint32_t GetQueryLevel() const { return m_query_level; }

  // accessor
  uint32_t GetVarNo() const { return m_varno; }

  // accessor
  int32_t GetAttNo() const { return m_attno; }

  // equality check
  bool Equals(const CGPDBAttInfo &gpdb_att_info) const {
    return m_query_level == gpdb_att_info.m_query_level && m_varno == gpdb_att_info.m_varno &&
           m_attno == gpdb_att_info.m_attno;
  }

  // hash value
  uint32_t HashValue() const {
    return gpos::CombineHashes(gpos::HashValue(&m_query_level),
                               gpos::CombineHashes(gpos::HashValue(&m_varno), gpos::HashValue(&m_attno)));
  }
};

// hash function
inline uint32_t HashGPDBAttInfo(const CGPDBAttInfo *gpdb_att_info) {
  GPOS_ASSERT(nullptr != gpdb_att_info);
  return gpdb_att_info->HashValue();
}

// equality function
inline bool EqualGPDBAttInfo(const CGPDBAttInfo *gpdb_att_info_a, const CGPDBAttInfo *gpdb_att_info_b) {
  GPOS_ASSERT(nullptr != gpdb_att_info_a && nullptr != gpdb_att_info_b);
  return gpdb_att_info_a->Equals(*gpdb_att_info_b);
}

}  // namespace gpdxl

#endif  // !GPDXL_CGPDBAttInfo_H

// EOF
