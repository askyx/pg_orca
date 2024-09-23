//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Greenplum
//
//	@filename:
//		COptColInfo.h
//
//	@doc:
//		Class to uniquely identify a column in optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_COptColInfo_H
#define GPDXL_COptColInfo_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/utils.h"

namespace gpdxl {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		COptColInfo
//
//	@doc:
//		pair of column id and column name
//
//---------------------------------------------------------------------------
class COptColInfo : public CRefCount {
 private:
  // column id
  uint32_t m_colid;

  // column name
  CWStringBase *m_str;

 public:
  COptColInfo(const COptColInfo &) = delete;

  // ctor
  COptColInfo(uint32_t colid, CWStringBase *str) : m_colid(colid), m_str(str) { GPOS_ASSERT(m_str); }

  // dtor
  ~COptColInfo() override { GPOS_DELETE(m_str); }

  // accessors
  uint32_t GetColId() const { return m_colid; }

  CWStringBase *GetOptColName() const { return m_str; }

  // equality check
  bool Equals(const COptColInfo &optcolinfo) const {
    // don't need to check name as column id is unique
    return m_colid == optcolinfo.m_colid;
  }

  // hash value
  uint32_t HashValue() const { return gpos::HashValue(&m_colid); }
};

// hash function
inline uint32_t UlHashOptColInfo(const COptColInfo *opt_col_info) {
  GPOS_ASSERT(nullptr != opt_col_info);
  return opt_col_info->HashValue();
}

// equality function
inline bool FEqualOptColInfo(const COptColInfo *opt_col_infoA, const COptColInfo *opt_col_infoB) {
  GPOS_ASSERT(nullptr != opt_col_infoA && nullptr != opt_col_infoB);
  return opt_col_infoA->Equals(*opt_col_infoB);
}

}  // namespace gpdxl

#endif  // !GPDXL_COptColInfo_H

// EOF
