//---------------------------------------------------------------------------
//      Greenplum Database
//      Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//      @filename:
//              CUpperBoundNDVs.h
//
//      @doc:
//              Upper bound on the number of distinct values for a given set of columns
//---------------------------------------------------------------------------

#ifndef GPNAUCRATES_CUpperBoundNDVs_H
#define GPNAUCRATES_CUpperBoundNDVs_H

#include "gpopt/base/CColRefSet.h"
#include "gpos/base.h"

namespace gpnaucrates {
using namespace gpos;
using namespace gpmd;

// forward decl
class CUpperBoundNDVs;

// dynamic array of upper bound ndvs
using CUpperBoundNDVPtrArray = CDynamicPtrArray<CUpperBoundNDVs, CleanupDelete>;

//---------------------------------------------------------------------------
//      @class:
//              CUpperBoundNDVs
//
//      @doc:
//              Upper bound on the number of distinct values for a given set of columns
//
//---------------------------------------------------------------------------

class CUpperBoundNDVs {
 private:
  // set of column references
  CColRefSet *m_column_refset;

  // upper bound of ndvs
  CDouble m_upper_bound_ndv;

 public:
  CUpperBoundNDVs(const CUpperBoundNDVs &) = delete;

  // ctor
  CUpperBoundNDVs(CColRefSet *column_refset, CDouble upper_bound_ndv)
      : m_column_refset(column_refset), m_upper_bound_ndv(upper_bound_ndv) {
    GPOS_ASSERT(nullptr != m_column_refset);
  }

  // dtor
  ~CUpperBoundNDVs() { m_column_refset->Release(); }

  // return the upper bound of ndvs
  CDouble UpperBoundNDVs() const { return m_upper_bound_ndv; }

  // check if the column is present
  bool IsPresent(const CColRef *column_ref) const { return m_column_refset->FMember(column_ref); }

  // copy upper bound ndvs
  CUpperBoundNDVs *CopyUpperBoundNDVs(CMemoryPool *mp) const;
  CUpperBoundNDVs *CopyUpperBoundNDVs(CMemoryPool *mp, CDouble upper_bound_ndv) const;

  // copy upper bound ndvs with remapped column id; function will
  // return null if there is no mapping found for any of the columns
  CUpperBoundNDVs *CopyUpperBoundNDVWithRemap(CMemoryPool *mp, UlongToColRefMap *colid_to_colref_map) const;

  // print function
  IOstream &OsPrint(IOstream &os) const;
};
}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_CUpperBoundNDVs_H

// EOF
