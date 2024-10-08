//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CDatumSortedSet_H
#define GPOPT_CDatumSortedSet_H

#include "gpopt/base/IComparator.h"
#include "gpopt/operators/CExpression.h"
#include "gpos/memory/CMemoryPool.h"
#include "naucrates/base/IDatum.h"

namespace gpopt {
// A sorted and uniq'd array of pointers to datums
// It facilitates the construction of CConstraintInterval
class CDatumSortedSet : public IDatumArray {
 private:
  bool m_fIncludesNull;

 public:
  CDatumSortedSet(CMemoryPool *mp, CExpression *pexprArray, const IComparator *pcomp);

  bool FIncludesNull() const;
};
}  // namespace gpopt

#endif
