//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CPhysicalParallelUnionAll_H
#define GPOPT_CPhysicalParallelUnionAll_H

#include "gpopt/operators/CPhysicalUnionAll.h"

namespace gpopt {
// Operator that implements logical union all, but creates a slice for each
// child relation to maximize concurrency.
// See gpopt::CPhysicalSerialUnionAll for its serial sibling.
class CPhysicalParallelUnionAll : public CPhysicalUnionAll {
 private:
 public:
  CPhysicalParallelUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput, CColRef2dArray *pdrgpdrgpcrInput);

  EOperatorId Eopid() const override;

  const CHAR *SzId() const override;

  ~CPhysicalParallelUnionAll() override;
};
}  // namespace gpopt

#endif  // GPOPT_CPhysicalParallelUnionAll_H
