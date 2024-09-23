//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#include "gpopt/operators/CPhysicalParallelUnionAll.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt {
CPhysicalParallelUnionAll::CPhysicalParallelUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
                                                     CColRef2dArray *pdrgpdrgpcrInput)
    : CPhysicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput) {
  // ParallelUnionAll creates two distribution requests to enforce distribution of its children:
  // (1) (StrictHashed, StrictHashed, ...): used to force redistribute motions that mirror the
  //     output columns
  // (2) (HashedNoOp, HashedNoOp, ...): used to force redistribution motions that mirror the
  //     underlying distribution of each relational child

  SetDistrRequests(2);
}

COperator::EOperatorId CPhysicalParallelUnionAll::Eopid() const {
  return EopPhysicalParallelUnionAll;
}

const char *CPhysicalParallelUnionAll::SzId() const {
  return "CPhysicalParallelUnionAll";
}

CPhysicalParallelUnionAll::~CPhysicalParallelUnionAll() {}
}  // namespace gpopt
