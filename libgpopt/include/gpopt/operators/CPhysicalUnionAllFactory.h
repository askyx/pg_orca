//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.
#ifndef GPOPT_CPhysicalUnionAllFactory_H
#define GPOPT_CPhysicalUnionAllFactory_H

#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpos/types.h"

namespace gpopt {
// Constructs a gpopt::CPhysicalUnionAll operator instance. Depending the
// parameter fParallel we construct either a CPhysicalParallelUnionAll or
// a CPhysicalSerialUnionAll instance.
class CPhysicalUnionAllFactory {
 private:
  CLogicalUnionAll *const m_popLogicalUnionAll;

 public:
  CPhysicalUnionAllFactory(CLogicalUnionAll *popLogicalUnionAll);

  CPhysicalUnionAll *PopPhysicalUnionAll(CMemoryPool *mp, bool fParallel);
};

}  // namespace gpopt

#endif  // GPOPT_CPhysicalUnionAllFactory_H
