//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CColConstraintsHashMapper_H
#define GPOPT_CColConstraintsHashMapper_H

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/IColConstraintsMapper.h"
#include "gpos/memory/CMemoryPool.h"

namespace gpopt {
class CColConstraintsHashMapper : public IColConstraintsMapper {
 public:
  CColConstraintsHashMapper(CMemoryPool *mp, CConstraintArray *pdrgPcnstr);

  CConstraintArray *PdrgPcnstrLookup(CColRef *colref) override;
  ~CColConstraintsHashMapper() override;

 private:
  ColRefToConstraintArrayMap *m_phmColConstr;
};
}  // namespace gpopt

#endif  // GPOPT_CColConstraintsHashMapper_H
