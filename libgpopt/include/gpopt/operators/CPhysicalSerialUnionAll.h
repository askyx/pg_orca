//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CPhysicalSerialUnionAll_H
#define GPOPT_CPhysicalSerialUnionAll_H

#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declaration

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalSerialUnionAll
//
//	@doc:
//		Physical union all operator. Executes each child serially.
//
//---------------------------------------------------------------------------
class CPhysicalSerialUnionAll : public CPhysicalUnionAll {
 private:
 public:
  CPhysicalSerialUnionAll(const CPhysicalSerialUnionAll &) = delete;

  // ctor
  CPhysicalSerialUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput, CColRef2dArray *pdrgpdrgpcrInput);

  // dtor
  ~CPhysicalSerialUnionAll() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalSerialUnionAll; }

  const CHAR *SzId() const override { return "CPhysicalSerialUnionAll"; }

};  // class CPhysicalSerialUnionAll

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalSerialUnionAll_H

// EOF
