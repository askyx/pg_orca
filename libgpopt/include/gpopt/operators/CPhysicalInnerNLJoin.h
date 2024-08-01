//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerNLJoin.h
//
//	@doc:
//		Inner nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalInnerNLJoin_H
#define GPOPT_CPhysicalInnerNLJoin_H

#include "gpopt/operators/CPhysicalNLJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalInnerNLJoin
//
//	@doc:
//		Inner nested-loops join operator
//
//---------------------------------------------------------------------------
class CPhysicalInnerNLJoin : public CPhysicalNLJoin {
 private:
 public:
  CPhysicalInnerNLJoin(const CPhysicalInnerNLJoin &) = delete;

  // ctor
  explicit CPhysicalInnerNLJoin(CMemoryPool *mp);

  // dtor
  ~CPhysicalInnerNLJoin() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalInnerNLJoin; }

  // return a string for operator name
  const CHAR *SzId() const override { return "CPhysicalInnerNLJoin"; }

  // conversion function
  static CPhysicalInnerNLJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(EopPhysicalInnerNLJoin == pop->Eopid());

    return dynamic_cast<CPhysicalInnerNLJoin *>(pop);
  }

};  // class CPhysicalInnerNLJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalInnerNLJoin_H

// EOF
