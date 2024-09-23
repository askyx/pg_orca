//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalLeftSemiNLJoin.h
//
//	@doc:
//		Left semi nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftSemiNLJoin_H
#define GPOPT_CPhysicalLeftSemiNLJoin_H

#include "gpopt/operators/CPhysicalNLJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalLeftSemiNLJoin
//
//	@doc:
//		Left semi nested-loops join operator
//
//---------------------------------------------------------------------------
class CPhysicalLeftSemiNLJoin : public CPhysicalNLJoin {
 private:
 public:
  CPhysicalLeftSemiNLJoin(const CPhysicalLeftSemiNLJoin &) = delete;

  // ctor
  explicit CPhysicalLeftSemiNLJoin(CMemoryPool *mp);

  // dtor
  ~CPhysicalLeftSemiNLJoin() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalLeftSemiNLJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalLeftSemiNLJoin"; }

  // check if required columns are included in output columns
  bool FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t ulOptReq) const override;

  // conversion function
  static CPhysicalLeftSemiNLJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(EopPhysicalLeftSemiNLJoin == pop->Eopid());

    return dynamic_cast<CPhysicalLeftSemiNLJoin *>(pop);
  }

};  // class CPhysicalLeftSemiNLJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalLeftSemiNLJoin_H

// EOF
