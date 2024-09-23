//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftOuterHashJoin.h
//
//	@doc:
//		Left outer hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftOuterHashJoin_H
#define GPOPT_CPhysicalLeftOuterHashJoin_H

#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalLeftOuterHashJoin
//
//	@doc:
//		Left outer hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalLeftOuterHashJoin : public CPhysicalHashJoin {
 public:
  CPhysicalLeftOuterHashJoin(const CPhysicalLeftOuterHashJoin &) = delete;

  // ctor
  CPhysicalLeftOuterHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                             CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
                             bool is_null_aware = true, CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CPhysicalLeftOuterHashJoin() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalLeftOuterHashJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalLeftOuterHashJoin"; }

  // conversion function
  static CPhysicalLeftOuterHashJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalLeftOuterHashJoin == pop->Eopid());

    return dynamic_cast<CPhysicalLeftOuterHashJoin *>(pop);
  }

};  // class CPhysicalLeftOuterHashJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalLeftOuterHashJoin_H

// EOF
