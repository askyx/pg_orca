//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CPhysicalLeftAntiSemiHashJoinNotIn.h
//
//	@doc:
//		Left anti semi hash join operator with NotIn semantics
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftAntiSemiHashJoinNotIn_H
#define GPOPT_CPhysicalLeftAntiSemiHashJoinNotIn_H

#include "gpopt/operators/CPhysicalLeftAntiSemiHashJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalLeftAntiSemiHashJoinNotIn
//
//	@doc:
//		Left anti semi hash join operator with NotIn semantics
//
//---------------------------------------------------------------------------
class CPhysicalLeftAntiSemiHashJoinNotIn : public CPhysicalLeftAntiSemiHashJoin {
 private:
 public:
  CPhysicalLeftAntiSemiHashJoinNotIn(const CPhysicalLeftAntiSemiHashJoinNotIn &) = delete;

  // ctor
  CPhysicalLeftAntiSemiHashJoinNotIn(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                                     CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
                                     BOOL is_null_aware = true, CXform::EXformId origin_xform = CXform::ExfSentinel);

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalLeftAntiSemiHashJoinNotIn; }

  // return a string for operator name
  const CHAR *SzId() const override { return "CPhysicalLeftAntiSemiHashJoinNotIn"; }

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // conversion function
  static CPhysicalLeftAntiSemiHashJoinNotIn *PopConvert(COperator *pop) {
    GPOS_ASSERT(EopPhysicalLeftAntiSemiHashJoinNotIn == pop->Eopid());

    return dynamic_cast<CPhysicalLeftAntiSemiHashJoinNotIn *>(pop);
  }

};  // class CPhysicalLeftAntiSemiHashJoinNotIn

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalLeftAntiSemiHashJoinNotIn_H

// EOF
