//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftSemiHashJoin.h
//
//	@doc:
//		Left semi hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftSemiHashJoin_H
#define GPOPT_CPhysicalLeftSemiHashJoin_H

#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalLeftSemiHashJoin
//
//	@doc:
//		Left semi hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalLeftSemiHashJoin : public CPhysicalHashJoin {
 private:
 public:
  CPhysicalLeftSemiHashJoin(const CPhysicalLeftSemiHashJoin &) = delete;

  // ctor
  CPhysicalLeftSemiHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys, CExpressionArray *pdrgpexprInnerKeys,
                            IMdIdArray *hash_opfamilies, bool is_null_aware = true,
                            CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CPhysicalLeftSemiHashJoin() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalLeftSemiHashJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalLeftSemiHashJoin"; }

  CPartitionPropagationSpec *PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                          CPartitionPropagationSpec *pppsRequired, uint32_t child_index,
                                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  CPartitionPropagationSpec *PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

  // check if required columns are included in output columns
  bool FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t ulOptReq) const override;

  // conversion function
  static CPhysicalLeftSemiHashJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(EopPhysicalLeftSemiHashJoin == pop->Eopid());

    return dynamic_cast<CPhysicalLeftSemiHashJoin *>(pop);
  }

};  // class CPhysicalLeftSemiHashJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalLeftSemiHashJoin_H

// EOF
