//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalInnerHashJoin.h
//
//	@doc:
//		Inner hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalInnerHashJoin_H
#define GPOPT_CPhysicalInnerHashJoin_H

#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalInnerHashJoin
//
//	@doc:
//		Inner hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalInnerHashJoin : public CPhysicalHashJoin {
 public:
  CPhysicalInnerHashJoin(const CPhysicalInnerHashJoin &) = delete;

  // ctor
  CPhysicalInnerHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys, CExpressionArray *pdrgpexprInnerKeys,
                         IMdIdArray *hash_opfamilies, bool is_null_aware = true,
                         CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CPhysicalInnerHashJoin() override;

  // ident accessors

  EOperatorId Eopid() const override { return EopPhysicalInnerHashJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalInnerHashJoin"; }

  // conversion function
  static CPhysicalInnerHashJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(EopPhysicalInnerHashJoin == pop->Eopid());

    return dynamic_cast<CPhysicalInnerHashJoin *>(pop);
  }

  CPartitionPropagationSpec *PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                          CPartitionPropagationSpec *pppsRequired, uint32_t child_index,
                                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  CPartitionPropagationSpec *PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
};  // class CPhysicalInnerHashJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalInnerHashJoin_H

// EOF
