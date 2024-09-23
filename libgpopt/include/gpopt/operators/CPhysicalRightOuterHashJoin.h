//---------------------------------------------------------------------------
//	Greenplum Database
//  Copyright (c) 2020 VMware, Inc.
//
//	@filename:
//		CPhysicalRightOuterHashJoin.h
//
//	@doc:
//		Right outer hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalRightOuterHashJoin_H
#define GPOPT_CPhysicalRightOuterHashJoin_H

#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalRightOuterHashJoin
//
//	@doc:
//		Right outer hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalRightOuterHashJoin : public CPhysicalHashJoin {
 private:
 protected:
  // create optimization requests
  void CreateOptRequests() override;

 public:
  CPhysicalRightOuterHashJoin(const CPhysicalRightOuterHashJoin &) = delete;

  // ctor
  CPhysicalRightOuterHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                              CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
                              bool is_null_aware = true, CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CPhysicalRightOuterHashJoin() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalRightOuterHashJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalRightOuterHashJoin"; }

  // conversion function
  static CPhysicalRightOuterHashJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalRightOuterHashJoin == pop->Eopid());

    return dynamic_cast<CPhysicalRightOuterHashJoin *>(pop);
  }
  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  CPartitionPropagationSpec *PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                          CPartitionPropagationSpec *pppsRequired, uint32_t child_index,
                                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  CPartitionPropagationSpec *PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

};  // class CPhysicalRightOuterHashJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalRightOuterHashJoin_H

// EOF
