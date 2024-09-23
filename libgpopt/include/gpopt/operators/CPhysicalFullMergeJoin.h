//	Greenplum Database
//	Copyright (C) 2019 VMware, Inc. or its affiliates.

#ifndef GPOPT_CPhysicalFullMergeJoin_H
#define GPOPT_CPhysicalFullMergeJoin_H

#include "gpopt/operators/CPhysicalJoin.h"
#include "gpos/base.h"

namespace gpopt {
class CPhysicalFullMergeJoin : public CPhysicalJoin {
 private:
  CExpressionArray *m_outer_merge_clauses;

  CExpressionArray *m_inner_merge_clauses;

 public:
  CPhysicalFullMergeJoin(const CPhysicalFullMergeJoin &) = delete;

  // ctor
  explicit CPhysicalFullMergeJoin(CMemoryPool *mp, CExpressionArray *outer_merge_clauses,
                                  CExpressionArray *inner_merge_clauses, IMdIdArray *hash_opfamilies,
                                  bool is_null_aware = true, CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CPhysicalFullMergeJoin() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalFullMergeJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalFullMergeJoin"; }

  // conversion function
  static CPhysicalFullMergeJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(EopPhysicalFullMergeJoin == pop->Eopid());

    return dynamic_cast<CPhysicalFullMergeJoin *>(pop);
  }

  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posInput, uint32_t child_index,
                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  // return order property enforcing type for this operator
  CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

};  // class CPhysicalFullMergeJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalFullMergeJoin_H

// EOF
