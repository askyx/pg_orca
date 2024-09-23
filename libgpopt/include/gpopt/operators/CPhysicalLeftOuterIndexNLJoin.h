//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	Left outer index nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftOuterIndexNLJoin_H
#define GPOPT_CPhysicalLeftOuterIndexNLJoin_H

#include "gpopt/operators/CPhysicalLeftOuterNLJoin.h"
#include "gpos/base.h"

namespace gpopt {
class CPhysicalLeftOuterIndexNLJoin : public CPhysicalLeftOuterNLJoin {
 private:
  // columns from outer child used for index lookup in inner child
  CColRefArray *m_pdrgpcrOuterRefs;

  // a copy of the original join predicate that has been pushed down to the inner side
  CExpression *m_origJoinPred;

 public:
  CPhysicalLeftOuterIndexNLJoin(const CPhysicalLeftOuterIndexNLJoin &) = delete;

  // ctor
  CPhysicalLeftOuterIndexNLJoin(CMemoryPool *mp, CColRefArray *colref_array, CExpression *origJoinPred);

  // dtor
  ~CPhysicalLeftOuterIndexNLJoin() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalLeftOuterIndexNLJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalLeftOuterIndexNLJoin"; }

  // match function
  bool Matches(COperator *pop) const override;

  // outer column references accessor
  CColRefArray *PdrgPcrOuterRefs() const { return m_pdrgpcrOuterRefs; }

  // execution order of children
  EChildExecOrder Eceo() const override {
    // we optimize inner (right) child first to be able to match child hashed distributions
    return EceoRightToLeft;
  }

  // conversion function
  static CPhysicalLeftOuterIndexNLJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(EopPhysicalLeftOuterIndexNLJoin == pop->Eopid());

    return dynamic_cast<CPhysicalLeftOuterIndexNLJoin *>(pop);
  }

  CExpression *OrigJoinPred() { return m_origJoinPred; }

};  // class CPhysicalLeftOuterIndexNLJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalLeftOuterIndexNLJoin_H

// EOF
