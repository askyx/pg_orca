//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalCorrelatedNotInLeftAntiSemiNLJoin.h
//
//	@doc:
//		Physical Left Anti Semi NLJ operator capturing correlated execution
//		with NOT-IN/ALL semantics
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCorrelatedNotInLeftAntiSemiNLJoin_H
#define GPOPT_CPhysicalCorrelatedNotInLeftAntiSemiNLJoin_H

#include "gpopt/operators/CPhysicalLeftAntiSemiNLJoinNotIn.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalCorrelatedNotInLeftAntiSemiNLJoin
//
//	@doc:
//		Physical left semi NLJ operator capturing correlated execution with
//		ANY/IN semantics
//
//---------------------------------------------------------------------------
class CPhysicalCorrelatedNotInLeftAntiSemiNLJoin : public CPhysicalLeftAntiSemiNLJoinNotIn {
 private:
  // columns from inner child used in correlated execution
  CColRefArray *m_pdrgpcrInner;

  // origin subquery id
  EOperatorId m_eopidOriginSubq;

 public:
  CPhysicalCorrelatedNotInLeftAntiSemiNLJoin(const CPhysicalCorrelatedNotInLeftAntiSemiNLJoin &) = delete;

  // ctor
  CPhysicalCorrelatedNotInLeftAntiSemiNLJoin(CMemoryPool *mp, CColRefArray *pdrgpcrInner, EOperatorId eopidOriginSubq)
      : CPhysicalLeftAntiSemiNLJoinNotIn(mp), m_pdrgpcrInner(pdrgpcrInner), m_eopidOriginSubq(eopidOriginSubq) {
    GPOS_ASSERT(nullptr != pdrgpcrInner);

    SetDistrRequests(UlDistrRequestsForCorrelatedJoin());
    GPOS_ASSERT(0 < UlDistrRequests());
  }

  // dtor
  ~CPhysicalCorrelatedNotInLeftAntiSemiNLJoin() override { m_pdrgpcrInner->Release(); }

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin; }

  // return a string for operator name
  const CHAR *SzId() const override { return "CPhysicalCorrelatedNotInLeftAntiSemiNLJoin"; }

  // match function
  BOOL Matches(COperator *pop) const override {
    if (pop->Eopid() == Eopid()) {
      return m_pdrgpcrInner->Equals(CPhysicalCorrelatedNotInLeftAntiSemiNLJoin::PopConvert(pop)->PdrgPcrInner());
    }

    return false;
  }

  // compute required rewindability of the n-th child
  CRewindabilitySpec *PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CRewindabilitySpec *prsRequired,
                                  ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override {
    return PrsRequiredCorrelatedJoin(mp, exprhdl, prsRequired, child_index, pdrgpdpCtxt, ulOptReq);
  }

  // conversion function
  static CPhysicalCorrelatedNotInLeftAntiSemiNLJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin == pop->Eopid());

    return dynamic_cast<CPhysicalCorrelatedNotInLeftAntiSemiNLJoin *>(pop);
  }

  // return true if operator is a correlated NL Join
  BOOL FCorrelated() const override { return true; }

  // return required inner columns
  CColRefArray *PdrgPcrInner() const override { return m_pdrgpcrInner; }

  // origin subquery id
  EOperatorId EopidOriginSubq() const { return m_eopidOriginSubq; }

  // print
  IOstream &OsPrint(IOstream &os) const override {
    os << this->SzId() << "(";
    (void)CUtils::OsPrintDrgPcr(os, m_pdrgpcrInner);
    os << ")";

    return os;
  }

};  // class CPhysicalCorrelatedNotInLeftAntiSemiNLJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalCorrelatedNotInLeftAntiSemiNLJoin_H

// EOF
