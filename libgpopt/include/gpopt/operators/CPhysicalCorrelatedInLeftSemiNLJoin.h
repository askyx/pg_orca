//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalCorrelatedInLeftSemiNLJoin.h
//
//	@doc:
//		Physical Left Semi NLJ operator capturing correlated execution
//		with IN/ANY semantics
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCorrelatedInLeftSemiNLJoin_H
#define GPOPT_CPhysicalCorrelatedInLeftSemiNLJoin_H

#include "gpopt/operators/CPhysicalLeftSemiNLJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalCorrelatedInLeftSemiNLJoin
//
//	@doc:
//		Physical left semi NLJ operator capturing correlated execution with
//		ANY/IN semantics
//
//---------------------------------------------------------------------------
class CPhysicalCorrelatedInLeftSemiNLJoin : public CPhysicalLeftSemiNLJoin {
 private:
  // columns from inner child used in correlated execution
  CColRefArray *m_pdrgpcrInner;

  // origin subquery id
  EOperatorId m_eopidOriginSubq;

 public:
  CPhysicalCorrelatedInLeftSemiNLJoin(const CPhysicalCorrelatedInLeftSemiNLJoin &) = delete;

  // ctor
  CPhysicalCorrelatedInLeftSemiNLJoin(CMemoryPool *mp, CColRefArray *pdrgpcrInner, EOperatorId eopidOriginSubq)
      : CPhysicalLeftSemiNLJoin(mp), m_pdrgpcrInner(pdrgpcrInner), m_eopidOriginSubq(eopidOriginSubq) {
    GPOS_ASSERT(nullptr != pdrgpcrInner);

    SetDistrRequests(UlDistrRequestsForCorrelatedJoin());
    GPOS_ASSERT(0 < UlDistrRequests());
  }

  // dtor
  ~CPhysicalCorrelatedInLeftSemiNLJoin() override { m_pdrgpcrInner->Release(); }

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalCorrelatedInLeftSemiNLJoin; }

  // return a string for operator name
  const CHAR *SzId() const override { return "CPhysicalCorrelatedInLeftSemiNLJoin"; }

  // match function
  BOOL Matches(COperator *pop) const override {
    if (pop->Eopid() == Eopid()) {
      return m_pdrgpcrInner->Equals(CPhysicalCorrelatedInLeftSemiNLJoin::PopConvert(pop)->PdrgPcrInner());
    }

    return false;
  }

  // conversion function
  static CPhysicalCorrelatedInLeftSemiNLJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalCorrelatedInLeftSemiNLJoin == pop->Eopid());

    return dynamic_cast<CPhysicalCorrelatedInLeftSemiNLJoin *>(pop);
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

};  // class CPhysicalCorrelatedInLeftSemiNLJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalCorrelatedInLeftSemiNLJoin_H

// EOF
