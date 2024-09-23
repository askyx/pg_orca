//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalCorrelatedLeftAntiSemiNLJoin.h
//
//	@doc:
//		Physical Left Anti Semi NLJ operator capturing correlated execution
//		of NOT EXISTS subqueries
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCorrelatedLeftAntiSemiNLJoin_H
#define GPOPT_CPhysicalCorrelatedLeftAntiSemiNLJoin_H

#include "gpopt/operators/CPhysicalLeftAntiSemiNLJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalCorrelatedLeftAntiSemiNLJoin
//
//	@doc:
//		Physical left anti semi NLJ operator capturing correlated execution of
//		NOT EXISTS subqueries
//
//---------------------------------------------------------------------------
class CPhysicalCorrelatedLeftAntiSemiNLJoin : public CPhysicalLeftAntiSemiNLJoin {
 private:
  // columns from inner child used in correlated execution
  CColRefArray *m_pdrgpcrInner;

  // origin subquery id
  EOperatorId m_eopidOriginSubq;

 public:
  CPhysicalCorrelatedLeftAntiSemiNLJoin(const CPhysicalCorrelatedLeftAntiSemiNLJoin &) = delete;

  // ctor
  CPhysicalCorrelatedLeftAntiSemiNLJoin(CMemoryPool *mp, CColRefArray *pdrgpcrInner, EOperatorId eopidOriginSubq)
      : CPhysicalLeftAntiSemiNLJoin(mp), m_pdrgpcrInner(pdrgpcrInner), m_eopidOriginSubq(eopidOriginSubq) {
    GPOS_ASSERT(nullptr != pdrgpcrInner);

    SetDistrRequests(UlDistrRequestsForCorrelatedJoin());
    GPOS_ASSERT(0 < UlDistrRequests());
  }

  // dtor
  ~CPhysicalCorrelatedLeftAntiSemiNLJoin() override { m_pdrgpcrInner->Release(); }

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalCorrelatedLeftAntiSemiNLJoin; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalCorrelatedLeftAntiSemiNLJoin"; }

  // match function
  bool Matches(COperator *pop) const override {
    if (pop->Eopid() == Eopid()) {
      return m_pdrgpcrInner->Equals(CPhysicalCorrelatedLeftAntiSemiNLJoin::PopConvert(pop)->PdrgPcrInner());
    }

    return false;
  }

  // return true if operator is a correlated NL Join
  bool FCorrelated() const override { return true; }

  // return required inner columns
  CColRefArray *PdrgPcrInner() const override { return m_pdrgpcrInner; }

  // print
  IOstream &OsPrint(IOstream &os) const override {
    os << this->SzId() << "(";
    (void)CUtils::OsPrintDrgPcr(os, m_pdrgpcrInner);
    os << ")";

    return os;
  }

  // origin subquery id
  EOperatorId EopidOriginSubq() const { return m_eopidOriginSubq; }

  // conversion function
  static CPhysicalCorrelatedLeftAntiSemiNLJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalCorrelatedLeftAntiSemiNLJoin == pop->Eopid());

    return dynamic_cast<CPhysicalCorrelatedLeftAntiSemiNLJoin *>(pop);
  }

};  // class CPhysicalCorrelatedLeftAntiSemiNLJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalCorrelatedLeftAntiSemiNLJoin_H

// EOF
