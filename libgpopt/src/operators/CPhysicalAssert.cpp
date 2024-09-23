//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalAssert.cpp
//
//	@doc:
//		Implementation of assert operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalAssert.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::CPhysicalAssert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalAssert::CPhysicalAssert(CMemoryPool *mp, CException *pexc) : CPhysical(mp), m_pexc(pexc) {
  GPOS_ASSERT(nullptr != pexc);

  // when Assert includes outer references, correlated execution has to be enforced,
  // in this case, we create two optimization requests to guarantee correct evaluation of parameters
  // (1) Broadcast
  // (2) Singleton

  SetDistrRequests(2);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::~CPhysicalAssert
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalAssert::~CPhysicalAssert() {
  GPOS_DELETE(m_pexc);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalAssert::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                          ULONG child_index,
                                          CDrvdPropArray *,  // pdrgpdpCtxt
                                          ULONG              // ulOptReq
) {
  GPOS_ASSERT(0 == child_index && "Required properties can only be computed on the relational child");

  return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, 1 /*ulScalarIndex*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalAssert::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired,
                                         ULONG child_index,
                                         CDrvdPropArray *,  // pdrgpdpCtxt
                                         ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalAssert::PcteRequired(CMemoryPool *,        // mp,
                                       CExpressionHandle &,  // exprhdl,
                                       CCTEReq *pcter,
                                       ULONG
#ifdef GPOS_DEBUG
                                           child_index
#endif
                                       ,
                                       CDrvdPropArray *,  // pdrgpdpCtxt,
                                       ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);
  return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalAssert::PosDerive(CMemoryPool *,  // mp
                                       CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL CPhysicalAssert::Matches(COperator *pop) const {
  if (Eopid() != pop->Eopid()) {
    return false;
  }

  CPhysicalAssert *popAssert = CPhysicalAssert::PopConvert(pop);
  return CException::Equals(*(popAssert->Pexc()), *m_pexc);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalAssert::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                        ULONG  // ulOptReq
) const {
  return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalAssert::EpetOrder(CExpressionHandle &,  // exprhdl
                                                         const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                             peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  // always force sort to be on top of assert
  return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAssert::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalAssert::OsPrint(IOstream &os) const {
  if (m_fPattern) {
    return COperator::OsPrint(os);
  }

  os << SzId() << " (Error code: " << m_pexc->GetSQLState() << ")";
  return os;
}

// EOF
