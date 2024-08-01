//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalFilter.cpp
//
//	@doc:
//		Implementation of filter operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalFilter.h"

#include "gpopt/base/CPartInfo.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::CPhysicalFilter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalFilter::CPhysicalFilter(CMemoryPool *mp) : CPhysical(mp) {
  // when Filter includes outer references, correlated execution has to be enforced,
  // in this case, we create two child optimization requests to guarantee correct evaluation of parameters
  // (1) Broadcast
  // (2) Singleton

  SetDistrRequests(2 /*ulDistrReqs*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::~CPhysicalFilter
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalFilter::~CPhysicalFilter() = default;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalFilter::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                          ULONG child_index,
                                          CDrvdPropArray *,  // pdrgpdpCtxt
                                          ULONG              // ulOptReq
) {
  GPOS_ASSERT(0 == child_index && "Required properties can only be computed on the relational child");

  return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, 1 /*ulScalarIndex*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalFilter::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired,
                                         ULONG child_index,
                                         CDrvdPropArray *,  // pdrgpdpCtxt
                                         ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *CPhysicalFilter::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                 CRewindabilitySpec *prsRequired, ULONG child_index,
                                                 CDrvdPropArray *,  // pdrgpdpCtxt
                                                 ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);
  if (prsRequired->IsOriginNLJoin()) {
    CRewindabilitySpec *prs = GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone, prsRequired->Emht());
    return prs;
  }
  return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalFilter::PcteRequired(CMemoryPool *,        // mp,
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
//		CPhysicalFilter::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalFilter::PosDerive(CMemoryPool *,  // mp
                                       CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *CPhysicalFilter::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  // In theory, CPhysicalFilter can support Mark Restore - we disable it
  // here for now similar to ExecSupportsMarkRestore().
  return PrsDerivePassThruOuter(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL CPhysicalFilter::Matches(COperator *pop) const {
  // filter doesn't contain any members as of now
  return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalFilter::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                        ULONG  // ulOptReq
) const {
  return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalFilter::EpetOrder(CExpressionHandle &,  // exprhdl
                                                         const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                             peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  // always force sort to be on top of filter
  return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalFilter::EpetRewindability(CExpressionHandle &exprhdl,
                                                                 const CEnfdRewindability *per) const {
  if (per->PrsRequired()->IsOriginNLJoin()) {
    return CEnfdProp::EpetRequired;
  }

  // get rewindability delivered by the Filter node
  CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
  if (per->FCompatible(prs)) {
    // required rewindability is already provided
    return CEnfdProp::EpetUnnecessary;
  }

  // always force spool to be on top of filter
  return CEnfdProp::EpetRequired;
}

// EOF
