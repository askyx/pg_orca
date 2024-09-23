//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSequence.cpp
//
//	@doc:
//		Implementation of physical sequence operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalSequence.h"

#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::CPhysicalSequence
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSequence::CPhysicalSequence(CMemoryPool *mp) : CPhysical(mp), m_pcrsEmpty(nullptr) {
  // Sequence generates two distribution requests for its children:
  // (1) If incoming distribution from above is Singleton, pass it through
  //		to all children, otherwise request Non-Singleton (Non-Replicated)
  //		on all children
  //
  // (2)	Optimize first child with Any distribution requirement, and compute
  //		distribution request on other children based on derived distribution
  //		of first child:
  //			* If distribution of first child is a Singleton, request Singleton
  //				on the second child
  //			* If distribution of first child is Replicated, request Replicated
  //				on the second child
  //			* Otherwise, request Non-Singleton (Non-Replicated) on the second
  //				child

  SetDistrRequests(2);

  m_pcrsEmpty = GPOS_NEW(mp) CColRefSet(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::~CPhysicalSequence
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSequence::~CPhysicalSequence() {
  m_pcrsEmpty->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
bool CPhysicalSequence::Matches(COperator *pop) const {
  return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PcrsRequired
//
//	@doc:
//		Compute required output columns of n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalSequence::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                            uint32_t child_index,
                                            CDrvdPropArray *,  // pdrgpdpCtxt
                                            uint32_t           // ulOptReq
) {
  const uint32_t arity = exprhdl.Arity();
  if (child_index == arity - 1) {
    // request required columns from the last child of the sequence
    return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, UINT32_MAX);
  }

  m_pcrsEmpty->AddRef();
  GPOS_ASSERT(0 == m_pcrsEmpty->Size());

  return m_pcrsEmpty;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalSequence::PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter,
                                         uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt,
                                         uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcter);
  if (child_index < exprhdl.Arity() - 1) {
    return pcter->PcterAllOptional(mp);
  }

  // derived CTE maps from previous children
  CCTEMap *pcmCombined = PcmCombine(mp, pdrgpdpCtxt);

  // pass the remaining requirements that have not been resolved
  CCTEReq *pcterUnresolved = pcter->PcterUnresolvedSequence(mp, pcmCombined, pdrgpdpCtxt);
  pcmCombined->Release();

  return pcterUnresolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::FProvidesReqdCols
//
//	@doc:
//		Helper for checking if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalSequence::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                          uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);

  // last child must provide required columns
  uint32_t arity = exprhdl.Arity();
  GPOS_ASSERT(0 < arity);

  CColRefSet *pcrsChild = exprhdl.DeriveOutputColumns(arity - 1);

  return pcrsChild->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSequence::PosRequired(CMemoryPool *mp,
                                           CExpressionHandle &,  // exprhdl,
                                           COrderSpec *,         // posRequired,
                                           uint32_t,             // child_index,
                                           CDrvdPropArray *,     // pdrgpdpCtxt
                                           uint32_t              // ulOptReq
) const {
  // no order requirement on the children
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSequence::PosDerive(CMemoryPool *,  // mp,
                                         CExpressionHandle &exprhdl) const {
  // pass through sort order from last child
  const uint32_t arity = exprhdl.Arity();

  GPOS_ASSERT(1 <= arity);

  COrderSpec *pos = exprhdl.Pdpplan(arity - 1 /*child_index*/)->Pos();
  pos->AddRef();

  return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::EpetOrder
//
//	@doc:
//		Return the enforcing type for the order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalSequence::EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const {
  GPOS_ASSERT(nullptr != peo);

  // get order delivered by the sequence node
  COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();

  if (peo->FCompatible(pos)) {
    // required order will be established by the sequence operator
    return CEnfdProp::EpetUnnecessary;
  }

  // required distribution will be enforced on sequence's output
  return CEnfdProp::EpetRequired;
}
