//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysical.cpp
//
//	@doc:
//		Implementation of basic physical operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysical.h"

#include <cwchar>

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::CPhysical
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysical::CPhysical(CMemoryPool *mp)
    : COperator(mp),
      m_phmrcr(nullptr),
      m_pdrgpulpOptReqsExpanded(nullptr),
      m_ulTotalOptRequests(1)  // by default, an operator creates a single request for each property
{
  GPOS_ASSERT(nullptr != mp);

  for (uint32_t ul = 0; ul < GPOPT_PLAN_PROPS; ul++) {
    // by default, an operator creates a single request for each property
    m_rgulOptReqs[ul] = 1;
  }
  UpdateOptRequests(0 /*ulPropIndex*/, 1 /*ulOrderReqs*/);

  m_phmrcr = GPOS_NEW(mp) ReqdColsReqToColRefSetMap(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::UpdateOptRequests
//
//	@doc:
//		Update number of requests of a given property,
//		re-compute total number of optimization requests as the product
//		of all properties requests
//
//---------------------------------------------------------------------------
void CPhysical::UpdateOptRequests(uint32_t ulPropIndex, uint32_t ulRequests) {
  GPOS_ASSERT(ulPropIndex < GPOPT_PLAN_PROPS);

  // update property requests
  m_rgulOptReqs[ulPropIndex] = ulRequests;

  // compute new value of total requests
  uint32_t ulOptReqs = 1;
  for (uint32_t ul = 0; ul < GPOPT_PLAN_PROPS; ul++) {
    ulOptReqs = ulOptReqs * m_rgulOptReqs[ul];
  }

  // update total requests
  m_ulTotalOptRequests = ulOptReqs;

  // update expanded requests
  const uint32_t ulOrderRequests = UlOrderRequests();
  const uint32_t ulDistrRequests = UlDistrRequests();
  const uint32_t ulRewindRequests = UlRewindRequests();
  const uint32_t ulPartPropagateRequests = UlPartPropagateRequests();

  CRefCount::SafeRelease(m_pdrgpulpOptReqsExpanded);
  m_pdrgpulpOptReqsExpanded = nullptr;
  m_pdrgpulpOptReqsExpanded = GPOS_NEW(m_mp) UlongPtrArray(m_mp);
  for (uint32_t ulOrder = 0; ulOrder < ulOrderRequests; ulOrder++) {
    for (uint32_t ulDistr = 0; ulDistr < ulDistrRequests; ulDistr++) {
      for (uint32_t ulRewind = 0; ulRewind < ulRewindRequests; ulRewind++) {
        for (uint32_t ulPartPropagate = 0; ulPartPropagate < ulPartPropagateRequests; ulPartPropagate++) {
          uintptr_t *pulpRequest = GPOS_NEW_ARRAY(m_mp, uintptr_t, GPOPT_PLAN_PROPS);

          pulpRequest[0] = ulOrder;
          pulpRequest[1] = ulDistr;
          pulpRequest[2] = ulRewind;
          pulpRequest[3] = ulPartPropagate;

          m_pdrgpulpOptReqsExpanded->Append(pulpRequest);
        }
      }
    }
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::LookupReqNo
//
//	@doc:
//		Map input request number to order, distribution, rewindability and
//		partition propagation requests
//
//---------------------------------------------------------------------------
void CPhysical::LookupRequest(uint32_t ulReqNo,              // input: request number
                              uint32_t *pulOrderReq,         // output: order request number
                              uint32_t *pulDistrReq,         // output: distribution request number
                              uint32_t *pulRewindReq,        // output: rewindability request number
                              uint32_t *pulPartPropagateReq  // output: partition propagation request number
) {
  GPOS_ASSERT(nullptr != m_pdrgpulpOptReqsExpanded);
  GPOS_ASSERT(ulReqNo < m_pdrgpulpOptReqsExpanded->Size());
  GPOS_ASSERT(nullptr != pulOrderReq);
  GPOS_ASSERT(nullptr != pulDistrReq);
  GPOS_ASSERT(nullptr != pulRewindReq);
  GPOS_ASSERT(nullptr != pulPartPropagateReq);

  uintptr_t *pulpRequest = (*m_pdrgpulpOptReqsExpanded)[ulReqNo];
  *pulOrderReq = (uint32_t)pulpRequest[0];
  *pulDistrReq = (uint32_t)pulpRequest[1];
  *pulRewindReq = (uint32_t)pulpRequest[2];
  *pulPartPropagateReq = (uint32_t)pulpRequest[3];
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PdpCreate
//
//	@doc:
//		Create base container of derived properties
//
//---------------------------------------------------------------------------
CDrvdProp *CPhysical::PdpCreate(CMemoryPool *mp) const {
  return GPOS_NEW(mp) CDrvdPropPlan();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *CPhysical::PopCopyWithRemappedColumns(CMemoryPool *,       // mp,
                                                 UlongToColRefMap *,  // colref_mapping,
                                                 bool                 // must_exist
) {
  GPOS_ASSERT(!"Invalid call of CPhysical::PopCopyWithRemappedColumns");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PrpCreate
//
//	@doc:
//		Create base container of required properties
//
//---------------------------------------------------------------------------
CReqdProp *CPhysical::PrpCreate(CMemoryPool *mp) const {
  return GPOS_NEW(mp) CReqdPropPlan();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CReqdColsRequest::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
uint32_t CPhysical::CReqdColsRequest::HashValue(const CReqdColsRequest *prcr) {
  GPOS_ASSERT(nullptr != prcr);

  uint32_t ulHash = prcr->GetColRefSet()->HashValue();
  ulHash = CombineHashes(ulHash, prcr->UlChildIndex());
  ;

  return CombineHashes(ulHash, prcr->UlScalarChildIndex());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CReqdColsRequest::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool CPhysical::CReqdColsRequest::Equals(const CReqdColsRequest *prcrFst, const CReqdColsRequest *prcrSnd) {
  GPOS_ASSERT(nullptr != prcrFst);
  GPOS_ASSERT(nullptr != prcrSnd);

  return prcrFst->UlChildIndex() == prcrSnd->UlChildIndex() &&
         prcrFst->UlScalarChildIndex() == prcrSnd->UlScalarChildIndex() &&
         prcrFst->GetColRefSet()->Equals(prcrSnd->GetColRefSet());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PosPassThru
//
//	@doc:
//		Helper for a simple case of of computing child's required sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysical::PosPassThru(CMemoryPool *,        // mp
                                   CExpressionHandle &,  // exprhdl
                                   COrderSpec *posRequired,
                                   uint32_t  // child_index
) {
  posRequired->AddRef();

  return posRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PosDerivePassThruOuter
//
//	@doc:
//		Helper for common case of sort order derivation
//
//---------------------------------------------------------------------------
COrderSpec *CPhysical::PosDerivePassThruOuter(CExpressionHandle &exprhdl) {
  COrderSpec *pos = exprhdl.Pdpplan(0 /*child_index*/)->Pos();
  pos->AddRef();

  return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcrsChildReqd
//
//	@doc:
//		Helper for computing required output columns of the n-th child;
//		the caller must be an operator whose ulScalarIndex-th child is a
//		scalar
//
//---------------------------------------------------------------------------
CColRefSet *CPhysical::PcrsChildReqd(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                     uint32_t child_index, uint32_t ulScalarIndex) {
  pcrsRequired->AddRef();
  CReqdColsRequest *prcr = GPOS_NEW(mp) CReqdColsRequest(pcrsRequired, child_index, ulScalarIndex);
  CColRefSet *pcrs = nullptr;

  // lookup required columns map first
  pcrs = m_phmrcr->Find(prcr);
  if (nullptr != pcrs) {
    prcr->Release();
    pcrs->AddRef();
    return pcrs;
  }

  // request was not found in map -- we need to compute it
  pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);
  if (UINT32_MAX != ulScalarIndex) {
    // include used columns and exclude defined columns of scalar child
    pcrs->Union(exprhdl.DeriveUsedColumns(ulScalarIndex));
    pcrs->Exclude(exprhdl.DeriveDefinedColumns(ulScalarIndex));
  }

  // intersect computed column set with child's output columns
  pcrs->Intersection(exprhdl.DeriveOutputColumns(child_index));

  // insert request in map
  pcrs->AddRef();
  bool fSuccess GPOS_ASSERTS_ONLY = m_phmrcr->Insert(prcr, pcrs);
  GPOS_ASSERT(fSuccess);

  return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FUnaryProvidesReqdCols
//
//	@doc:
//		Helper for checking if output columns of a unary operator that defines
//		no new columns include the required columns
//
//---------------------------------------------------------------------------
bool CPhysical::FUnaryProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired) {
  GPOS_ASSERT(nullptr != pcrsRequired);

  CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns(0 /*child_index*/);

  return pcrsOutput->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcterPushThru
//
//	@doc:
//		Helper for pushing cte requirement to the child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysical::PcterPushThru(CCTEReq *pcter) {
  GPOS_ASSERT(nullptr != pcter);
  pcter->AddRef();
  return pcter;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcmCombine
//
//	@doc:
//		Combine the derived CTE maps of the first n children
//		of the given expression handle
//
//---------------------------------------------------------------------------
CCTEMap *CPhysical::PcmCombine(CMemoryPool *mp, CDrvdPropArray *pdrgpdpCtxt) {
  GPOS_ASSERT(nullptr != pdrgpdpCtxt);

  const uint32_t size = pdrgpdpCtxt->Size();
  CCTEMap *pcmCombined = GPOS_NEW(mp) CCTEMap(mp);
  for (uint32_t ul = 0; ul < size; ul++) {
    CCTEMap *pcmChild = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[ul])->GetCostModel();

    // get the remaining requirements that have not been met by child
    CCTEMap *pcm = CCTEMap::PcmCombine(mp, *pcmCombined, *pcmChild);
    pcmCombined->Release();
    pcmCombined = pcm;
  }

  return pcmCombined;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcterNAry
//
//	@doc:
//		Helper for computing cte requirement for the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysical::PcterNAry(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter, uint32_t child_index,
                              CDrvdPropArray *pdrgpdpCtxt) const {
  GPOS_ASSERT(nullptr != pcter);

  if (EceoLeftToRight == Eceo()) {
    uint32_t ulLastNonScalarChild = exprhdl.UlLastNonScalarChild();
    if (UINT32_MAX != ulLastNonScalarChild && child_index < ulLastNonScalarChild) {
      return pcter->PcterAllOptional(mp);
    }
  } else {
    GPOS_ASSERT(EceoRightToLeft == Eceo());

    uint32_t ulFirstNonScalarChild = exprhdl.UlFirstNonScalarChild();
    if (UINT32_MAX != ulFirstNonScalarChild && child_index > ulFirstNonScalarChild) {
      return pcter->PcterAllOptional(mp);
    }
  }

  CCTEMap *pcmCombined = PcmCombine(mp, pdrgpdpCtxt);

  // pass the remaining requirements that have not been resolved
  CCTEReq *pcterUnresolved = pcter->PcterUnresolved(mp, pcmCombined);
  pcmCombined->Release();

  return pcterUnresolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FCanPushPartReqToChild
//
//	@doc:
//		Check whether we can push a part table requirement to a given child, given
// 		the knowledge of where the part index id is defined
//
//---------------------------------------------------------------------------
bool CPhysical::FCanPushPartReqToChild(CBitSet *pbsPartConsumer, uint32_t child_index) {
  GPOS_ASSERT(nullptr != pbsPartConsumer);

  // if part index id comes from more that one child, we cannot push request to just one child
  if (1 < pbsPartConsumer->Size()) {
    return false;
  }

  // child where the part index is defined should be the same child being processed
  return (pbsPartConsumer->Get(child_index));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcmDerive
//
//	@doc:
//		Common case of combining cte maps of all logical children
//
//---------------------------------------------------------------------------
CCTEMap *CPhysical::PcmDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  GPOS_ASSERT(0 < exprhdl.Arity());

  CCTEMap *pcm = GPOS_NEW(mp) CCTEMap(mp);
  const uint32_t arity = exprhdl.Arity();
  for (uint32_t ul = 0; ul < arity; ul++) {
    if (!exprhdl.FScalarChild(ul)) {
      CCTEMap *pcmChild = exprhdl.Pdpplan(ul)->GetCostModel();
      GPOS_ASSERT(nullptr != pcmChild);

      CCTEMap *pcmCombined = CCTEMap::PcmCombine(mp, *pcm, *pcmChild);
      pcm->Release();
      pcm = pcmCombined;
    }
  }

  return pcm;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FProvidesReqdCTEs
//
//	@doc:
//		Check if required CTEs are included in derived CTE map
//
//---------------------------------------------------------------------------
bool CPhysical::FProvidesReqdCTEs(CExpressionHandle &exprhdl, const CCTEReq *pcter) const {
  CCTEMap *pcmDrvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->GetCostModel();
  GPOS_ASSERT(nullptr != pcmDrvd);
  return pcmDrvd->FSatisfies(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::GetSkew
//
//	@doc:
//		Helper to compute skew estimate based on given stats and
//		distribution spec
//
//---------------------------------------------------------------------------
CDouble CPhysical::GetSkew(IStatistics *stats) {
  CDouble dSkew = 1.0;
  (void)stats;

  return CDouble(dSkew);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FUnaryUsesDefinedColumns
//
//	@doc:
//		Return true if the given column set includes any of the columns defined
//		by the unary node, as given by the handle
//
//---------------------------------------------------------------------------
bool CPhysical::FUnaryUsesDefinedColumns(CColRefSet *pcrs, CExpressionHandle &exprhdl) {
  GPOS_ASSERT(nullptr != pcrs);
  GPOS_ASSERT(2 == exprhdl.Arity() && "Not a unary operator");

  if (0 == pcrs->Size()) {
    return false;
  }

  return !pcrs->IsDisjoint(exprhdl.DeriveDefinedColumns(1));
}

CEnfdOrder::EOrderMatching CPhysical::Eom(CReqdPropPlan *, uint32_t, CDrvdPropArray *, uint32_t) {
  // request satisfaction by default
  return CEnfdOrder::EomSatisfy;
}

CPartitionPropagationSpec *CPhysical::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                   CPartitionPropagationSpec *pppsRequired, uint32_t child_index,
                                                   CDrvdPropArray *, uint32_t) const {
  // pass through consumer<x> requests to the appropriate child.
  // do not pass through any propagator<x> requests
  CPartitionPropagationSpec *pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);

  CBitSet *allowed_scan_ids = GPOS_NEW(mp) CBitSet(mp);
  CPartInfo *part_info = exprhdl.DerivePartitionInfo(child_index);
  for (uint32_t ul = 0; ul < part_info->UlConsumers(); ++ul) {
    uint32_t scan_id = part_info->ScanId(ul);
    allowed_scan_ids->ExchangeSet(scan_id);
  }

  pps_result->InsertAllowedConsumers(pppsRequired, allowed_scan_ids);
  allowed_scan_ids->Release();

  return pps_result;
}

CEnfdProp::EPropEnforcingType CPhysical::EpetPartitionPropagation(CExpressionHandle &exprhdl,
                                                                  const CEnfdPartitionPropagation *pps_reqd) const {
  GPOS_ASSERT(nullptr != pps_reqd);

  CPartitionPropagationSpec *pps_drvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Ppps();
  if (pps_reqd->FCompatible(pps_drvd)) {
    // all requests are resolved
    return CEnfdProp::EpetUnnecessary;
  }

  return CEnfdProp::EpetRequired;
}

CPartitionPropagationSpec *CPhysical::PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  CPartitionPropagationSpec *pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);

  for (uint32_t ul = 0; ul < exprhdl.Arity(); ++ul) {
    if (exprhdl.FScalarChild(ul)) {
      continue;
    }
    CPartitionPropagationSpec *pps = exprhdl.Pdpplan(ul)->Ppps();
    pps_result->InsertAll(pps);
  }

  return pps_result;
}

// EOF
