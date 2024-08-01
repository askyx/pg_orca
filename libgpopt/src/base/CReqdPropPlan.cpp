//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CReqdPropPlan.cpp
//
//	@doc:
//		Required plan properties;
//---------------------------------------------------------------------------

#include "gpopt/base/CReqdPropPlan.h"

#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CEnfdOrder.h"
#include "gpopt/base/CEnfdPartitionPropagation.h"
#include "gpopt/base/CEnfdRewindability.h"
#include "gpopt/base/CPartInfo.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpos/base.h"
#include "gpos/common/CPrintablePointer.h"
#include "gpos/error/CAutoTrace.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//     @function:
//             CReqdPropPlan::CReqdPropPlan
//
//     @doc:
//             Ctor
//
//---------------------------------------------------------------------------
CReqdPropPlan::CReqdPropPlan(CColRefSet *pcrs, CEnfdOrder *peo, CEnfdRewindability *per,
                             CEnfdPartitionPropagation *pepp, CCTEReq *pcter)
    : m_pcrs(pcrs), m_peo(peo), m_per(per), m_pepp(pepp), m_pcter(pcter) {
  GPOS_ASSERT(nullptr != pcrs);
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(nullptr != per);
  GPOS_ASSERT(nullptr != pepp);
  GPOS_ASSERT(nullptr != pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::~CReqdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdPropPlan::~CReqdPropPlan() {
  CRefCount::SafeRelease(m_pcrs);
  CRefCount::SafeRelease(m_peo);
  CRefCount::SafeRelease(m_per);
  CRefCount::SafeRelease(m_pepp);
  CRefCount::SafeRelease(m_pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCols
//
//	@doc:
//		Compute required columns
//
//---------------------------------------------------------------------------
void CReqdPropPlan::ComputeReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index,
                                    CDrvdPropArray *pdrgpdpCtxt) {
  GPOS_ASSERT(nullptr == m_pcrs);

  CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
  CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
  m_pcrs = popPhysical->PcrsRequired(mp, exprhdl, prppInput->PcrsRequired(), child_index, pdrgpdpCtxt, 0 /*ulOptReq*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCTEs
//
//	@doc:
//		Compute required CTEs
//
//---------------------------------------------------------------------------
void CReqdPropPlan::ComputeReqdCTEs(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index,
                                    CDrvdPropArray *pdrgpdpCtxt) {
  GPOS_ASSERT(nullptr == m_pcter);

  CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
  CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
  m_pcter = popPhysical->PcteRequired(mp, exprhdl, prppInput->Pcter(), child_index, pdrgpdpCtxt, 0 /*ulOptReq*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CReqdPropPlan::Compute(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index,
                            CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) {
  GPOS_CHECK_ABORT;

  CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
  CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
  ComputeReqdCols(mp, exprhdl, prpInput, child_index, pdrgpdpCtxt);
  ComputeReqdCTEs(mp, exprhdl, prpInput, child_index, pdrgpdpCtxt);

  ULONG ulOrderReq = 0;
  ULONG ulDistrReq = 0;
  ULONG ulRewindReq = 0;
  ULONG ulPartPropagateReq = 0;
  popPhysical->LookupRequest(ulOptReq, &ulOrderReq, &ulDistrReq, &ulRewindReq, &ulPartPropagateReq);

  m_peo = GPOS_NEW(mp) CEnfdOrder(
      popPhysical->PosRequired(mp, exprhdl, prppInput->Peo()->PosRequired(), child_index, pdrgpdpCtxt, ulOrderReq),
      popPhysical->Eom(prppInput, child_index, pdrgpdpCtxt, ulOrderReq));

  m_per = GPOS_NEW(mp) CEnfdRewindability(
      popPhysical->PrsRequired(mp, exprhdl, prppInput->Per()->PrsRequired(), child_index, pdrgpdpCtxt, ulRewindReq),
      popPhysical->Erm(prppInput, child_index, pdrgpdpCtxt, ulRewindReq));

  m_pepp =
      GPOS_NEW(mp) CEnfdPartitionPropagation(popPhysical->PppsRequired(mp, exprhdl, prppInput->Pepp()->PppsRequired(),
                                                                       child_index, pdrgpdpCtxt, ulPartPropagateReq),
                                             CEnfdPartitionPropagation::EppmSatisfy);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Pps
//
//	@doc:
//		Given a property spec type, return the corresponding property spec
//		member
//
//---------------------------------------------------------------------------
CPropSpec *CReqdPropPlan::Pps(ULONG ul) const {
  CPropSpec::EPropSpecType epst = (CPropSpec::EPropSpecType)ul;
  switch (epst) {
    case CPropSpec::EpstOrder:
      return m_peo->PosRequired();

    case CPropSpec::EpstRewindability:
      return m_per->PrsRequired();

    case CPropSpec::EpstPartPropagation:
      return m_pepp->PppsRequired();

    default:
      GPOS_ASSERT(!"Invalid property spec index");
  }

  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Check if expression attached to handle provides required columns
//		by all plan properties
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FProvidesReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl, ULONG ulOptReq) const {
  CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());

  // check if operator provides required columns
  if (!popPhysical->FProvidesReqdCols(exprhdl, m_pcrs, ulOptReq)) {
    return false;
  }

  CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns();

  // check if property spec members use columns from operator output
  BOOL fProvidesReqdCols = true;
  for (ULONG ul = 0; fProvidesReqdCols && ul < CPropSpec::EpstSentinel; ul++) {
    CPropSpec *pps = Pps(ul);
    if (nullptr == pps) {
      continue;
    }

    CColRefSet *pcrsUsed = pps->PcrsUsed(mp);
    fProvidesReqdCols = pcrsOutput->ContainsAll(pcrsUsed);
    pcrsUsed->Release();
  }

  return fProvidesReqdCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::Equals(const CReqdPropPlan *prpp) const {
  GPOS_ASSERT(nullptr != prpp);

  BOOL result = PcrsRequired()->Equals(prpp->PcrsRequired()) && Pcter()->Equals(prpp->Pcter()) &&
                Peo()->Matches(prpp->Peo()) && Per()->Matches(prpp->Per());

  if (result) {
    if (nullptr == Pepp() || nullptr == prpp->Pepp()) {
      result = (nullptr == Pepp() && nullptr == prpp->Pepp());
    } else {
      result = Pepp()->Matches(prpp->Pepp());
    }
  }

  return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::HashValue
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
ULONG
CReqdPropPlan::HashValue() const {
  GPOS_ASSERT(nullptr != m_pcrs);
  GPOS_ASSERT(nullptr != m_peo);
  GPOS_ASSERT(nullptr != m_per);
  GPOS_ASSERT(nullptr != m_pcter);

  ULONG ulHash = m_pcrs->HashValue();
  ulHash = gpos::CombineHashes(ulHash, m_peo->HashValue());
  ulHash = gpos::CombineHashes(ulHash, m_per->HashValue());
  ulHash = gpos::CombineHashes(ulHash, m_pcter->HashValue());

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FSatisfied
//
//	@doc:
//		Check if plan properties are satisfied by the given derived properties
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FSatisfied(const CDrvdPropRelational *pdprel, const CDrvdPropPlan *pdpplan) const {
  GPOS_ASSERT(nullptr != pdprel);
  GPOS_ASSERT(nullptr != pdpplan);
  GPOS_ASSERT(pdprel->IsComplete());

  // first, check satisfiability of relational properties
  if (!pdprel->FSatisfies(this)) {
    return false;
  }

  // second, check satisfiability of plan properties;
  // if max cardinality <= 1, then any order requirement is already satisfied;
  // we only need to check satisfiability of distribution and rewindability
  if (pdprel->GetMaxCard().Ull() <= 1) {
    GPOS_ASSERT(nullptr != pdpplan->Ppps());

    return pdpplan->Prs()->FSatisfies(this->Per()->PrsRequired()) &&
           pdpplan->Ppps()->FSatisfies(this->Pepp()->PppsRequired()) &&
           pdpplan->GetCostModel()->FSatisfies(this->Pcter());
  }

  // otherwise, check satisfiability of all plan properties
  return pdpplan->FSatisfies(this);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FCompatible
//
//	@doc:
//		Check if plan properties are compatible with the given derived properties
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FCompatible(CExpressionHandle &exprhdl, CPhysical *popPhysical, const CDrvdPropRelational *pdprel,
                                const CDrvdPropPlan *pdpplan) const {
  GPOS_ASSERT(nullptr != pdpplan);
  GPOS_ASSERT(nullptr != pdprel);

  // first, check satisfiability of relational properties, including required columns
  if (!pdprel->FSatisfies(this)) {
    return false;
  }

  return m_peo->FCompatible(pdpplan->Pos()) && m_per->FCompatible(pdpplan->Prs()) &&
         pdpplan->Ppps()->FSatisfies(m_pepp->PppsRequired()) && popPhysical->FProvidesReqdCTEs(exprhdl, m_pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppEmpty
//
//	@doc:
//		Generate empty required properties
//
//---------------------------------------------------------------------------
CReqdPropPlan *CReqdPropPlan::PrppEmpty(CMemoryPool *mp) {
  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
  COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
  CRewindabilitySpec *prs =
      GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone, CRewindabilitySpec::EmhtNoMotion);
  CPartitionPropagationSpec *pps = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
  CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
  CEnfdRewindability *per = GPOS_NEW(mp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
  CEnfdPartitionPropagation *pepp = GPOS_NEW(mp) CEnfdPartitionPropagation(pps, CEnfdPartitionPropagation::EppmSatisfy);
  CCTEReq *pcter = GPOS_NEW(mp) CCTEReq(mp);

  return GPOS_NEW(mp) CReqdPropPlan(pcrs, peo, per, pepp, pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &CReqdPropPlan::OsPrint(IOstream &os) const {
  if (GPOS_FTRACE(EopttracePrintRequiredColumns)) {
    os << "req cols: [";
    if (nullptr != m_pcrs) {
      os << (*m_pcrs);
    }
    os << "], ";
  }

  os << "req CTEs: [";
  if (nullptr != m_pcter) {
    os << (*m_pcter);
  }

  os << "], req order: [";
  if (nullptr != m_peo) {
    os << (*m_peo);
  }

  os << "], req rewind: [";
  if (nullptr != m_per) {
    os << "], req rewind: [" << (*m_per);
  }

  os << "], req partition propagation: [";
  if (nullptr != m_pepp) {
    os << GetPrintablePtr(m_pepp);
  }
  os << "]";

  return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::UlHashForCostBounding
//
//	@doc:
//		Hash function used for cost bounding
//
//---------------------------------------------------------------------------
ULONG
CReqdPropPlan::UlHashForCostBounding(const CReqdPropPlan *prpp) {
  GPOS_ASSERT(nullptr != prpp);

  ULONG ulHash = prpp->PcrsRequired()->HashValue();

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FEqualForCostBounding
//
//	@doc:
//		Equality function used for cost bounding
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FEqualForCostBounding(const CReqdPropPlan *prppFst, const CReqdPropPlan *prppSnd) {
  GPOS_ASSERT(nullptr != prppFst);
  GPOS_ASSERT(nullptr != prppSnd);

  return prppFst->PcrsRequired()->Equals(prppSnd->PcrsRequired());
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppRemap
//
//	@doc:
//		Map input required and derived plan properties into new required
//		plan properties for the CTE producer
//
//---------------------------------------------------------------------------
CReqdPropPlan *CReqdPropPlan::PrppRemapForCTE(CMemoryPool *mp, CReqdPropPlan *prppProducer,
                                              CDrvdPropPlan *pdpplanProducer, CDrvdPropPlan *pdpplanConsumer,
                                              UlongToColRefMap *colref_mapping) {
  GPOS_ASSERT(nullptr != colref_mapping);
  GPOS_ASSERT(nullptr != prppProducer);
  GPOS_ASSERT(nullptr != pdpplanProducer);
  GPOS_ASSERT(nullptr != pdpplanConsumer);

  // Remap derived sort order to a required sort order.
  COrderSpec *pos = pdpplanConsumer->Pos()->PosCopyWithRemappedColumns(mp, colref_mapping, false /*must_exist*/);
  CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, prppProducer->Peo()->Eom());

  // other properties are copied from input

  prppProducer->PcrsRequired()->AddRef();
  CColRefSet *pcrsRequired = prppProducer->PcrsRequired();

  prppProducer->Per()->AddRef();
  CEnfdRewindability *per = prppProducer->Per();

  prppProducer->Pepp()->AddRef();
  CEnfdPartitionPropagation *pepp = prppProducer->Pepp();

  prppProducer->Pcter()->AddRef();
  CCTEReq *pcter = prppProducer->Pcter();

  return GPOS_NEW(mp) CReqdPropPlan(pcrsRequired, peo, per, pepp, pcter);
}

// EOF
