//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalCTEProducer.cpp
//
//	@doc:
//		Implementation of CTE producer operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalCTEProducer.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalSpool.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::CPhysicalCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalCTEProducer::CPhysicalCTEProducer(CMemoryPool *mp, uint32_t id, CColRefArray *colref_array)
    : CPhysical(mp), m_id(id), m_pdrgpcr(colref_array), m_pcrs(nullptr) {
  GPOS_ASSERT(nullptr != colref_array);
  m_pcrs = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::~CPhysicalCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalCTEProducer::~CPhysicalCTEProducer() {
  m_pdrgpcr->Release();
  m_pcrs->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalCTEProducer::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                               uint32_t child_index,
                                               CDrvdPropArray *,  // pdrgpdpCtxt
                                               uint32_t           // ulOptReq
) {
  GPOS_ASSERT(0 == child_index);
  GPOS_ASSERT(0 == pcrsRequired->Size());

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrs);
  pcrs->Union(pcrsRequired);
  CColRefSet *pcrsChildReqd = PcrsChildReqd(mp, exprhdl, pcrs, child_index, UINT32_MAX);

  GPOS_ASSERT(pcrsChildReqd->Size() == m_pdrgpcr->Size());
  pcrs->Release();

  return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalCTEProducer::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired,
                                              uint32_t child_index,
                                              CDrvdPropArray *,  // pdrgpdpCtxt
                                              uint32_t           // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalCTEProducer::PcteRequired(CMemoryPool *,        // mp,
                                            CExpressionHandle &,  // exprhdl,
                                            CCTEReq *pcter,
                                            uint32_t
#ifdef GPOS_DEBUG
                                                child_index
#endif
                                            ,
                                            CDrvdPropArray *,  // pdrgpdpCtxt,
                                            uint32_t           // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);
  return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalCTEProducer::PosDerive(CMemoryPool *,  // mp
                                            CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
CCTEMap *CPhysicalCTEProducer::PcmDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  GPOS_ASSERT(1 == exprhdl.Arity());

  CCTEMap *pcmChild = exprhdl.Pdpplan(0)->GetCostModel();

  CCTEMap *pcmProducer = GPOS_NEW(mp) CCTEMap(mp);
  // store plan properties of the child in producer's CTE map
  pcmProducer->Insert(m_id, CCTEMap::EctProducer, exprhdl.Pdpplan(0));

  CCTEMap *pcmCombined = CCTEMap::PcmCombine(mp, *pcmProducer, *pcmChild);
  pcmProducer->Release();

  return pcmCombined;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalCTEProducer::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                             uint32_t  // ulOptReq
) const {
  return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalCTEProducer::EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
  if (peo->FCompatible(pos)) {
    return CEnfdProp::EpetUnnecessary;
  }

  return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
bool CPhysicalCTEProducer::Matches(COperator *pop) const {
  if (pop->Eopid() != Eopid()) {
    return false;
  }

  CPhysicalCTEProducer *popCTEProducer = CPhysicalCTEProducer::PopConvert(pop);

  return m_id == popCTEProducer->UlCTEId() && m_pdrgpcr->Equals(popCTEProducer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
uint32_t CPhysicalCTEProducer::HashValue() const {
  uint32_t ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
  ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalCTEProducer::OsPrint(IOstream &os) const {
  os << SzId() << " (";
  os << m_id;
  os << "), Columns: [";
  CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
  os << "]";

  return os;
}

// EOF
