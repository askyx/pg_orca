//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of CTE consumer operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalCTEConsumer.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::CPhysicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalCTEConsumer::CPhysicalCTEConsumer(CMemoryPool *mp, uint32_t id, CColRefArray *colref_array,
                                           UlongToColRefMap *colref_mapping)
    : CPhysical(mp), m_id(id), m_pdrgpcr(colref_array), m_phmulcr(colref_mapping) {
  GPOS_ASSERT(nullptr != colref_array);
  GPOS_ASSERT(nullptr != colref_mapping);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::~CPhysicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalCTEConsumer::~CPhysicalCTEConsumer() {
  m_pdrgpcr->Release();
  m_phmulcr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalCTEConsumer::PcrsRequired(CMemoryPool *,        // mp,
                                               CExpressionHandle &,  // exprhdl,
                                               CColRefSet *,         // pcrsRequired,
                                               uint32_t,             // child_index,
                                               CDrvdPropArray *,     // pdrgpdpCtxt
                                               uint32_t              // ulOptReq
) {
  GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalCTEConsumer::PosRequired(CMemoryPool *,        // mp,
                                              CExpressionHandle &,  // exprhdl,
                                              COrderSpec *,         // posRequired,
                                              uint32_t,             // child_index,
                                              CDrvdPropArray *,     // pdrgpdpCtxt
                                              uint32_t              // ulOptReq
) const {
  GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalCTEConsumer::PcteRequired(CMemoryPool *,        // mp,
                                            CExpressionHandle &,  // exprhdl,
                                            CCTEReq *,            // pcter,
                                            uint32_t,             // child_index,
                                            CDrvdPropArray *,     // pdrgpdpCtxt,
                                            uint32_t              // ulOptReq
) const {
  GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalCTEConsumer::PosDerive(CMemoryPool *,       // mp
                                            CExpressionHandle &  // exprhdl
) const {
  GPOS_ASSERT(!"Unexpected call to CTE consumer order property derivation");

  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
CCTEMap *CPhysicalCTEConsumer::PcmDerive(CMemoryPool *mp, CExpressionHandle &
#ifdef GPOS_DEBUG
                                                              exprhdl
#endif
) const {
  GPOS_ASSERT(0 == exprhdl.Arity());

  CCTEMap *pcmConsumer = GPOS_NEW(mp) CCTEMap(mp);
  pcmConsumer->Insert(m_id, CCTEMap::EctConsumer, nullptr /*pdpplan*/);

  return pcmConsumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalCTEConsumer::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                             uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);

  CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns();
  return pcrsOutput->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalCTEConsumer::EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const {
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
//		CPhysicalCTEConsumer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
bool CPhysicalCTEConsumer::Matches(COperator *pop) const {
  if (pop->Eopid() != Eopid()) {
    return false;
  }

  CPhysicalCTEConsumer *popCTEConsumer = CPhysicalCTEConsumer::PopConvert(pop);

  return m_id == popCTEConsumer->UlCTEId() && m_pdrgpcr->Equals(popCTEConsumer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
uint32_t CPhysicalCTEConsumer::HashValue() const {
  uint32_t ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
  ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalCTEConsumer::OsPrint(IOstream &os) const {
  os << SzId() << " (";
  os << m_id;
  os << "), Columns: [";
  CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
  os << "]";

  return os;
}

// EOF
