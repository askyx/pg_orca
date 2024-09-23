//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSplit.cpp
//
//	@doc:
//		Implementation of physical split operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalSplit.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::CPhysicalSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSplit::CPhysicalSplit(CMemoryPool *mp, CColRefArray *pdrgpcrDelete, CColRefArray *pdrgpcrInsert,
                               CColRef *pcrCtid, CColRef *pcrSegmentId, CColRef *pcrAction)
    : CPhysical(mp),
      m_pdrgpcrDelete(pdrgpcrDelete),
      m_pdrgpcrInsert(pdrgpcrInsert),
      m_pcrCtid(pcrCtid),
      m_pcrSegmentId(pcrSegmentId),
      m_pcrAction(pcrAction),
      m_pcrsRequiredLocal(nullptr) {
  GPOS_ASSERT(nullptr != pdrgpcrDelete);
  GPOS_ASSERT(nullptr != pdrgpcrInsert);
  GPOS_ASSERT(pdrgpcrInsert->Size() == pdrgpcrDelete->Size());
  GPOS_ASSERT(nullptr != pcrCtid);
  GPOS_ASSERT(nullptr != pcrSegmentId);
  GPOS_ASSERT(nullptr != pcrAction);

  m_pcrsRequiredLocal = GPOS_NEW(mp) CColRefSet(mp);
  m_pcrsRequiredLocal->Include(m_pdrgpcrDelete);
  m_pcrsRequiredLocal->Include(m_pdrgpcrInsert);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::~CPhysicalSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSplit::~CPhysicalSplit() {
  m_pdrgpcrDelete->Release();
  m_pdrgpcrInsert->Release();
  m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PosRequired
//
//	@doc:
//		Compute required sort columns of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSplit::PosRequired(CMemoryPool *mp,
                                        CExpressionHandle &,  // exprhdl
                                        COrderSpec *,         // posRequired
                                        ULONG
#ifdef GPOS_DEBUG
                                            child_index
#endif  // GPOS_DEBUG
                                        ,
                                        CDrvdPropArray *,  // pdrgpdpCtxt
                                        ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  // return empty sort order
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSplit::PosDerive(CMemoryPool *,  // mp,
                                      CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalSplit::EpetOrder(CExpressionHandle &,  // exprhdl
                                                        const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                            peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  // always force sort to be on top of split
  return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalSplit::PcrsRequired(CMemoryPool *mp,
                                         CExpressionHandle &,  // exprhdl,
                                         CColRefSet *pcrsRequired,
                                         ULONG
#ifdef GPOS_DEBUG
                                             child_index
#endif  // GPOS_DEBUG
                                         ,
                                         CDrvdPropArray *,  // pdrgpdpCtxt
                                         ULONG              // ulOptReq
) {
  GPOS_ASSERT(0 == child_index);

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsRequiredLocal);
  pcrs->Union(pcrsRequired);
  pcrs->Exclude(m_pcrAction);

  return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalSplit::PcteRequired(CMemoryPool *,        // mp,
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
//		CPhysicalSplit::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalSplit::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                       ULONG  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);
  GPOS_ASSERT(2 == exprhdl.Arity());

  CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
  // include defined column
  pcrs->Include(m_pcrAction);

  // include output columns of the relational child
  pcrs->Union(exprhdl.DeriveOutputColumns(0 /*child_index*/));

  BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
  pcrs->Release();

  return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalSplit::HashValue() const {
  ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), CUtils::UlHashColArray(m_pdrgpcrInsert));
  ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(m_pcrCtid));
  ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(m_pcrSegmentId));
  ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(m_pcrAction));

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::Matches
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL CPhysicalSplit::Matches(COperator *pop) const {
  if (pop->Eopid() == Eopid()) {
    CPhysicalSplit *popSplit = CPhysicalSplit::PopConvert(pop);

    return m_pcrCtid == popSplit->PcrCtid() && m_pcrSegmentId == popSplit->PcrSegmentId() &&
           m_pcrAction == popSplit->PcrAction() && m_pdrgpcrDelete->Equals(popSplit->PdrgpcrDelete()) &&
           m_pdrgpcrInsert->Equals(popSplit->PdrgpcrInsert());
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalSplit::OsPrint(IOstream &os) const {
  if (m_fPattern) {
    return COperator::OsPrint(os);
  }

  os << SzId() << " -- Delete Columns: [";
  CUtils::OsPrintDrgPcr(os, m_pdrgpcrDelete);
  os << "], Insert Columns: [";
  CUtils::OsPrintDrgPcr(os, m_pdrgpcrInsert);
  os << "], ";
  m_pcrCtid->OsPrint(os);
  os << ", ";
  m_pcrSegmentId->OsPrint(os);
  os << ", Action: ";
  m_pcrAction->OsPrint(os);

  return os;
}

// EOF
