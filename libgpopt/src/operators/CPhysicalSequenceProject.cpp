//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSequenceProject.cpp
//
//	@doc:
//		Implementation of physical sequence project operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalSequenceProject.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::CPhysicalSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSequenceProject::CPhysicalSequenceProject(CMemoryPool *mp, COrderSpecArray *pdrgpos,
                                                   CWindowFrameArray *pdrgpwf)
    : CPhysical(mp), m_pdrgpos(pdrgpos), m_pdrgpwf(pdrgpwf), m_pos(nullptr), m_pcrsRequiredLocal(nullptr) {
  GPOS_ASSERT(nullptr != pdrgpos);
  GPOS_ASSERT(nullptr != pdrgpwf);

  CreateOrderSpec(mp);
  ComputeRequiredLocalColumns(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::CreateOrderSpec
//
//	@doc:
//		Create local order spec that we request relational child to satisfy
//
//---------------------------------------------------------------------------
void CPhysicalSequenceProject::CreateOrderSpec(CMemoryPool *mp) {
  GPOS_ASSERT(nullptr == m_pos);
  GPOS_ASSERT(nullptr != m_pdrgpos);

  m_pos = GPOS_NEW(mp) COrderSpec(mp);

  if (0 == m_pdrgpos->Size()) {
    return;
  }

  COrderSpec *posFirst = (*m_pdrgpos)[0];
#ifdef GPOS_DEBUG
  const uint32_t length = m_pdrgpos->Size();
  for (uint32_t ul = 1; ul < length; ul++) {
    COrderSpec *posCurrent = (*m_pdrgpos)[ul];
    GPOS_ASSERT(posFirst->FSatisfies(posCurrent) && "first order spec must satisfy all other order specs");
  }
#endif  // GPOS_DEBUG

  // we assume here that the first order spec in the children array satisfies all other
  // order specs in the array, this happens as part of the initial normalization
  // so we need to add columns only from the first order spec
  const uint32_t size = posFirst->UlSortColumns();
  for (uint32_t ul = 0; ul < size; ul++) {
    const CColRef *colref = posFirst->Pcr(ul);
    gpmd::IMDId *mdid = posFirst->GetMdIdSortOp(ul);
    mdid->AddRef();
    COrderSpec::ENullTreatment ent = posFirst->Ent(ul);
    m_pos->Append(mdid, colref, ent);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::ComputeRequiredLocalColumns
//
//	@doc:
//		Compute local required columns
//
//---------------------------------------------------------------------------
void CPhysicalSequenceProject::ComputeRequiredLocalColumns(CMemoryPool *mp) {
  GPOS_ASSERT(nullptr != m_pos);
  GPOS_ASSERT(nullptr != m_pdrgpos);
  GPOS_ASSERT(nullptr != m_pdrgpwf);
  GPOS_ASSERT(nullptr == m_pcrsRequiredLocal);

  m_pcrsRequiredLocal = m_pos->PcrsUsed(mp);

  // add the columns used in the window frames
  const uint32_t size = m_pdrgpwf->Size();
  for (uint32_t ul = 0; ul < size; ul++) {
    CWindowFrame *pwf = (*m_pdrgpwf)[ul];
    if (nullptr != pwf->PexprLeading()) {
      m_pcrsRequiredLocal->Union(pwf->PexprLeading()->DeriveUsedColumns());
    }
    if (nullptr != pwf->PexprTrailing()) {
      m_pcrsRequiredLocal->Union(pwf->PexprTrailing()->DeriveUsedColumns());
    }
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::~CPhysicalSequenceProject
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSequenceProject::~CPhysicalSequenceProject() {
  m_pdrgpos->Release();
  m_pdrgpwf->Release();
  m_pos->Release();
  m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
bool CPhysicalSequenceProject::Matches(COperator *pop) const {
  GPOS_ASSERT(nullptr != pop);
  if (Eopid() == pop->Eopid()) {
    CPhysicalSequenceProject *popPhysicalSequenceProject = CPhysicalSequenceProject::PopConvert(pop);
    return CWindowFrame::Equals(m_pdrgpwf, popPhysicalSequenceProject->Pdrgpwf()) &&
           COrderSpec::Equals(m_pdrgpos, popPhysicalSequenceProject->Pdrgpos());
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::HashValue
//
//	@doc:
//		Hashing function
//
//---------------------------------------------------------------------------
uint32_t CPhysicalSequenceProject::HashValue() const {
  uint32_t ulHash = 0;
  ulHash = gpos::CombineHashes(ulHash, CWindowFrame::HashValue(m_pdrgpwf, 3 /*ulMaxSize*/));
  ulHash = gpos::CombineHashes(ulHash, COrderSpec::HashValue(m_pdrgpos, 3 /*ulMaxSize*/));

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalSequenceProject::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                   CColRefSet *pcrsRequired, uint32_t child_index,
                                                   CDrvdPropArray *,  // pdrgpdpCtxt
                                                   uint32_t           // ulOptReq
) {
  GPOS_ASSERT(0 == child_index && "Required properties can only be computed on the relational child");

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsRequiredLocal);
  pcrs->Union(pcrsRequired);

  CColRefSet *pcrsOutput = PcrsChildReqd(mp, exprhdl, pcrs, child_index, 1 /*ulScalarIndex*/);
  pcrs->Release();

  return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSequenceProject::PosRequired(CMemoryPool *,        // mp
                                                  CExpressionHandle &,  // exprhdl
                                                  COrderSpec *,         // posRequired
                                                  uint32_t
#ifdef GPOS_DEBUG
                                                      child_index
#endif  // GPOS_DEBUG
                                                  ,
                                                  CDrvdPropArray *,  // pdrgpdpCtxt
                                                  uint32_t           // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  m_pos->AddRef();

  return m_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalSequenceProject::PcteRequired(CMemoryPool *,        // mp,
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
//		CPhysicalSequenceProject::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalSequenceProject::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                                 uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);
  GPOS_ASSERT(2 == exprhdl.Arity());

  CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
  // include defined columns by scalar project list
  pcrs->Union(exprhdl.DeriveDefinedColumns(1));

  // include output columns of the relational child
  pcrs->Union(exprhdl.DeriveOutputColumns(0 /*child_index*/));

  bool fProvidesCols = pcrs->ContainsAll(pcrsRequired);
  pcrs->Release();

  return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSequenceProject::PosDerive(CMemoryPool *,  // mp
                                                CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalSequenceProject::EpetOrder(CExpressionHandle &exprhdl,
                                                                  const CEnfdOrder *peo) const {
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
//		CPhysicalSequenceProject::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalSequenceProject::OsPrint(IOstream &os) const {
  os << SzId() << " (";
  (void)COrderSpec::OsPrint(os, m_pdrgpos);
  os << ", ";
  (void)CWindowFrame::OsPrint(os, m_pdrgpwf);

  return os << ")";
}

// EOF
