//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSequenceProject.cpp
//
//	@doc:
//		Implementation of sequence project operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalSequenceProject.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::CLogicalSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSequenceProject::CLogicalSequenceProject(CMemoryPool *mp, COrderSpecArray *pdrgpos, CWindowFrameArray *pdrgpwf)
    : CLogicalUnary(mp), m_pdrgpos(pdrgpos), m_pdrgpwf(pdrgpwf), m_fHasOrderSpecs(false), m_fHasFrameSpecs(false) {
  GPOS_ASSERT(nullptr != pdrgpos);
  GPOS_ASSERT(nullptr != pdrgpwf);

  // set flags indicating that current operator has non-empty order specs/frame specs
  SetHasOrderSpecs(mp);
  SetHasFrameSpecs(mp);

  // include columns used by Partition By, Order By, and window frame edges
  CColRefSet *pcrsSort = COrderSpec::GetColRefSet(mp, m_pdrgpos);
  m_pcrsLocalUsed->Include(pcrsSort);
  pcrsSort->Release();

  const uint32_t ulFrames = m_pdrgpwf->Size();
  for (uint32_t ul = 0; ul < ulFrames; ul++) {
    CWindowFrame *pwf = (*m_pdrgpwf)[ul];
    if (!CWindowFrame::IsEmpty(pwf)) {
      m_pcrsLocalUsed->Include(pwf->PcrsUsed());
    }
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::CLogicalSequenceProject
//
//	@doc:
//		Ctor for patterns
//
//---------------------------------------------------------------------------
CLogicalSequenceProject::CLogicalSequenceProject(CMemoryPool *mp)
    : CLogicalUnary(mp), m_pdrgpos(nullptr), m_pdrgpwf(nullptr), m_fHasOrderSpecs(false), m_fHasFrameSpecs(false) {
  m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::~CLogicalSequenceProject
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalSequenceProject::~CLogicalSequenceProject() {
  CRefCount::SafeRelease(m_pdrgpos);
  CRefCount::SafeRelease(m_pdrgpwf);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *CLogicalSequenceProject::PopCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping,
                                                               bool must_exist) {
  COrderSpecArray *pdrgpos = GPOS_NEW(mp) COrderSpecArray(mp);
  const uint32_t ulOrderSpec = m_pdrgpos->Size();
  for (uint32_t ul = 0; ul < ulOrderSpec; ul++) {
    COrderSpec *pos = ((*m_pdrgpos)[ul])->PosCopyWithRemappedColumns(mp, colref_mapping, must_exist);
    pdrgpos->Append(pos);
  }

  CWindowFrameArray *pdrgpwf = GPOS_NEW(mp) CWindowFrameArray(mp);
  const uint32_t ulWindowFrames = m_pdrgpwf->Size();
  for (uint32_t ul = 0; ul < ulWindowFrames; ul++) {
    CWindowFrame *pwf = ((*m_pdrgpwf)[ul])->PwfCopyWithRemappedColumns(mp, colref_mapping, must_exist);
    pdrgpwf->Append(pwf);
  }

  return GPOS_NEW(mp) CLogicalSequenceProject(mp, pdrgpos, pdrgpwf);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::SetHasOrderSpecs
//
//	@doc:
//		Set the flag indicating that SeqPrj has specified order specs
//
//---------------------------------------------------------------------------
void CLogicalSequenceProject::SetHasOrderSpecs(CMemoryPool *mp) {
  GPOS_ASSERT(nullptr != m_pdrgpos);

  const uint32_t ulOrderSpecs = m_pdrgpos->Size();
  if (0 == ulOrderSpecs) {
    // if no order specs are given, we add one empty order spec
    m_pdrgpos->Append(GPOS_NEW(mp) COrderSpec(mp));
  }
  bool fHasOrderSpecs = false;
  for (uint32_t ul = 0; !fHasOrderSpecs && ul < ulOrderSpecs; ul++) {
    fHasOrderSpecs = !(*m_pdrgpos)[ul]->IsEmpty();
  }
  m_fHasOrderSpecs = fHasOrderSpecs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::SetHasFrameSpecs
//
//	@doc:
//		Set the flag indicating that SeqPrj has specified frame specs
//
//---------------------------------------------------------------------------
void CLogicalSequenceProject::SetHasFrameSpecs(CMemoryPool *  // mp
) {
  GPOS_ASSERT(nullptr != m_pdrgpwf);

  const uint32_t ulFrameSpecs = m_pdrgpwf->Size();
  if (0 == ulFrameSpecs) {
    // if no frame specs are given, we add one empty frame
    CWindowFrame *pwf = const_cast<CWindowFrame *>(CWindowFrame::PwfEmpty());
    pwf->AddRef();
    m_pdrgpwf->Append(pwf);
  }
  bool fHasFrameSpecs = false;
  for (uint32_t ul = 0; !fHasFrameSpecs && ul < ulFrameSpecs; ul++) {
    fHasFrameSpecs = !CWindowFrame::IsEmpty((*m_pdrgpwf)[ul]);
  }
  m_fHasFrameSpecs = fHasFrameSpecs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *CLogicalSequenceProject::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl) {
  GPOS_ASSERT(2 == exprhdl.Arity());

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

  pcrs->Union(exprhdl.DeriveOutputColumns(0));

  // the scalar child defines additional columns
  pcrs->Union(exprhdl.DeriveDefinedColumns(1));

  return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::DeriveOuterReferences
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *CLogicalSequenceProject::DeriveOuterReferences(CMemoryPool *mp, CExpressionHandle &exprhdl) {
  CColRefSet *outer_refs = CLogical::DeriveOuterReferences(mp, exprhdl, m_pcrsLocalUsed);

  return outer_refs;
}

//---------------------------------------------------------------------------
// CLogicalSequenceProject::FHasLocalReferencesTo
//
// Return true if Partition/Order or window frame edges reference one of
// the provided ColRefs
//
//---------------------------------------------------------------------------
bool CLogicalSequenceProject::FHasLocalReferencesTo(const CColRefSet *outerRefsToCheck) const {
  return !outerRefsToCheck->IsDisjoint(m_pcrsLocalUsed);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *CLogicalSequenceProject::DeriveKeyCollection(CMemoryPool *,  // mp
                                                             CExpressionHandle &exprhdl) const {
  return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard CLogicalSequenceProject::DeriveMaxCard(CMemoryPool *,  // mp
                                                CExpressionHandle &exprhdl) const {
  // pass on max card of first child
  return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::Matches
//
//	@doc:
//		Matching function
//
//---------------------------------------------------------------------------
bool CLogicalSequenceProject::Matches(COperator *pop) const {
  GPOS_ASSERT(nullptr != pop);
  if (Eopid() == pop->Eopid()) {
    CLogicalSequenceProject *popLogicalSequenceProject = CLogicalSequenceProject::PopConvert(pop);
    return CWindowFrame::Equals(m_pdrgpwf, popLogicalSequenceProject->Pdrgpwf()) &&
           COrderSpec::Equals(m_pdrgpos, popLogicalSequenceProject->Pdrgpos());
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::HashValue
//
//	@doc:
//		Hashing function
//
//---------------------------------------------------------------------------
uint32_t CLogicalSequenceProject::HashValue() const {
  uint32_t ulHash = 0;
  ulHash = gpos::CombineHashes(ulHash, CWindowFrame::HashValue(m_pdrgpwf, 3 /*ulMaxSize*/));
  ulHash = gpos::CombineHashes(ulHash, COrderSpec::HashValue(m_pdrgpos, 3 /*ulMaxSize*/));

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *CLogicalSequenceProject::PxfsCandidates(CMemoryPool *mp) const {
  CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
  (void)xform_set->ExchangeSet(CXform::ExfSequenceProject2Apply);
  (void)xform_set->ExchangeSet(CXform::ExfImplementSequenceProject);

  return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PstatsDerive
//
//	@doc:
//		Derive statistics based on filter predicates
//
//---------------------------------------------------------------------------
IStatistics *CLogicalSequenceProject::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                   IStatisticsArray *  // stats_ctxt
) const {
  return PstatsDeriveProject(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &CLogicalSequenceProject::OsPrint(IOstream &os) const {
  os << SzId() << " (";
  os << "Order Spec:";
  (void)COrderSpec::OsPrint(os, m_pdrgpos);
  os << ", ";
  os << "WindowFrame Spec:";
  (void)CWindowFrame::OsPrint(os, m_pdrgpwf);

  return os << ")";
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PopRemoveLocalOuterRefs
//
//	@doc:
//		Filter out outer references from Order By/ Partition By
//		clauses, and return a new operator
//
//---------------------------------------------------------------------------
CLogicalSequenceProject *CLogicalSequenceProject::PopRemoveLocalOuterRefs(CMemoryPool *mp, CExpressionHandle &exprhdl) {
  GPOS_ASSERT(this == exprhdl.Pop());

  CColRefSet *outer_refs = exprhdl.DeriveOuterReferences();

  COrderSpecArray *pdrgpos = COrderSpec::PdrgposExclude(mp, m_pdrgpos, outer_refs);

  // for window frame edges, outer references cannot be removed since this can change
  // the semantics of frame edge from delayed-bounding to unbounded,
  // we re-use the frame edges without changing here
  m_pdrgpwf->AddRef();

  return GPOS_NEW(mp) CLogicalSequenceProject(mp, pdrgpos, m_pdrgpwf);
}

// EOF
