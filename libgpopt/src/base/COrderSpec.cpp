//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COrderSpec.cpp
//
//	@doc:
//		Specification of order property
//---------------------------------------------------------------------------

#include "gpopt/base/COrderSpec.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalSort.h"

#ifdef GPOS_DEBUG
#include "gpos/error/CAutoTrace.h"
#endif  // GPOS_DEBUG

using namespace gpopt;
using namespace gpmd;

// string encoding of null treatment
const char rgszNullCode[][16] = {"Auto", "NULLsFirst", "NULLsLast"};
GPOS_CPL_ASSERT(COrderSpec::EntSentinel == GPOS_ARRAY_SIZE(rgszNullCode), "");

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::COrderExpression
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderSpec::COrderExpression::COrderExpression(gpmd::IMDId *mdid, const CColRef *colref, ENullTreatment ent)
    : m_mdid(mdid), m_pcr(colref), m_ent(ent) {
  GPOS_ASSERT(nullptr != colref);
  GPOS_ASSERT(mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::~COrderExpression
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderSpec::COrderExpression::~COrderExpression() {
  m_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::Matches
//
//	@doc:
//		Check if order expression equal to given one;
//
//---------------------------------------------------------------------------
bool COrderSpec::COrderExpression::Matches(const COrderExpression *poe) const {
  GPOS_ASSERT(nullptr != poe);

  return poe->m_mdid->Equals(m_mdid) && poe->m_pcr == m_pcr && poe->m_ent == m_ent;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::OsPrint
//
//	@doc:
//		Print order expression
//
//---------------------------------------------------------------------------
IOstream &COrderSpec::COrderExpression::OsPrint(IOstream &os) const {
  os << "( ";
  m_mdid->OsPrint(os);
  os << ", ";
  m_pcr->OsPrint(os);
  os << ", " << rgszNullCode[m_ent] << " )";

  return os;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderSpec::COrderSpec(CMemoryPool *mp) : m_mp(mp), m_pdrgpoe(nullptr) {
  m_pdrgpoe = GPOS_NEW(mp) COrderExpressionArray(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::~COrderSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderSpec::~COrderSpec() {
  m_pdrgpoe->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Append
//
//	@doc:
//		Append order expression;
//
//---------------------------------------------------------------------------
void COrderSpec::Append(gpmd::IMDId *mdid, const CColRef *colref, ENullTreatment ent) {
  COrderExpression *poe = GPOS_NEW(m_mp) COrderExpression(mdid, colref, ent);
  m_pdrgpoe->Append(poe);
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Matches
//
//	@doc:
//		Check for equality between order specs
//
//---------------------------------------------------------------------------
bool COrderSpec::Matches(const COrderSpec *pos) const {
  bool fMatch = m_pdrgpoe->Size() == pos->m_pdrgpoe->Size() && FSatisfies(pos);

  GPOS_ASSERT_IMP(fMatch, pos->FSatisfies(this));

  return fMatch;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::FSatisfies
//
//	@doc:
//		Check if this order spec satisfies the given one
//
//---------------------------------------------------------------------------
bool COrderSpec::FSatisfies(const COrderSpec *pos) const {
  const uint32_t arity = pos->m_pdrgpoe->Size();
  bool fSatisfies = (m_pdrgpoe->Size() >= arity);

  for (uint32_t ul = 0; fSatisfies && ul < arity; ul++) {
    fSatisfies = (*m_pdrgpoe)[ul]->Matches((*(pos->m_pdrgpoe))[ul]);
  }

  return fSatisfies;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers enforcers to dynamic array
//
//---------------------------------------------------------------------------
void COrderSpec::AppendEnforcers(CMemoryPool *mp,
                                 CExpressionHandle &,  // exprhdl
                                 CReqdPropPlan *
#ifdef GPOS_DEBUG
                                     prpp
#endif  // GPOS_DEBUG
                                 ,
                                 CExpressionArray *pdrgpexpr, CExpression *pexpr) {
  GPOS_ASSERT(nullptr != prpp);
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != pdrgpexpr);
  GPOS_ASSERT(nullptr != pexpr);
  GPOS_ASSERT(this == prpp->Peo()->PosRequired() && "required plan properties don't match enforced order spec");

  AddRef();
  pexpr->AddRef();
  CExpression *pexprSort = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPhysicalSort(mp, this), pexpr);
  pdrgpexpr->Append(pexprSort);
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::HashValue
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
uint32_t COrderSpec::HashValue() const {
  uint32_t ulHash = 0;
  uint32_t arity = m_pdrgpoe->Size();

  for (uint32_t ul = 0; ul < arity; ul++) {
    COrderExpression *poe = (*m_pdrgpoe)[ul];
    ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(poe->Pcr()));
  }

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PosCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the order spec with remapped columns
//
//---------------------------------------------------------------------------
COrderSpec *COrderSpec::PosCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, bool must_exist) {
  COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);

  const uint32_t num_cols = m_pdrgpoe->Size();
  for (uint32_t ul = 0; ul < num_cols; ul++) {
    COrderExpression *poe = (*m_pdrgpoe)[ul];
    IMDId *mdid = poe->GetMdIdSortOp();
    mdid->AddRef();

    const CColRef *colref = poe->Pcr();
    uint32_t id = colref->Id();
    CColRef *pcrMapped = colref_mapping->Find(&id);
    if (nullptr == pcrMapped) {
      if (must_exist) {
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        // not found in hashmap, so create a new colref and add to hashmap
        pcrMapped = col_factory->PcrCopy(colref);

        bool result GPOS_ASSERTS_ONLY = colref_mapping->Insert(GPOS_NEW(mp) uint32_t(id), pcrMapped);
        GPOS_ASSERT(result);
      } else {
        pcrMapped = const_cast<CColRef *>(colref);
      }
    }

    COrderSpec::ENullTreatment ent = poe->Ent();
    pos->Append(mdid, pcrMapped, ent);
  }

  return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PosExcludeColumns
//
//	@doc:
//		Return a copy of the order spec after excluding the given columns
//
//---------------------------------------------------------------------------
COrderSpec *COrderSpec::PosExcludeColumns(CMemoryPool *mp, CColRefSet *pcrs) {
  GPOS_ASSERT(nullptr != pcrs);

  COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);

  const uint32_t num_cols = m_pdrgpoe->Size();
  for (uint32_t ul = 0; ul < num_cols; ul++) {
    COrderExpression *poe = (*m_pdrgpoe)[ul];
    const CColRef *colref = poe->Pcr();

    if (pcrs->FMember(colref)) {
      continue;
    }

    IMDId *mdid = poe->GetMdIdSortOp();
    mdid->AddRef();
    pos->Append(mdid, colref, poe->Ent());
  }

  return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::ExtractCols
//
//	@doc:
//		Extract columns from order spec into the given column set
//
//---------------------------------------------------------------------------
void COrderSpec::ExtractCols(CColRefSet *pcrs) const {
  GPOS_ASSERT(nullptr != pcrs);

  const uint32_t ulOrderExprs = m_pdrgpoe->Size();
  for (uint32_t ul = 0; ul < ulOrderExprs; ul++) {
    pcrs->Include((*m_pdrgpoe)[ul]->Pcr());
  }
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PcrsUsed
//
//	@doc:
//		Extract colref set from order components
//
//---------------------------------------------------------------------------
CColRefSet *COrderSpec::PcrsUsed(CMemoryPool *mp) const {
  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
  ExtractCols(pcrs);

  return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::GetColRefSet
//
//	@doc:
//		Extract colref set from order specs in the given array
//
//---------------------------------------------------------------------------
CColRefSet *COrderSpec::GetColRefSet(CMemoryPool *mp, COrderSpecArray *pdrgpos) {
  GPOS_ASSERT(nullptr != pdrgpos);

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
  const uint32_t ulOrderSpecs = pdrgpos->Size();
  for (uint32_t ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++) {
    COrderSpec *pos = (*pdrgpos)[ulSpec];
    pos->ExtractCols(pcrs);
  }

  return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PdrgposExclude
//
//	@doc:
//		Filter out array of order specs from order expressions using the
//		passed columns
//
//---------------------------------------------------------------------------
COrderSpecArray *COrderSpec::PdrgposExclude(CMemoryPool *mp, COrderSpecArray *pdrgpos, CColRefSet *pcrsToExclude) {
  GPOS_ASSERT(nullptr != pdrgpos);
  GPOS_ASSERT(nullptr != pcrsToExclude);

  if (0 == pcrsToExclude->Size()) {
    // no columns to exclude
    pdrgpos->AddRef();
    return pdrgpos;
  }

  COrderSpecArray *pdrgposNew = GPOS_NEW(mp) COrderSpecArray(mp);
  const uint32_t ulOrderSpecs = pdrgpos->Size();
  for (uint32_t ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++) {
    COrderSpec *pos = (*pdrgpos)[ulSpec];
    COrderSpec *posNew = pos->PosExcludeColumns(mp, pcrsToExclude);
    pdrgposNew->Append(posNew);
  }

  return pdrgposNew;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::OsPrint
//
//	@doc:
//		Print order spec
//
//---------------------------------------------------------------------------
IOstream &COrderSpec::OsPrint(IOstream &os) const {
  const uint32_t arity = m_pdrgpoe->Size();
  if (0 == arity) {
    os << "<empty>";
  } else {
    for (uint32_t ul = 0; ul < arity; ul++) {
      (*m_pdrgpoe)[ul]->OsPrint(os) << " ";
    }
  }

  return os;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Equals
//
//	@doc:
//		 Matching function over order spec arrays
//
//---------------------------------------------------------------------------
bool COrderSpec::Equals(const COrderSpecArray *pdrgposFirst, const COrderSpecArray *pdrgposSecond) {
  if (nullptr == pdrgposFirst || nullptr == pdrgposSecond) {
    return (nullptr == pdrgposFirst && nullptr == pdrgposSecond);
  }

  if (pdrgposFirst->Size() != pdrgposSecond->Size()) {
    return false;
  }

  const uint32_t size = pdrgposFirst->Size();
  bool fMatch = true;
  for (uint32_t ul = 0; fMatch && ul < size; ul++) {
    fMatch = (*pdrgposFirst)[ul]->Matches((*pdrgposSecond)[ul]);
  }

  return fMatch;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::HashValue
//
//	@doc:
//		 Combine hash values of a maximum number of entries
//
//---------------------------------------------------------------------------
uint32_t COrderSpec::HashValue(const COrderSpecArray *pdrgpos, uint32_t ulMaxSize) {
  GPOS_ASSERT(nullptr != pdrgpos);
  uint32_t size = std::min(ulMaxSize, pdrgpos->Size());

  uint32_t ulHash = 0;
  for (uint32_t ul = 0; ul < size; ul++) {
    ulHash = gpos::CombineHashes(ulHash, (*pdrgpos)[ul]->HashValue());
  }

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::OsPrint
//
//	@doc:
//		 Print array of order spec objects
//
//---------------------------------------------------------------------------
IOstream &COrderSpec::OsPrint(IOstream &os, const COrderSpecArray *pdrgpos) {
  const uint32_t size = pdrgpos->Size();
  os << "[";
  if (0 < size) {
    for (uint32_t ul = 0; ul < size - 1; ul++) {
      (void)(*pdrgpos)[ul]->OsPrint(os);
      os << ", ";
    }

    (void)(*pdrgpos)[size - 1]->OsPrint(os);
  }

  return os << "]";
}

// EOF
