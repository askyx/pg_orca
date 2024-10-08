//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CFunctionalDependency.cpp
//
//	@doc:
//		Implementation of functional dependency
//---------------------------------------------------------------------------

#include "gpopt/base/CFunctionalDependency.h"

#include "gpopt/base/CUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::CFunctionalDependency
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFunctionalDependency::CFunctionalDependency(CColRefSet *pcrsKey, CColRefSet *pcrsDetermined)
    : m_pcrsKey(pcrsKey), m_pcrsDetermined(pcrsDetermined) {
  GPOS_ASSERT(0 < pcrsKey->Size());
  GPOS_ASSERT(0 < m_pcrsDetermined->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::~CFunctionalDependency
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFunctionalDependency::~CFunctionalDependency() {
  m_pcrsKey->Release();
  m_pcrsDetermined->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::FIncluded
//
//	@doc:
//		Determine if all FD columns are included in the given column set
//
//---------------------------------------------------------------------------
bool CFunctionalDependency::FIncluded(CColRefSet *pcrs) const {
  return pcrs->ContainsAll(m_pcrsKey) && pcrs->ContainsAll(m_pcrsDetermined);
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
uint32_t CFunctionalDependency::HashValue() const {
  return gpos::CombineHashes(m_pcrsKey->HashValue(), m_pcrsDetermined->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool CFunctionalDependency::Equals(const CFunctionalDependency *pfd) const {
  if (nullptr == pfd) {
    return false;
  }

  return m_pcrsKey->Equals(pfd->PcrsKey()) && m_pcrsDetermined->Equals(pfd->PcrsDetermined());
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &CFunctionalDependency::OsPrint(IOstream &os) const {
  os << "(" << *m_pcrsKey << ")";
  os << " --> (" << *m_pcrsDetermined << ")";
  return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
uint32_t CFunctionalDependency::HashValue(const CFunctionalDependencyArray *pdrgpfd) {
  uint32_t ulHash = 0;
  if (nullptr != pdrgpfd) {
    const uint32_t size = pdrgpfd->Size();
    for (uint32_t ul = 0; ul < size; ul++) {
      ulHash = gpos::CombineHashes(ulHash, (*pdrgpfd)[ul]->HashValue());
    }
  }

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool CFunctionalDependency::Equals(const CFunctionalDependencyArray *pdrgpfdFst,
                                   const CFunctionalDependencyArray *pdrgpfdSnd) {
  if (nullptr == pdrgpfdFst && nullptr == pdrgpfdSnd) {
    return true; /* both empty */
  }

  if (nullptr == pdrgpfdFst || nullptr == pdrgpfdSnd) {
    return false; /* one is empty, the other is not */
  }

  const uint32_t ulLenFst = pdrgpfdFst->Size();
  const uint32_t ulLenSnd = pdrgpfdSnd->Size();

  if (ulLenFst != ulLenSnd) {
    return false;
  }

  bool fEqual = true;
  for (uint32_t ulFst = 0; fEqual && ulFst < ulLenFst; ulFst++) {
    const CFunctionalDependency *pfdFst = (*pdrgpfdFst)[ulFst];
    bool fMatch = false;
    for (uint32_t ulSnd = 0; !fMatch && ulSnd < ulLenSnd; ulSnd++) {
      const CFunctionalDependency *pfdSnd = (*pdrgpfdSnd)[ulSnd];
      fMatch = pfdFst->Equals(pfdSnd);
    }
    fEqual = fMatch;
  }

  return fEqual;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::PcrsKeys
//
//	@doc:
//		Create a set of all keys
//
//---------------------------------------------------------------------------
CColRefSet *CFunctionalDependency::PcrsKeys(CMemoryPool *mp, const CFunctionalDependencyArray *pdrgpfd) {
  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

  if (pdrgpfd != nullptr) {
    const uint32_t size = pdrgpfd->Size();
    for (uint32_t ul = 0; ul < size; ul++) {
      pcrs->Include((*pdrgpfd)[ul]->PcrsKey());
    }
  }

  return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::PdrgpcrKeys
//
//	@doc:
//		Create an array of all keys
//
//---------------------------------------------------------------------------
CColRefArray *CFunctionalDependency::PdrgpcrKeys(CMemoryPool *mp, const CFunctionalDependencyArray *pdrgpfd) {
  CColRefSet *pcrs = PcrsKeys(mp, pdrgpfd);
  CColRefArray *colref_array = pcrs->Pdrgpcr(mp);
  pcrs->Release();

  return colref_array;
}

// EOF
