//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CColRef.cpp
//
//	@doc:
//		Implementation of column reference class
//---------------------------------------------------------------------------

#include "gpopt/base/CColRef.h"

#include "gpos/base.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif  // GPOS_DEBUG

using namespace gpopt;

// invalid key
const uint32_t CColRef::m_ulInvalid = UINT32_MAX;

//---------------------------------------------------------------------------
//	@function:
//		CColRef::CColRef
//
//	@doc:
//		ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRef::CColRef(const IMDType *pmdtype, const int32_t type_modifier, uint32_t id, const CName *pname)
    : m_pmdtype(pmdtype),
      m_type_modifier(type_modifier),
      m_pname(pname),
      m_used(EUnknown),
      m_mdid_table(nullptr),
      m_id(id) {
  GPOS_ASSERT(nullptr != pmdtype);
  GPOS_ASSERT(pmdtype->MDId()->IsValid());
  GPOS_ASSERT(nullptr != pname);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::~CColRef
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColRef::~CColRef() {
  // we own the name
  GPOS_DELETE(m_pname);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::HashValue
//
//	@doc:
//		static hash function
//
//---------------------------------------------------------------------------
uint32_t CColRef::HashValue(const uint32_t &ulptr) {
  return gpos::HashValue<uint32_t>(&ulptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::HashValue
//
//	@doc:
//		static hash function
//
//---------------------------------------------------------------------------
uint32_t CColRef::HashValue(const CColRef *colref) {
  uint32_t id = colref->Id();
  return gpos::HashValue<uint32_t>(&id);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &CColRef::OsPrint(IOstream &os) const {
  m_pname->OsPrint(os);
  os << " (" << Id() << ")";

  return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::Pdrgpul
//
//	@doc:
//		Extract array of colids from array of colrefs
//
//---------------------------------------------------------------------------
ULongPtrArray *CColRef::Pdrgpul(CMemoryPool *mp, CColRefArray *colref_array) {
  ULongPtrArray *pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
  const uint32_t length = colref_array->Size();
  for (uint32_t ul = 0; ul < length; ul++) {
    CColRef *colref = (*colref_array)[ul];
    pdrgpul->Append(GPOS_NEW(mp) uint32_t(colref->Id()));
  }

  return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::Equals
//
//	@doc:
//		Are the two arrays of column references equivalent
//
//---------------------------------------------------------------------------
bool CColRef::Equals(const CColRefArray *pdrgpcr1, const CColRefArray *pdrgpcr2) {
  if (nullptr == pdrgpcr1 || nullptr == pdrgpcr2) {
    return (nullptr == pdrgpcr1 && nullptr == pdrgpcr2);
  }

  return pdrgpcr1->Equals(pdrgpcr2);
}

// check if the the array of column references are equal. Note that since we have unique
// copy of the column references, we can compare pointers.
bool CColRef::Equals(const CColRef2dArray *pdrgdrgpcr1, const CColRef2dArray *pdrgdrgpcr2) {
  uint32_t ulLen1 = (pdrgdrgpcr1 == nullptr) ? 0 : pdrgdrgpcr1->Size();
  uint32_t ulLen2 = (pdrgdrgpcr2 == nullptr) ? 0 : pdrgdrgpcr2->Size();

  if (ulLen1 != ulLen2) {
    return false;
  }

  for (uint32_t ulLevel = 0; ulLevel < ulLen1; ulLevel++) {
    bool fEqual = (*pdrgdrgpcr1)[ulLevel]->Equals((*pdrgdrgpcr2)[ulLevel]);
    if (!fEqual) {
      return false;
    }
  }

  return true;
}
