//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CScalarBitmapIndexProbe.cpp
//
//	@doc:
//		Bitmap index probe scalar operator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarBitmapIndexProbe.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXform.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapIndexProbe::CScalarBitmapIndexProbe
//
//	@doc:
//		Ctor
//		Takes ownership of the index descriptor and the bitmap type id.
//
//---------------------------------------------------------------------------
CScalarBitmapIndexProbe::CScalarBitmapIndexProbe(CMemoryPool *mp, CIndexDescriptor *pindexdesc,
                                                 CTableDescriptor *ptabdesc, IMDId *pmdidBitmapType)
    : CScalar(mp), m_pindexdesc(pindexdesc), m_ptabdesc(ptabdesc), m_pmdidBitmapType(pmdidBitmapType) {
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != pindexdesc);
  GPOS_ASSERT(nullptr != ptabdesc);
  GPOS_ASSERT(nullptr != pmdidBitmapType);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapIndexProbe::~CScalarBitmapIndexProbe
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarBitmapIndexProbe::~CScalarBitmapIndexProbe() {
  m_pindexdesc->Release();
  m_ptabdesc->Release();
  m_pmdidBitmapType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapIndexProbe::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
uint32_t CScalarBitmapIndexProbe::HashValue() const {
  return gpos::CombineHashes(COperator::HashValue(), m_pindexdesc->MDId()->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapIndexProbe::Matches
//
//	@doc:
//		Match this operator with the given one.
//
//---------------------------------------------------------------------------
bool CScalarBitmapIndexProbe::Matches(COperator *pop) const {
  if (pop->Eopid() != Eopid()) {
    return false;
  }
  CScalarBitmapIndexProbe *popIndexProbe = PopConvert(pop);

  return m_pindexdesc->MDId()->Equals(popIndexProbe->Pindexdesc()->MDId());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapIndexProbe::OsPrint
//
//	@doc:
//		Debug print of this operator
//
//---------------------------------------------------------------------------
IOstream &CScalarBitmapIndexProbe::OsPrint(IOstream &os) const {
  os << SzId() << " ";
  // index name
  os << "  Bitmap Index Name: (";
  m_pindexdesc->Name().OsPrint(os);
  os << ")";

  return os;
}

// EOF
