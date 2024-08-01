//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerIndexNLJoin.cpp
//
//	@doc:
//		Implementation of index inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin(CMemoryPool *mp, CColRefArray *colref_array,
                                                     CExpression *origJoinPred)
    : CPhysicalInnerNLJoin(mp), m_pdrgpcrOuterRefs(colref_array), m_origJoinPred(origJoinPred) {
  GPOS_ASSERT(nullptr != colref_array);
  if (nullptr != origJoinPred) {
    origJoinPred->AddRef();
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin() {
  m_pdrgpcrOuterRefs->Release();
  CRefCount::SafeRelease(m_origJoinPred);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL CPhysicalInnerIndexNLJoin::Matches(COperator *pop) const {
  if (pop->Eopid() == Eopid()) {
    return m_pdrgpcrOuterRefs->Equals(CPhysicalInnerIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs());
  }

  return false;
}
