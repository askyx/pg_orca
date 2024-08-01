//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Greenplum, Inc.
//
//	Implementation of left outer index nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLeftOuterIndexNLJoin.h"

#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"

using namespace gpopt;

CPhysicalLeftOuterIndexNLJoin::CPhysicalLeftOuterIndexNLJoin(CMemoryPool *mp, CColRefArray *colref_array,
                                                             CExpression *origJoinPred)
    : CPhysicalLeftOuterNLJoin(mp), m_pdrgpcrOuterRefs(colref_array), m_origJoinPred(origJoinPred) {
  GPOS_ASSERT(nullptr != colref_array);
  if (nullptr != origJoinPred) {
    origJoinPred->AddRef();
  }
}

CPhysicalLeftOuterIndexNLJoin::~CPhysicalLeftOuterIndexNLJoin() {
  m_pdrgpcrOuterRefs->Release();
  CRefCount::SafeRelease(m_origJoinPred);
}

BOOL CPhysicalLeftOuterIndexNLJoin::Matches(COperator *pop) const {
  if (pop->Eopid() == Eopid()) {
    return m_pdrgpcrOuterRefs->Equals(CPhysicalLeftOuterIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs());
  }

  return false;
}
