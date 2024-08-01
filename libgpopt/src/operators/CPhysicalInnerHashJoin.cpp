//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalInnerHashJoin.cpp
//
//	@doc:
//		Implementation of inner hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerHashJoin.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::CPhysicalInnerHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::CPhysicalInnerHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                                               CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
                                               BOOL is_null_aware, CXform::EXformId origin_xform)
    : CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys, hash_opfamilies, is_null_aware, origin_xform) {}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin() = default;

CPartitionPropagationSpec *CPhysicalInnerHashJoin::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                                CPartitionPropagationSpec *pppsRequired,
                                                                ULONG child_index, CDrvdPropArray *pdrgpdpCtxt,
                                                                ULONG ulOptReq) const {
  return PppsRequiredForJoins(mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, ulOptReq);
}

CPartitionPropagationSpec *CPhysicalInnerHashJoin::PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  return PppsDeriveForJoins(mp, exprhdl);
}

// EOF
