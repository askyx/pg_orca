//---------------------------------------------------------------------------
//	Greenplum Database
//  Copyright (c) 2020 VMware, Inc.
//
//	@filename:
//		CPhysicalRightOuterHashJoin.cpp
//
//	@doc:
//		Implementation of right outer hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalRightOuterHashJoin.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightOuterHashJoin::CPhysicalRightOuterHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalRightOuterHashJoin::CPhysicalRightOuterHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                                                         CExpressionArray *pdrgpexprInnerKeys,
                                                         IMdIdArray *hash_opfamilies, bool is_null_aware,
                                                         CXform::EXformId origin_xform)
    : CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys, hash_opfamilies, is_null_aware, origin_xform) {
  SetPartPropagateRequests(2);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightOuterHashJoin::~CPhysicalRightOuterHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalRightOuterHashJoin::~CPhysicalRightOuterHashJoin() = default;

void CPhysicalRightOuterHashJoin::CreateOptRequests() {
  SetPartPropagateRequests(2);
}

CPartitionPropagationSpec *CPhysicalRightOuterHashJoin::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                                     CPartitionPropagationSpec *pppsRequired,
                                                                     uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt,
                                                                     uint32_t ulOptReq) const {
  return PppsRequiredForJoins(mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, ulOptReq);
}

// In the following function, we are generating the Derived property :
// "Partition Propagation Spec" of Right Outer Hash join.
CPartitionPropagationSpec *CPhysicalRightOuterHashJoin::PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  return PppsDeriveForJoins(mp, exprhdl);
}
// EOF
