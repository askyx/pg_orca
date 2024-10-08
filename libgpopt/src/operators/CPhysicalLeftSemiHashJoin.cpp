//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftSemiHashJoin.cpp
//
//	@doc:
//		Implementation of left semi hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLeftSemiHashJoin.h"

#include "gpopt/base/CUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::CPhysicalLeftSemiHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftSemiHashJoin::CPhysicalLeftSemiHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                                                     CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
                                                     bool is_null_aware, CXform::EXformId origin_xform)
    : CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys, hash_opfamilies, is_null_aware, origin_xform) {}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::~CPhysicalLeftSemiHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalLeftSemiHashJoin::~CPhysicalLeftSemiHashJoin() = default;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalLeftSemiHashJoin::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                                  uint32_t  // ulOptReq
) const {
  // left semi join only propagates columns from left child
  return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}

CPartitionPropagationSpec *CPhysicalLeftSemiHashJoin::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                                   CPartitionPropagationSpec *pppsRequired,
                                                                   uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt,
                                                                   uint32_t ulOptReq) const {
  return PppsRequiredForJoins(mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, ulOptReq);
}

// In the following function, we are generating the Derived property :
// "Partition Propagation Spec" of Right Outer Hash join.
CPartitionPropagationSpec *CPhysicalLeftSemiHashJoin::PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  return PppsDeriveForJoins(mp, exprhdl);
}
// EOF
