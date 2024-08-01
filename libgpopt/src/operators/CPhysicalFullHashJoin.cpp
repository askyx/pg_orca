//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom
//
//	@filename:
//		CPhysicalFullHashJoin.cpp
//
//	@doc:
//		Implementation of full hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalFullHashJoin.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFullHashJoin::CPhysicalFullHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalFullHashJoin::CPhysicalFullHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                                             CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
                                             BOOL is_null_aware, CXform::EXformId origin_xform)
    : CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys, hash_opfamilies, is_null_aware, origin_xform) {
  SetPartPropagateRequests(2);
}
