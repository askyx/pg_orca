//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CPhysicalLeftAntiSemiHashJoinNotIn.cpp
//
//	@doc:
//		Implementation of left anti semi hash join operator with NotIn semantics
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLeftAntiSemiHashJoinNotIn.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftAntiSemiHashJoinNotIn::CPhysicalLeftAntiSemiHashJoinNotIn
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftAntiSemiHashJoinNotIn::CPhysicalLeftAntiSemiHashJoinNotIn(CMemoryPool *mp,
                                                                       CExpressionArray *pdrgpexprOuterKeys,
                                                                       CExpressionArray *pdrgpexprInnerKeys,
                                                                       IMdIdArray *hash_opfamilies, bool is_null_aware,
                                                                       CXform::EXformId origin_xform)
    : CPhysicalLeftAntiSemiHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys, hash_opfamilies, is_null_aware,
                                    origin_xform) {}
