//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftOuterHashJoin.cpp
//
//	@doc:
//		Implementation of left outer hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLeftOuterHashJoin.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftOuterHashJoin::CPhysicalLeftOuterHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftOuterHashJoin::CPhysicalLeftOuterHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                                                       CExpressionArray *pdrgpexprInnerKeys,
                                                       IMdIdArray *hash_opfamilies, BOOL is_null_aware,
                                                       CXform::EXformId origin_xform)
    : CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys, hash_opfamilies, is_null_aware, origin_xform) {}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftOuterHashJoin::~CPhysicalLeftOuterHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalLeftOuterHashJoin::~CPhysicalLeftOuterHashJoin() = default;
