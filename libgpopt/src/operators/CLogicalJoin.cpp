//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLogicalJoin.cpp
//
//	@doc:
//		Implementation of logical join class
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalJoin.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::CLogicalJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------

CLogicalJoin::CLogicalJoin(CMemoryPool *mp, CXform::EXformId origin_xform)
    : CLogical(mp), m_origin_xform(origin_xform) {
  GPOS_ASSERT(nullptr != mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
bool CLogicalJoin::Matches(COperator *pop) const {
  return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *CLogicalJoin::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                        IStatisticsArray *stats_ctxt) const {
  IStatistics *pstats = CJoinStatsProcessor::DeriveJoinStats(mp, exprhdl, stats_ctxt);

  return pstats;
}

// EOF
