//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalUnary.cpp
//
//	@doc:
//		Implementation of logical unary operators
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalUnary.h"

#include "gpopt/xforms/CXformUtils.h"
#include "gpos/base.h"
#include "naucrates/statistics/CProjectStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL CLogicalUnary::Matches(COperator *pop) const {
  return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise CLogicalUnary::Esp(CExpressionHandle &exprhdl) const {
  // low promise for stat derivation if scalar predicate has subqueries, or logical
  // expression has outer-refs or is part of an Apply expression
  if (exprhdl.DeriveHasSubquery(1) || exprhdl.HasOuterRefs() ||
      (nullptr != exprhdl.Pgexpr() && CXformUtils::FGenerateApply(exprhdl.Pgexpr()->ExfidOrigin()))) {
    return EspLow;
  }

  return EspHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::PstatsDeriveProject
//
//	@doc:
//		Derive statistics for projection operators
//
//---------------------------------------------------------------------------
IStatistics *CLogicalUnary::PstatsDeriveProject(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                UlongToIDatumMap *phmuldatum,
                                                UlongToConstColRefMap *colidToColrefMapForNDVExpr) const {
  GPOS_ASSERT(Esp(exprhdl) > EspNone);
  IStatistics *child_stats = exprhdl.Pstats(0);
  CReqdPropRelational *prprel = CReqdPropRelational::GetReqdRelationalProps(exprhdl.Prp());
  CColRefSet *pcrs = prprel->PcrsStat();
  auto colids = pcrs->ExtractColIds();

  IStatistics *stats = CProjectStatsProcessor::CalcProjStats(mp, dynamic_cast<CStatistics *>(child_stats), colids,
                                                             phmuldatum, colidToColrefMapForNDVExpr);

  return stats;
}

// EOF
