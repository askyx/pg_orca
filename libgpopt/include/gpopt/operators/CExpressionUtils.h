//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CExpressionUtils.h
//
//	@doc:
//		Utility routines for transforming expressions
//
//	@owner:
//		,
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CExpressionUtils_H
#define GPOPT_CExpressionUtils_H

#include "gpopt/operators/CExpression.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

// fwd declarations
class CExpression;

//---------------------------------------------------------------------------
//	@class:
//		CExpressionUtils
//
//	@doc:
//		Utility routines for transforming expressions
//
//---------------------------------------------------------------------------
class CExpressionUtils {
 private:
  // unnest a given expression's child and append unnested nodes to given array
  static void UnnestChild(CMemoryPool *mp, CExpression *pexpr, uint32_t UlChildIndex, bool fAnd, bool fOr,
                          bool fNotChildren, CExpressionArray *pdrgpexpr);

  // append the unnested children of given expression to given array
  static void AppendChildren(CMemoryPool *mp, CExpression *pexpr, CExpressionArray *pdrgpexpr);

  // return an array of expression children after being unnested
  static CExpressionArray *PdrgpexprUnnestChildren(CMemoryPool *mp, CExpression *pexpr);

  // push not expression one level down the given expression
  static CExpression *PexprPushNotOneLevel(CMemoryPool *mp, CExpression *pexpr);

 public:
  // remove duplicate AND/OR children
  static CExpression *PexprDedupChildren(CMemoryPool *mp, CExpression *pexpr);

  // unnest AND/OR/NOT predicates
  static CExpression *PexprUnnest(CMemoryPool *mp, CExpression *pexpr);

  // get constraints property from LogicalSelect operator with EXISTS/ANY subquery
  static CPropConstraint *GetPropConstraintFromSubquery(CMemoryPool *mp, CExpression *pexpr);
};
}  // namespace gpopt

#endif  // !GPOPT_CExpressionUtils_H

// EOF
