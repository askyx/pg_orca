//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifySubquery.h
//
//	@doc:
//		Simplify existential/quantified subqueries by transforming
//		into count(*) subqueries
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSimplifySubquery_H
#define GPOPT_CXformSimplifySubquery_H

#include "gpopt/xforms/CXformExploration.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSimplifySubquery
//
//	@doc:
//		Simplify existential/quantified subqueries by transforming
//		into count(*) subqueries
//
//---------------------------------------------------------------------------
class CXformSimplifySubquery : public CXformExploration {
 private:
  // definition of simplification function
  using FnSimplify = bool(CMemoryPool *, CExpression *, CExpression **);

  // definition of matching function
  using FnMatch = bool(COperator *);

  // transform existential subqueries to count(*) subqueries
  static bool FSimplifyExistential(CMemoryPool *mp, CExpression *pexprScalar, CExpression **ppexprNewScalar);

  // transform quantified subqueries to count(*) subqueries
  static bool FSimplifyQuantified(CMemoryPool *mp, CExpression *pexprScalar, CExpression **ppexprNewScalar);

  // main driver, transform existential/quantified subqueries to count(*) subqueries
  static bool FSimplifySubqueryRecursive(CMemoryPool *mp, CExpression *pexprScalar, CExpression **ppexprNewScalar,
                                         FnSimplify *pfnsimplify, FnMatch *pfnmatch);

  static CExpression *FSimplifySubquery(CMemoryPool *mp, CExpression *pexprInput, FnSimplify *pfnsimplify,
                                        FnMatch *pfnmatch);

 public:
  CXformSimplifySubquery(const CXformSimplifySubquery &) = delete;

  // ctor
  explicit CXformSimplifySubquery(CExpression *pexprPattern);

  // dtor
  ~CXformSimplifySubquery() override = default;

  // compute xform promise for a given expression handle
  EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

  // actual transform
  void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const override;

};  // class CXformSimplifySubquery

}  // namespace gpopt

#endif  // !GPOPT_CXformSimplifySubquery_H

// EOF
