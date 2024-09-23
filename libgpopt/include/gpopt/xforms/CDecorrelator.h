//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDecorrelator.h
//
//	@doc:
//		Decorrelation processor
//---------------------------------------------------------------------------
#ifndef GPOPT_CDecorrelator_H
#define GPOPT_CDecorrelator_H

#include "gpopt/operators/CExpression.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDecorrelator
//
//	@doc:
//		Helper class for extracting correlated expressions
//
//---------------------------------------------------------------------------
class CDecorrelator {
 private:
  // definition of operator processor
  using FnProcessor = bool(CMemoryPool *, CExpression *, bool, CExpression **, CExpressionArray *, CColRefSet *);

  //---------------------------------------------------------------------------
  //	@struct:
  //		SOperatorProcessor
  //
  //	@doc:
  //		Mapping of operator to a processor function
  //
  //---------------------------------------------------------------------------
  struct SOperatorProcessor {
    // scalar operator id
    COperator::EOperatorId m_eopid;

    // pointer to handler function
    FnProcessor *m_pfnp;

  };  // struct SOperatorHandler

  // helper to check if correlations below join are valid to be pulled-up
  static bool FPullableCorrelations(CMemoryPool *mp, CExpression *pexpr, CExpressionArray *pdrgpexpr,
                                    CExpressionArray *pdrgpexprCorrelations);

  // check if scalar operator can be delayed
  static bool FDelayableScalarOp(CExpression *pexprScalar);

  // check if scalar expression can be lifted
  static bool FDelayable(CExpression *pexprLogical, CExpression *pexprScalar, bool fEqualityOnly);

  // switch function for all operators
  static bool FProcessOperator(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly,
                               CExpression **ppexprDecorrelated, CExpressionArray *pdrgpexprCorrelations,
                               CColRefSet *outerRefsToRemove);

  // processor for predicates
  static bool FProcessPredicate(CMemoryPool *mp, CExpression *pexprLogical, CExpression *pexprScalar,
                                bool fEqualityOnly, CExpression **ppexprDecorrelated,
                                CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

  // processor for select operators
  static bool FProcessSelect(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly, CExpression **ppexprDecorrelated,
                             CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

  // processor for aggregates
  static bool FProcessGbAgg(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly, CExpression **ppexprDecorrelated,
                            CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

  // processor for joins (inner/n-ary)
  static bool FProcessJoin(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly, CExpression **ppexprDecorrelated,
                           CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

  // processor for projects
  static bool FProcessProject(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly, CExpression **ppexprDecorrelated,
                              CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

  // processor for assert
  static bool FProcessAssert(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly, CExpression **ppexprDecorrelated,
                             CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

  // processor for MaxOneRow
  static bool FProcessMaxOneRow(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly,
                                CExpression **ppexprDecorrelated, CExpressionArray *pdrgpexprCorrelations,
                                CColRefSet *outerRefsToRemove);

  // processor for limits
  static bool FProcessLimit(CMemoryPool *mp, CExpression *pexpr, bool fEqualityOnly, CExpression **ppexprDecorrelated,
                            CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

 public:
  // private dtor
  virtual ~CDecorrelator() = delete;

  // private ctor
  CDecorrelator() = delete;

  CDecorrelator(const CDecorrelator &) = delete;

  // main handler
  static bool FProcess(CMemoryPool *mp, CExpression *pexprOrig, bool fEqualityOnly, CExpression **ppexprDecorrelated,
                       CExpressionArray *pdrgpexprCorrelations, CColRefSet *outerRefsToRemove);

};  // class CDecorrelator

}  // namespace gpopt

#endif  // !GPOPT_CDecorrelator_H

// EOF
