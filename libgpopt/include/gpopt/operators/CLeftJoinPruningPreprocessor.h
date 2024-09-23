//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLeftJoinPruningPreprocessor.h
//
//	@doc:
//		Preprocessing routines of left join pruning
//---------------------------------------------------------------------------

#ifndef GPOPT_CLeftJoinPruningPreprocessor_H
#define GPOPT_CLeftJoinPruningPreprocessor_H

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpos/base.h"

namespace gpopt {
class CLeftJoinPruningPreprocessor {
 private:
  static CExpression *PexprJoinPruningScalarSubquery(CMemoryPool *mp, CExpression *pexprScalar);

  static void ComputeOutputColumns(const CExpression *pexpr, const CColRefSet *derived_output_columns,
                                   CColRefSet *output_columns, CColRefSet *childs_output_columns,
                                   const CColRefSet *pcrsOutput);

  static CExpression *JoinPruningTreeTraversal(CMemoryPool *mp, const CExpression *pexpr, CExpressionArray *pdrgpexpr,
                                               const CColRefSet *childs_output_columns);

  static CExpression *PexprCheckLeftOuterJoinPruningConditions(CMemoryPool *mp, CExpression *pexprNew,
                                                               CColRefSet *output_columns);

  static bool CheckJoinPruningCondOnInnerRel(const CExpression *pexprNew, CColRefSet *output_columns);

  static bool CheckJoinPruningCondOnJoinCond(CMemoryPool *mp, const CExpression *pexprNew, CColRefSet *result);

  static bool CheckAndCondInJoinCond(CMemoryPool *mp, const CExpression *join_cond, const CColRefSet *inner_unique_keys,
                                     CColRefSet *result, const CColRefSet *outer_rel_columns);

  static void CheckUniqueKeyInJoinCond(CColRefSet *inner_columns, const CColRefSet *usedColumns, CColRefSet *result,
                                       const CColRefSet *inner_unique_keys, const CColRefSet *outer_rel_columns);

  static bool CheckForFullUniqueKeySetInInnerRel(CMemoryPool *mp, const CExpression *pexprNew,
                                                 const CColRefSet *result);

 public:
  CLeftJoinPruningPreprocessor(const CLeftJoinPruningPreprocessor &) = delete;

  // Main Driver
  static CExpression *PexprPreprocess(CMemoryPool *mp, CExpression *pexpr, const CColRefSet *pcrsOutput);
};  // class CLeftJoinPruningPreprocessor
}  // namespace gpopt

#endif
