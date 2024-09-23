//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		COptimizer.h
//
//	@doc:
//		Optimizer class, entry point for query optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_COptimizer_H
#define GPOPT_COptimizer_H

#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/search/CSearchStage.h"
#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"

class Plan;

namespace gpdxl {
class CDXLNode;
}

namespace gpmd {
class IMDProvider;
}

using namespace gpos;
using namespace gpdxl;

namespace gpopt {
// forward declarations
class ICostModel;
class COptimizerConfig;
class CQueryContext;
class CEnumeratorConfig;

//---------------------------------------------------------------------------
//	@class:
//		COptimizer
//
//	@doc:
//		Optimizer class, entry point for query optimization
//
//---------------------------------------------------------------------------
class COptimizer {
 private:
  // handle exception after finalizing minidump
  static void HandleExceptionAfterFinalizingMinidump(CException &ex);

  // optimize query in the given query context
  static CExpression *PexprOptimize(CMemoryPool *mp, CQueryContext *pqc, CSearchStageArray *search_stage_array);

  // translate an optimizer expression into a DXL tree
  static CDXLNode *CreateDXLNode(CMemoryPool *mp, CMDAccessor *md_accessor, CExpression *pexpr,
                                 CColRefArray *colref_array, CMDNameArray *pdrgpmdname);

  // helper function to print query expression
  static void PrintQuery(CMemoryPool *mp, CExpression *pexprTranslated, CQueryContext *pqc);

  // helper function to print query plan
  static void PrintPlan(CMemoryPool *mp, CExpression *pexprPlan);

  // print query or plan tree
  static void PrintQueryOrPlan(CMemoryPool *mp, CExpression *pexpr, CQueryContext *pqc = nullptr);

  // Check for a plan with CTE, if both CTEProducer and CTEConsumer are executed on the same locality.
  static void CheckCTEConsistency(CMemoryPool *mp);

 public:
  // main optimizer function
  static void *PdxlnOptimize(CMemoryPool *mp,
                             CMDAccessor *md_accessor,  // MD accessor
                             const CDXLNode *query,
                             const CDXLNodeArray *query_output_dxlnode_array,  // required output columns
                             const CDXLNodeArray *cte_producers,
                             IConstExprEvaluator *pceeval,           // constant expression evaluator
                             CSearchStageArray *search_stage_array,  // search strategy
                             COptimizerConfig *optimizer_config      // optimizer configurations
  );
};  // class COptimizer
}  // namespace gpopt

#endif  // !GPOPT_COptimizer_H

// EOF
