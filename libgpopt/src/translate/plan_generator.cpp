#include "gpopt/translate/plan_generator.h"

#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/exception.h"
#include "gpopt/gpdbwrappers.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalConstTableGet.h"
#include "gpopt/operators/CPhysicalCorrelatedLeftOuterNLJoin.h"
#include "gpopt/operators/CPhysicalHashAgg.h"
#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalNLJoin.h"
#include "gpopt/operators/CPhysicalScalarAgg.h"
#include "gpopt/operators/CPhysicalSort.h"
#include "gpopt/operators/CPhysicalStreamAgg.h"
#include "gpopt/operators/CPhysicalTVF.h"
#include "gpopt/operators/CPhysicalTableScan.h"
#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarArray.h"
#include "gpopt/operators/CScalarCast.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarCoerceViaIO.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarIsDistinctFrom.h"
#include "gpopt/operators/CScalarOp.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/base/CDatumBoolGPDB.h"
#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumInt2GPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/base/CDatumOidGPDB.h"
#include "naucrates/exception.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDScalarOp.h"

extern "C" {
#include <postgres.h>

#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/plannodes.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/tlist.h>
#include <parser/parse_agg.h>
#include <utils/datum.h>
#include <utils/palloc.h>
}

namespace gpopt {

PlanGenerator::PlanGenerator(CMemoryPool *m_mp, CMDAccessor *catalog) : catalog_(catalog), m_mp(m_mp) {}

void FixTragetName(Plan *plan, gpmd::CMDNameArray *names) {
  auto *target_list = plan->targetlist;
  uint32_t idx = 0;
  foreach_node(TargetEntry, te, target_list) {
    pfree(te->resname);
    te->resname = CTranslatorUtils::CreateMultiByteCharStringFromWCString((*names)[idx++]->GetMDName()->GetBuffer());
  }
}

PlanResult *PlanGenerator::GeneratePlan(CExpression *pexpr, CColRefArray *colref_array, gpmd::CMDNameArray *names) {
  CDXLTranslateContext tt_ctx{false, nullptr};
  PlanGeneratorContext ctx{
      .expr = pexpr,
      .out_cols = colref_array,
      .names = names,
      .translate_ctxt = &tt_ctx,
  };
  auto *plan = GeneratePlanInternal(&ctx);
  return new PlanResult{.plan = plan, .rtable = rtable_, .relationOids = relationOids_};
}

Plan *PlanGenerator::GeneratePlanInternal(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  switch (expr->Pop()->Eopid()) {
    case COperator::EopPhysicalTableScan:
      return GenerateSeqScanPlan(ctx);

    case COperator::EopPhysicalConstTableGet:
      return GenerateConstTableGetPlan(ctx);

    case COperator::EopPhysicalComputeScalar:
      return GenerateComputeScalarPlan(ctx);

    case COperator::EopPhysicalFilter:
      return GenerateFilterPlan(ctx);

    case COperator::EopPhysicalTVF:
      return GenerateTVFPlan(ctx);

    case COperator::EopPhysicalLimit:
      return GenerateLimitPlan(ctx);

    case COperator::EopPhysicalIndexScan:
      return GenerateIndexScanPlan(ctx);

    case COperator::EopPhysicalSort:
      return GenerateSortPlan(ctx);

    case COperator::EopPhysicalScalarAgg:
    case COperator::EopPhysicalHashAgg:
    case COperator::EopPhysicalStreamAgg:
      return GenerateAggPlan(ctx);

    case COperator::EopPhysicalSerialUnionAll:
    case COperator::EopPhysicalParallelUnionAll:
      return GenerateAppendPlan(ctx);

    case COperator::EopPhysicalSpool:
      return GenerateMaterializePlan(ctx);

    case COperator::EopPhysicalInnerNLJoin:
    case COperator::EopPhysicalInnerIndexNLJoin:
    case COperator::EopPhysicalLeftOuterIndexNLJoin:
    case COperator::EopPhysicalLeftOuterNLJoin:
    case COperator::EopPhysicalLeftSemiNLJoin:
    case COperator::EopPhysicalLeftAntiSemiNLJoin:
    case COperator::EopPhysicalLeftAntiSemiNLJoinNotIn:
      return GenerateNLJoinPlan(ctx);

    case COperator::EopPhysicalInnerHashJoin:
    case COperator::EopPhysicalLeftOuterHashJoin:
    case COperator::EopPhysicalLeftSemiHashJoin:
    case COperator::EopPhysicalLeftAntiSemiHashJoin:
    case COperator::EopPhysicalLeftAntiSemiHashJoinNotIn:
    case COperator::EopPhysicalRightOuterHashJoin:
    case COperator::EopPhysicalFullHashJoin:
      return GenerateHashJoinPlan(ctx);

    case COperator::EopPhysicalCorrelatedInnerNLJoin:
    case COperator::EopPhysicalCorrelatedLeftOuterNLJoin:
    case COperator::EopPhysicalCorrelatedLeftSemiNLJoin:
    case COperator::EopPhysicalCorrelatedInLeftSemiNLJoin:
    case COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin:
    case COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin:
      return GenerateCorrelatedNLJoinPlan(ctx);

    default:
      return nullptr;
      GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, expr->Pop()->SzId());
  }

  return nullptr;
}

static SubLinkType Edxlsubplantype(CExpression *expr) {
  switch (expr->Pop()->Eopid()) {
    case COperator::EopPhysicalCorrelatedLeftOuterNLJoin: {
      COperator::EOperatorId eopidSubq = CPhysicalCorrelatedLeftOuterNLJoin::PopConvert(expr->Pop())->EopidOriginSubq();
      switch (eopidSubq) {
        case COperator::EopScalarSubquery:
          return EXPR_SUBLINK;

        case COperator::EopScalarSubqueryAll:
          return ALL_SUBLINK;

        case COperator::EopScalarSubqueryAny:
          return ANY_SUBLINK;

        case COperator::EopScalarSubqueryExists:
          return EXISTS_SUBLINK;

          // case COperator::EopScalarSubqueryNotExists:
          //   return EdxlSubPlanTypeNotExists;

        default:
          GPOS_ASSERT(!"Unexpected origin subquery in correlated left outer join");
      }
    }

    case COperator::EopPhysicalCorrelatedInnerNLJoin:
      return EXPR_SUBLINK;

    case COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin:
      return ALL_SUBLINK;

    case COperator::EopPhysicalCorrelatedInLeftSemiNLJoin:
      return ANY_SUBLINK;

    case COperator::EopPhysicalCorrelatedLeftSemiNLJoin:
      return EXISTS_SUBLINK;

      // case COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin:
      //   return EdxlSubPlanTypeNotExists;

    default:
      GPOS_ASSERT(!"Unexpected correlated join");
  }
}

Expr *PlanGenerator::PdxlnExistentialSubplan(CColRefArray *pdrgpcrInner, CExpression *expr, CColRefSet *outer_refs) {
  SubPlan *subplan = makeNode(SubPlan);

  subplan->subLinkType = Edxlsubplantype(expr);

  return (Expr *)subplan;
}

Expr *PlanGenerator::BuildSubplans(CExpression *expr, CColRefSet *outer_refs) {
  CColRefArray *pdrgpcrInner = CPhysicalNLJoin::PopConvert(expr->Pop())->PdrgPcrInner();
  GPOS_ASSERT(nullptr != pdrgpcrInner);
  switch (expr->Pop()->Eopid()) {
      // case COperator::EopPhysicalCorrelatedLeftOuterNLJoin:
      //   BuildSubplansForCorrelatedLOJ(pexprCorrelatedNLJoin, dxl_colref_array, ppdxlnScalar, pulNonGatherMotions,
      //   pfDML); return;

      // case COperator::EopPhysicalCorrelatedInnerNLJoin:
      //   BuildScalarSubplans(pdrgpcrInner, pexprInner, dxl_colref_array, pulNonGatherMotions, pfDML);

      //   // now translate the scalar - references to the inner child will be
      //   // replaced by the subplan
      //   *ppdxlnScalar = PdxlnScalar(pexprScalar);
      //   return;

      // case COperator::EopPhysicalCorrelatedInLeftSemiNLJoin:
      // case COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin:
      //   pdxlnSubPlan =
      //       PdxlnQuantifiedSubplan(pdrgpcrInner, pexprCorrelatedNLJoin, dxl_colref_array, pulNonGatherMotions,
      //       pfDML);
      //   pdxlnSubPlan->AddRef();
      //   *ppdxlnScalar = pdxlnSubPlan;
      //   return;

    case COperator::EopPhysicalCorrelatedLeftSemiNLJoin:
    case COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin:
      return PdxlnExistentialSubplan(pdrgpcrInner, expr, outer_refs);

    default:
      GPOS_ASSERT(!"Unsupported correlated join");
  }
}

Plan *PlanGenerator::GenerateCorrelatedNLJoinPlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  CExpression *pexprOuterChild = (*expr)[0];
  CExpression *pexprInnerChild = (*expr)[1];
  CExpression *pexprScalar = (*expr)[2];

  CColRefSet *outer_refs = pexprInnerChild->DeriveOuterReferences();

  COperator::EOperatorId op_id = expr->Pop()->Eopid();

  Expr *pdxlnCond = nullptr;

  if (CUtils::FScalarConstTrue(pexprScalar) && (COperator::EopPhysicalCorrelatedInnerNLJoin == op_id ||
                                                COperator::EopPhysicalCorrelatedInLeftSemiNLJoin == op_id)) {
    // TODO
  } else {
    pdxlnCond = BuildSubplans(expr, outer_refs);
  }
  switch (pexprOuterChild->Pop()->Eopid()) {
    case COperator::EopPhysicalTableScan: {
      CDXLTranslateContext r_ctx{false, ctx->translate_ctxt->GetColIdToParamIdMap()};
      PlanGeneratorContext left_ctx{
          .expr = expr,
          .out_cols = ctx->out_cols,
          .translate_ctxt = &r_ctx,
      };

      auto *plan = GeneratePlanInternal(&left_ctx);

      plan->qual = lappend(plan->qual, pdxlnCond);

      return plan;
    }

      // case COperator::EopPhysicalFilter: {
      // return PdxlnResultFromNLJoinOuter(pexprOuterChild, pdxlnCond, colref_array, pulNonGatherMotions, pfDML,
      // dxl_properties);
      // }

    default: {
      // create a result node over outer child
      // return PdxlnResult(pexprOuterChild, colref_array, pulNonGatherMotions, pfDML, pdxlnCond, dxl_properties);
      return nullptr;
    }
  }

  return nullptr;
}

Plan *PlanGenerator::GenerateHashPlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  Hash *hash = makeNode(Hash);
  Plan *plan = &(hash->plan);
  plan->plan_node_id = GetNextPlanNodeID();

  CDXLTranslateContext r_ctx{false, ctx->translate_ctxt->GetColIdToParamIdMap()};

  PlanGeneratorContext left_ctx{
      .expr = expr,
      .out_cols = ctx->out_cols,
      .translate_ctxt = &r_ctx,
  };

  plan->lefttree = GeneratePlanInternal(&left_ctx);

  child_ctx_.push_back(&r_ctx);
  output_context_ = ctx->translate_ctxt;

  plan->targetlist = GeneratePlanTargetList(OUTER_VAR, expr->Prpp()->PcrsRequired(), ctx->out_cols);

  child_ctx_.clear();

  return plan;
}

static bool SetHashKeysVarnoWalker(Node *node, void *context) {
  if (nullptr == node) {
    return false;
  }

  if (IsA(node, Var) && ((Var *)node)->varno == INNER_VAR) {
    ((Var *)node)->varno = OUTER_VAR;
    return false;
  }

  return expression_tree_walker(node, SetHashKeysVarnoWalker, context);
}

Plan *PlanGenerator::GenerateHashJoinPlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  // CPhysicalHashJoin *popHJ = CPhysicalHashJoin::PopConvert(expr->Pop());

  HashJoin *hashjoin = makeNode(HashJoin);
  Join *join = &(hashjoin->join);
  Plan *plan = &(join->plan);
  plan->plan_node_id = GetNextPlanNodeID();

  CExpression *pexprOuterChild = (*expr)[0];
  CExpression *pexprInnerChild = (*expr)[1];
  CExpression *pexprScalar = (*expr)[2];

  CExpressionArray *pdrgpexprPredicates = CPredicateUtils::PdrgpexprConjuncts(m_mp, pexprScalar);
  CExpressionArray *pdrgpexprRemainingPredicates = GPOS_NEW(m_mp) CExpressionArray(m_mp);

  std::vector<CExpression *> hash_clauses;

  const uint32_t size = pdrgpexprPredicates->Size();
  for (uint32_t ul = 0; ul < size; ul++) {
    CExpression *pexprPred = (*pdrgpexprPredicates)[ul];
    if (CPhysicalJoin::FHashJoinCompatible(pexprPred, pexprOuterChild, pexprInnerChild)) {
      CExpression *pexprPredOuter;
      CExpression *pexprPredInner;
      IMDId *mdid_scop;
      CPhysicalJoin::AlignJoinKeyOuterInner(pexprPred, pexprOuterChild, pexprInnerChild, &pexprPredOuter,
                                            &pexprPredInner, &mdid_scop);

      pexprPredOuter->AddRef();
      pexprPredInner->AddRef();
      // create hash join predicate based on conjunct type
      if (CPredicateUtils::IsEqualityOp(pexprPred)) {
        pexprPred = CUtils::PexprScalarCmp(m_mp, pexprPredOuter, pexprPredInner, mdid_scop);
      } else {
        GPOS_ASSERT(CPredicateUtils::FINDF(pexprPred));
        pexprPred = CUtils::PexprINDF(m_mp, pexprPredOuter, pexprPredInner, mdid_scop);
      }

      hash_clauses.push_back(pexprPred);

    } else {
      pdrgpexprRemainingPredicates->Append(pexprPred);
    }
  }

  CDXLTranslateContext l_ctx{false, ctx->translate_ctxt->GetColIdToParamIdMap()};
  CDXLTranslateContext r_ctx{false, ctx->translate_ctxt->GetColIdToParamIdMap()};

  PlanGeneratorContext left_ctx{
      .expr = (*expr)[0],
      .out_cols = ctx->out_cols,
      .translate_ctxt = &l_ctx,
  };

  PlanGeneratorContext right_ctx{
      .expr = (*expr)[1],
      .translate_ctxt = &r_ctx,
  };

  auto *lefttree = GeneratePlanInternal(&left_ctx);
  auto *righttree = GenerateHashPlan(&right_ctx);

  child_ctx_.push_back(&l_ctx);
  child_ctx_.push_back(&r_ctx);
  output_context_ = ctx->translate_ctxt;

  if (0 < pdrgpexprRemainingPredicates->Size()) {
    CExpression *pexprJoinCond = CPredicateUtils::PexprConjunction(m_mp, pdrgpexprRemainingPredicates);
    if (auto *qual = TransExpr(pexprJoinCond); qual)
      join->joinqual = lappend(join->joinqual, qual);
    pexprJoinCond->Release();
  } else {
    pdrgpexprRemainingPredicates->Release();
  }

  for (auto *expr : hash_clauses) {
    if (expr->Pop()->Eopid() == COperator::EopScalarBoolOp)
      expr = (*expr)[0];

    // GP do this
    auto *qual = TransExpr(expr);
    if (IsA(qual, DistinctExpr))
      qual->type = T_OpExpr;

    hashjoin->hashclauses = lappend(hashjoin->hashclauses, qual);
  }

  for (auto *expr : hash_clauses)
    expr->Release();

  plan->targetlist = GeneratePlanTargetList(OUTER_VAR, expr->Prpp()->PcrsRequired(), ctx->out_cols);

  List *hashoperators = NIL;
  List *hashcollations = NIL;
  List *outer_hashkeys = NIL;
  List *inner_hashkeys = NIL;
  ListCell *lc;
  foreach (lc, hashjoin->hashclauses) {
    OpExpr *hclause = lfirst_node(OpExpr, lc);

    hashoperators = gpdb::LAppendOid(hashoperators, hclause->opno);
    hashcollations = gpdb::LAppendOid(hashcollations, hclause->inputcollid);
    outer_hashkeys = gpdb::LAppend(outer_hashkeys, linitial(hclause->args));
    inner_hashkeys = gpdb::LAppend(inner_hashkeys, gpdb::CopyObject(lsecond(hclause->args)));
  }

  hashjoin->hashoperators = hashoperators;
  hashjoin->hashcollations = hashcollations;

  hashjoin->hashkeys = outer_hashkeys;

  SetHashKeysVarnoWalker((Node *)inner_hashkeys, nullptr);

  ((Hash *)righttree)->hashkeys = inner_hashkeys;

  plan->lefttree = lefttree;
  plan->righttree = righttree;

  ApplyPlanStats(plan, ctx->expr);
  child_ctx_.clear();

  pdrgpexprPredicates->Release();

  return plan;
}

Plan *PlanGenerator::GenerateMaterializePlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  Material *materialize = makeNode(Material);
  Plan *plan = &(materialize->plan);
  plan->plan_node_id = GetNextPlanNodeID();

  CDXLTranslateContext l_ctx{false, ctx->translate_ctxt->GetColIdToParamIdMap()};

  PlanGeneratorContext left_ctx{
      .expr = (*expr)[0],
      .out_cols = ctx->out_cols,
      .translate_ctxt = &l_ctx,
  };
  plan->lefttree = GeneratePlanInternal(&left_ctx);
  child_ctx_.push_back(&l_ctx);
  output_context_ = ctx->translate_ctxt;

  plan->targetlist = GeneratePlanTargetList(OUTER_VAR, expr->Prpp()->PcrsRequired(), ctx->out_cols);

  ApplyPlanStats(plan, ctx->expr);
  child_ctx_.clear();

  return plan;
}

Plan *PlanGenerator::GenerateNLJoinPlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  CPhysical *pop = CPhysical::PopConvert(expr->Pop());
  NestLoop *nested_loop = makeNode(NestLoop);
  Join *join = &(nested_loop->join);
  Plan *plan = &(join->plan);
  plan->plan_node_id = GetNextPlanNodeID();

  switch (pop->Eopid()) {
    case COperator::EopPhysicalInnerNLJoin:
      join->jointype = JOIN_INNER;
      break;

    case COperator::EopPhysicalInnerIndexNLJoin:
      join->jointype = JOIN_INNER;
      break;

    case COperator::EopPhysicalLeftOuterIndexNLJoin:
      join->jointype = JOIN_LEFT;
      break;

    case COperator::EopPhysicalLeftOuterNLJoin:
      join->jointype = JOIN_LEFT;
      break;

    case COperator::EopPhysicalLeftSemiNLJoin:
      join->jointype = JOIN_SEMI;
      break;

    case COperator::EopPhysicalLeftAntiSemiNLJoin:
      join->jointype = JOIN_ANTI;
      break;

      // case COperator::EopPhysicalLeftAntiSemiNLJoinNotIn:
      //   join_type = EdxljtLeftAntiSemijoinNotIn;
      //   break;

    default:
      GPOS_ASSERT(!"Invalid join type");
  }

  Plan *left_plan = nullptr;
  Plan *right_plan = nullptr;
  CDXLTranslateContext l_ctx{false, ctx->translate_ctxt->GetColIdToParamIdMap()};
  CDXLTranslateContext r_ctx{false, ctx->translate_ctxt->GetColIdToParamIdMap()};

  PlanGeneratorContext left_ctx{
      .expr = (*expr)[0],
      .translate_ctxt = &l_ctx,
  };
  left_plan = GeneratePlanInternal(&left_ctx);

  PlanGeneratorContext right_ctx{
      .expr = (*expr)[1],
      .translate_ctxt = &r_ctx,
  };
  right_plan = GeneratePlanInternal(&right_ctx);
  child_ctx_.push_back(&l_ctx);
  child_ctx_.push_back(&r_ctx);

  if (auto *qual = TransExpr((*expr)[2]); qual)
    join->joinqual = lappend(join->joinqual, qual);

  plan->lefttree = left_plan;
  plan->righttree = right_plan;

  output_context_ = ctx->translate_ctxt;

  plan->targetlist = GeneratePlanTargetList(OUTER_VAR, expr->Prpp()->PcrsRequired(), ctx->out_cols);

  ApplyPlanStats(plan, ctx->expr);
  child_ctx_.clear();

  return plan;
}

Plan *PlanGenerator::GenerateAppendPlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  CPhysicalUnionAll *popUnionAll = CPhysicalUnionAll::PopConvert(expr->Pop());

  Append *append = makeNode(Append);

  Plan *plan = &(append->plan);
  plan->plan_node_id = GetNextPlanNodeID();

  CColRefSet *reqdCols = expr->Prpp()->PcrsRequired();

  CColRefArray *pdrgpcrOutputAll = popUnionAll->PdrgpcrOutput();
  CColRefArray *reqd_col_array = GPOS_NEW(m_mp) CColRefArray(m_mp);
  uint32_t num_total_cols = pdrgpcrOutputAll->Size();
  for (uint32_t c = 0; c < num_total_cols; c++) {
    if (reqdCols->FMember((*pdrgpcrOutputAll)[c])) {
      reqd_col_array->Append((*pdrgpcrOutputAll)[c]);
    }
  }
  ULongPtrArray *reqd_col_positions = pdrgpcrOutputAll->IndexesOfSubsequence(reqd_col_array);
  CColRefArray *requiredOutput = pdrgpcrOutputAll->CreateReducedArray(reqd_col_positions);
  reqd_col_array->Release();

  CColRefSet *pcrsOutput = GPOS_NEW(m_mp) CColRefSet(m_mp);
  pcrsOutput->Include(reqdCols);

  CColRef2dArray *pdrgpdrgpcrInput = popUnionAll->PdrgpdrgpcrInput();
  const uint32_t length = expr->Arity();
  CDXLTranslateContext tt_ctx{false, nullptr};

  for (uint32_t ul = 0; ul < length; ul++) {
    CColRefArray *pdrgpcrInput = (*pdrgpdrgpcrInput)[ul];
    CColRefArray *requiredInput = pdrgpcrInput->CreateReducedArray(reqd_col_positions);
    CExpression *pexprChild = (*expr)[ul];

    PlanGeneratorContext left_ctx{
        .expr = pexprChild,
        .out_cols = requiredInput,
        .translate_ctxt = &tt_ctx,
    };

    auto *left = GeneratePlanInternal(&left_ctx);
    append->appendplans = lappend(append->appendplans, left);
    requiredInput->Release();
  }
  reqd_col_positions->Release();
  child_ctx_.push_back(&tt_ctx);
  output_context_ = ctx->translate_ctxt;

  plan->targetlist = GeneratePlanTargetList(OUTER_VAR, pcrsOutput, requiredOutput);
  pcrsOutput->Release();
  requiredOutput->Release();

  ApplyPlanStats(plan, ctx->expr);
  child_ctx_.clear();
  return plan;
}

Plan *PlanGenerator::GenerateAggPlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  Agg *agg = makeNode(Agg);

  CDXLTranslateContext tt_ctx{false, nullptr};

  Plan *plan = &(agg->plan);

  plan->plan_node_id = GetNextPlanNodeID();

  PlanGeneratorContext left_ctx{
      .expr = (*expr)[0],
      .translate_ctxt = &tt_ctx,
  };

  auto *left = GeneratePlanInternal(&left_ctx);
  child_ctx_.push_back(left_ctx.translate_ctxt);
  output_context_ = ctx->translate_ctxt;

  plan->targetlist = GeneratePlanTargetList((*expr)[1], expr->Prpp()->PcrsRequired(), ctx->out_cols);

  plan->lefttree = left;

  int32_t aggsplit = 0;
  foreach_node(TargetEntry, te, plan->targetlist) {
    if (IsA(te->expr, Aggref)) {
      Aggref *aggref = (Aggref *)te->expr;

      aggsplit |= aggref->aggsplit;
    }
  }
  agg->aggsplit = (AggSplit)aggsplit;

  CPhysicalAgg *popAgg = nullptr;

  switch (expr->Pop()->Eopid()) {
    case COperator::EopPhysicalStreamAgg: {
      agg->aggstrategy = AGG_SORTED;
      popAgg = CPhysicalStreamAgg::PopConvert(expr->Pop());
      break;
    }
    case COperator::EopPhysicalHashAgg: {
      popAgg = CPhysicalHashAgg::PopConvert(expr->Pop());
      agg->aggstrategy = AGG_HASHED;
      break;
    }
    case COperator::EopPhysicalScalarAgg: {
      popAgg = CPhysicalScalarAgg::PopConvert(expr->Pop());
      agg->aggstrategy = AGG_PLAIN;
      break;
    }
    default: {
      return nullptr;  // to silence the compiler
    }
  }

  const CColRefArray *pdrgpcrGroupingCols = popAgg->PdrgpcrGroupingCols();
  (void)pdrgpcrGroupingCols;

  // group
  agg->numCols = pdrgpcrGroupingCols->Size();
  if (agg->numCols > 0) {
    agg->grpColIdx = (AttrNumber *)palloc(agg->numCols * sizeof(AttrNumber));
    agg->grpOperators = (Oid *)palloc(agg->numCols * sizeof(Oid));
    agg->grpCollations = (Oid *)palloc(agg->numCols * sizeof(Oid));
  } else {
    agg->grpColIdx = nullptr;
    agg->grpOperators = nullptr;
    agg->grpCollations = nullptr;
  }

  for (int i = 0; i < agg->numCols; i++) {
    const CColRef *colref = (*pdrgpcrGroupingCols)[i];
    auto [target_entry, _] = GetChildTarget(colref->Id());
    agg->grpColIdx[i] = target_entry->resno;
    Oid typeId = exprType((Node *)target_entry->expr);
    agg->grpOperators[i] = gpdb::GetEqualityOp(typeId);
    agg->grpCollations[i] = gpdb::ExprCollation((Node *)target_entry->expr);
  }

  agg->numGroups = 1;

  ApplyPlanStats(plan, ctx->expr);

  child_ctx_.clear();

  return plan;
}

Plan *PlanGenerator::GenerateSortPlan(PlanGeneratorContext *ctx) {
  auto *expr = ctx->expr;
  CPhysicalSort *popSort = CPhysicalSort::PopConvert(expr->Pop());

  CExpression *pexprChild = (*expr)[0];

  CDXLTranslateContext tt_ctx{false, nullptr};

  PlanGeneratorContext left_ctx{
      .expr = pexprChild,
      .out_cols = ctx->out_cols,
      .translate_ctxt = &tt_ctx,
  };

  Sort *sort = makeNode(Sort);
  Plan *plan = &(sort->plan);
  plan->plan_node_id = GetNextPlanNodeID();

  auto *left = GeneratePlanInternal(&left_ctx);
  plan->lefttree = left;
  child_ctx_.push_back(left_ctx.translate_ctxt);
  output_context_ = ctx->translate_ctxt;

  plan->targetlist = GeneratePlanTargetList(OUTER_VAR, ctx->expr->Prpp()->PcrsRequired(), ctx->out_cols);

  auto *pos = popSort->Pos();
  sort->numCols = pos->UlSortColumns();
  sort->sortColIdx = (AttrNumber *)palloc(sort->numCols * sizeof(AttrNumber));
  sort->sortOperators = (Oid *)palloc(sort->numCols * sizeof(Oid));
  sort->collations = (Oid *)palloc(sort->numCols * sizeof(Oid));
  sort->nullsFirst = (bool *)palloc(sort->numCols * sizeof(bool));

  for (uint32_t ul = 0; ul < pos->UlSortColumns(); ul++) {
    const CColRef *colref = pos->Pcr(ul);
    auto [target_entry, _] = GetChildTarget(colref->Id());
    sort->sortColIdx[ul] = target_entry->resno;
    sort->sortOperators[ul] = CMDIdGPDB::CastMdid(pos->GetMdIdSortOp(ul))->Oid();
    sort->nullsFirst[ul] = (pos->Ent(ul) == COrderSpec::EntFirst) ? true : false;
    sort->collations[ul] = gpdb::ExprCollation((Node *)target_entry->expr);
  }

  ApplyPlanStats(plan, ctx->expr);
  child_ctx_.clear();
  return plan;
}

Plan *PlanGenerator::GenerateIndexScanPlan(PlanGeneratorContext *ctx) {
  CPhysicalIndexScan *popIs = CPhysicalIndexScan::PopConvert(ctx->expr->Pop());
  IndexScan *index_scan = makeNode(IndexScan);

  CMDIdGPDB *mdid_index = CMDIdGPDB::CastMdid(popIs->Pindexdesc()->MDId());
  // const IMDIndex *md_index = catalog_->RetrieveIndex(mdid_index);

  TranslateContextBaseTable base_table_context;
  translate_ctxt_base_table_ = &base_table_context;

  output_context_ = ctx->translate_ctxt;

  index_scan->scan.scanrelid = ProcessDXLTblDescr(popIs->Ptabdesc(), popIs->PdrgpcrOutput(), base_table_context);
  index_scan->indexid = mdid_index->Oid();

  index_scan->indexorderdir = CTranslatorUtils::GetScanDirection(
      (popIs->IndexScanDirection() == EForwardScan) ? EdxlisdForward : EdxlisdBackward);

  CExpression *index_cond = (*ctx->expr)[0];
  CExpression *filter = (2 == ctx->expr->Arity()) ? (*ctx->expr)[1] : nullptr;

  Plan *plan = &(index_scan->scan.plan);

  plan->targetlist =
      GeneratePlanTargetList(index_scan->scan.scanrelid, ctx->expr->Prpp()->PcrsRequired(), ctx->out_cols);

  // orca do this , check later
  if (auto *qual = TransExpr(filter); qual)
    plan->qual = lappend(plan->qual, qual);

  if (auto *index_qual = TransExpr(index_cond); index_qual) {
    index_scan->indexqual = lappend(index_scan->indexqual, index_qual);
    index_scan->indexqualorig = (List *)copyObject(index_scan->indexqual);
  }

  ApplyPlanStats(plan, ctx->expr);
  translate_ctxt_base_table_ = nullptr;

  return plan;
}

Plan *PlanGenerator::GenerateLimitPlan(PlanGeneratorContext *ctx) {
  CExpression *pexprChild = (*ctx->expr)[0];
  CExpression *pexprOffset = (*ctx->expr)[1];
  CExpression *pexprCount = (*ctx->expr)[2];

  CDXLTranslateContext tt_ctx{false, nullptr};

  PlanGeneratorContext left_ctx{
      .expr = pexprChild,
      .out_cols = ctx->out_cols,
      .translate_ctxt = &tt_ctx,
  };

  // todo: support limit with ties
  Limit *limit = makeNode(Limit);
  Plan *plan = &(limit->plan);
  plan->plan_node_id = GetNextPlanNodeID();

  auto *left = GeneratePlanInternal(&left_ctx);
  child_ctx_.push_back(left_ctx.translate_ctxt);
  output_context_ = ctx->translate_ctxt;

  auto *left_targetlist = GeneratePlanTargetList(OUTER_VAR, ctx->expr->Prpp()->PcrsRequired(), ctx->out_cols);

  plan->targetlist = left_targetlist;

  plan->lefttree = left;
  limit->limitCount = (Node *)TransExpr(pexprCount);
  limit->limitOffset = (Node *)TransExpr(pexprOffset);

  ApplyPlanStats(plan, ctx->expr);
  child_ctx_.clear();

  return plan;
}

Plan *PlanGenerator::GenerateComputeScalarPlan(PlanGeneratorContext *ctx) {
  auto *pexprProjList = (*ctx->expr)[1];

  PlanGeneratorContext child_ctx{
      .expr = (*ctx->expr)[0],
      .target = pexprProjList,
      .translate_ctxt = ctx->translate_ctxt,
  };

  auto *plan = GeneratePlanInternal(&child_ctx);
  ApplyPlanStats(plan, ctx->expr);
  return plan;
}

Plan *PlanGenerator::GenerateConstTableGetPlan(PlanGeneratorContext *ctx) {
  CPhysicalConstTableGet *popCTG = CPhysicalConstTableGet::PopConvert(ctx->expr->Pop());
  CColRefArray *pdrgpcrCTGOutput = popCTG->PdrgpcrOutput();
  IDatum2dArray *pdrgpdrgdatum = popCTG->Pdrgpdrgpdatum();

  TranslateContextBaseTable base_table_context;
  translate_ctxt_base_table_ = &base_table_context;
  output_context_ = ctx->translate_ctxt;

  const uint32_t ulRows = pdrgpdrgdatum->Size();

  {
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind = ulRows > 1 ? RTE_VALUES : RTE_RESULT;
    Alias *alias = makeNode(Alias);
    alias->aliasname = ulRows > 1 ? pstrdup("*VALUES*") : pstrdup("*RESULT*");
    for (uint32_t ul = 0; ul < pdrgpcrCTGOutput->Size(); ul++) {
      CColRef *colref = (*pdrgpcrCTGOutput)[ul];
      char *col_name_char_array =
          CTranslatorUtils::CreateMultiByteCharStringFromWCString(colref->Name().Pstr()->GetBuffer());
      alias->colnames = lappend(alias->colnames, makeString(col_name_char_array));
      base_table_context.colid_to_attno_map[colref->Id()] = ul + 1;
    }
    rte->eref = alias;
    rtable_ = lappend(rtable_, rte);
  }

  base_table_context.rte_index = list_length(rtable_);

  Plan *plan = nullptr;
  if (ulRows > 1) {
    ValuesScan *value_scan = makeNode(ValuesScan);
    plan = &value_scan->scan.plan;

    value_scan->scan.scanrelid = list_length(rtable_);
    plan->targetlist = GeneratePlanTargetList(list_length(rtable_), ctx->expr->DeriveOutputColumns(), pdrgpcrCTGOutput);

    for (uint32_t ulTuplePos = 0; ulTuplePos < ulRows; ulTuplePos++) {
      IDatumArray *pdrgpdatum = (*pdrgpdrgdatum)[ulTuplePos];
      const uint32_t num_cols = pdrgpdatum->Size();

      List *values_list = NIL;
      for (uint32_t ulColPos = 0; ulColPos < num_cols; ulColPos++) {
        IDatum *datum = (*pdrgpdatum)[ulColPos];

        values_list = lappend(values_list, CreateConstFromItem(datum));
      }
      value_scan->values_lists = lappend(value_scan->values_lists, values_list);
    }

  } else {
    Result *result = makeNode(Result);
    plan = &result->plan;
    // TODO: select 1;   values(1);
    if (ctx->target) {
      auto *target_exprs = ctx->target;
      int resno = list_length(plan->targetlist) + 1;
      for (uint32_t ul = 0; ul < target_exprs->Arity(); ul++) {
        CExpression *pexprProjElem = (*target_exprs)[ul];

        const CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprProjElem->Pop());

        char *name =
            CTranslatorUtils::CreateMultiByteCharStringFromWCString(popScPrEl->Pcr()->Name().Pstr()->GetBuffer());
        auto *target_entry = makeTargetEntry(TransExpr((*pexprProjElem)[0]), resno++, name, false);
        plan->targetlist = lappend(plan->targetlist, target_entry);
        output_context_->InsertMapping(popScPrEl->Pcr()->Id(), target_entry);
      }
    } else {
      for (uint32_t ulTuplePos = 0; ulTuplePos < ulRows; ulTuplePos++) {
        IDatumArray *pdrgpdatum = (*pdrgpdrgdatum)[ulTuplePos];
        const uint32_t num_cols = pdrgpdatum->Size();

        for (uint32_t ulColPos = 0; ulColPos < num_cols; ulColPos++) {
          IDatum *datum = (*pdrgpdatum)[ulColPos];
          CColRef *colref = (*pdrgpcrCTGOutput)[ulColPos];

          TargetEntry *target_entry = makeNode(TargetEntry);
          target_entry->expr = (Expr *)CreateConstFromItem(datum);
          target_entry->resname =
              CTranslatorUtils::CreateMultiByteCharStringFromWCString(colref->Name().Pstr()->GetBuffer());
          target_entry->resno = (AttrNumber)(ulColPos + 1);
          plan->targetlist = lappend(plan->targetlist, target_entry);
          output_context_->InsertMapping(colref->Id(), target_entry);
        }
      }
    }
  }
  if (ctx->filter) {
    if (auto *qual = TransExpr(ctx->filter); qual)
      plan->qual = lappend(plan->qual, qual);
  }

  plan->plan_node_id = GetNextPlanNodeID();

  ApplyPlanStats(plan, ctx->expr);

  return plan;
}

Plan *PlanGenerator::GenerateFilterPlan(PlanGeneratorContext *ctx) {
  auto *pexprFilter = ctx->expr;
  CExpression *pexprRelational = (*pexprFilter)[0];
  CExpression *pexprScalar = (*pexprFilter)[1];

  switch (pexprRelational->Pop()->Eopid()) {
    case COperator::EopPhysicalTableScan:
    case COperator::EopPhysicalForeignScan: {
      PlanGeneratorContext scan_ctx{
          .expr = pexprRelational,
          .upper_cols = pexprFilter->Prpp()->PcrsRequired(),
          .filter = pexprScalar,
          .target = ctx->target,
          .translate_ctxt = ctx->translate_ctxt,
      };
      auto *plan = GeneratePlanInternal(&scan_ctx);
      ApplyPlanStats(plan, ctx->expr);
      return plan;
    }

    case COperator::EopPhysicalBitmapTableScan: {
    }
    case COperator::EopPhysicalIndexOnlyScan:
    case COperator::EopPhysicalIndexScan: {
    }

    default: {
      PlanGeneratorContext scan_ctx{
          .expr = pexprRelational,
          .filter = pexprScalar,
          .target = ctx->target,
          .translate_ctxt = ctx->translate_ctxt,
      };
      auto *plan = GeneratePlanInternal(&scan_ctx);
      ApplyPlanStats(plan, ctx->expr);
      return plan;
    }
  }

  return nullptr;
}

Plan *PlanGenerator::GenerateTVFPlan(PlanGeneratorContext *ctx) {
  CPhysicalTVF *popTVF = CPhysicalTVF::PopConvert(ctx->expr->Pop());

  CColRefSet *pcrsOutput = popTVF->DeriveOutputColumns();

  TranslateContextBaseTable base_table_context;
  translate_ctxt_base_table_ = &base_table_context;

  output_context_ = ctx->translate_ctxt;

  FunctionScan *func_scan = makeNode(FunctionScan);
  Plan *plan = &(func_scan->scan.plan);

  {
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_FUNCTION;

    Alias *alias = makeNode(Alias);
    alias->colnames = NIL;
    alias->aliasname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(popTVF->Pstr()->GetBuffer());
    CColRefSetIter crsi(*pcrsOutput);
    while (crsi.Advance()) {
      CColRef *colref = crsi.Pcr();
      char *col_name_char_array =
          CTranslatorUtils::CreateMultiByteCharStringFromWCString(colref->Name().Pstr()->GetBuffer());
      alias->colnames = lappend(alias->colnames, makeString(col_name_char_array));

      base_table_context.colid_to_attno_map[colref->Id()] = list_length(alias->colnames);
    }
    rte->alias = alias;
    rte->eref = alias;
    rtable_ = lappend(rtable_, rte);
    base_table_context.rte_index = list_length(rtable_);
  }

  plan->plan_node_id = GetNextPlanNodeID();
  ApplyPlanStats(plan, ctx->expr);

  RangeTblFunction *rtfunc = makeNode(RangeTblFunction);
  FuncExpr *func_expr = makeNode(FuncExpr);
  func_expr->funcid = CMDIdGPDB::CastMdid(popTVF->FuncMdId())->Oid();
  func_expr->funcretset = gpdb::GetFuncRetset(func_expr->funcid);
  // this is a function call, as opposed to a cast
  func_expr->funcformat = COERCE_EXPLICIT_CALL;
  func_expr->funcresulttype = CMDIdGPDB::CastMdid(popTVF->ReturnTypeMdId())->Oid();

  for (uint32_t ul = 0; ul < ctx->expr->Arity(); ul++) {
    CExpression *pexprChild = (*ctx->expr)[ul];
    Expr *child_expr = TransExpr(pexprChild);
    func_expr->args = lappend(func_expr->args, child_expr);
  }
  func_expr->inputcollid = gpdb::ExprCollation((Node *)func_expr->args);
  func_expr->funccollid = gpdb::TypeCollation(func_expr->funcresulttype);
  rtfunc->funcexpr = (Node *)func_expr;
  rtfunc->funccolcount = pcrsOutput->Size();

  func_scan->functions = list_make1(rtfunc);

  func_scan->scan.scanrelid = list_length(rtable_);
  plan->targetlist = nullptr;
  if (ctx->target) {
    auto *target_exprs = ctx->target;
    int resno = list_length(plan->targetlist) + 1;
    for (uint32_t ul = 0; ul < target_exprs->Arity(); ul++) {
      CExpression *pexprProjElem = (*target_exprs)[ul];

      const CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprProjElem->Pop());

      char *name =
          CTranslatorUtils::CreateMultiByteCharStringFromWCString(popScPrEl->Pcr()->Name().Pstr()->GetBuffer());
      auto *target_entry = makeTargetEntry(TransExpr((*pexprProjElem)[0]), resno++, name, false);
      plan->targetlist = lappend(plan->targetlist, target_entry);
      output_context_->InsertMapping(popScPrEl->Pcr()->Id(), target_entry);
    }
  } else
    plan->targetlist = GeneratePlanTargetList(func_scan->scan.scanrelid, pcrsOutput, nullptr);

  if (ctx->filter) {
    if (auto *qual = TransExpr(ctx->filter); qual)
      plan->qual = lappend(plan->qual, qual);
  }

  translate_ctxt_base_table_ = nullptr;
  return plan;
}

Plan *PlanGenerator::GenerateSeqScanPlan(PlanGeneratorContext *ctx) {
  auto *popTblScan = CPhysicalTableScan::PopConvert(ctx->expr->Pop());

  TranslateContextBaseTable base_table_context;
  translate_ctxt_base_table_ = &base_table_context;

  output_context_ = ctx->translate_ctxt;

  SeqScan *seq_scan = makeNode(SeqScan);
  seq_scan->scan.scanrelid =
      ProcessDXLTblDescr(popTblScan->Ptabdesc(), popTblScan->PdrgpcrOutput(), base_table_context);

  auto *cols = ctx->expr->Prpp()->PcrsRequired();
  if (ctx->upper_cols)
    cols = ctx->upper_cols;

  auto *plan = &seq_scan->scan.plan;
  plan->plan_node_id = GetNextPlanNodeID();
  plan->targetlist = GeneratePlanTargetList(seq_scan->scan.scanrelid, cols, ctx->out_cols, true);

  if (ctx->target) {
    auto *target_exprs = ctx->target;
    int resno = list_length(plan->targetlist) + 1;
    for (uint32_t ul = 0; ul < target_exprs->Arity(); ul++) {
      CExpression *pexprProjElem = (*target_exprs)[ul];

      const CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprProjElem->Pop());

      char *name =
          CTranslatorUtils::CreateMultiByteCharStringFromWCString(popScPrEl->Pcr()->Name().Pstr()->GetBuffer());
      auto *target_entry = makeTargetEntry(TransExpr((*pexprProjElem)[0]), resno++, name, false);
      plan->targetlist = lappend(plan->targetlist, target_entry);
      output_context_->InsertMapping(popScPrEl->Pcr()->Id(), target_entry);
    }
  }

  if (ctx->filter) {
    if (auto *qual = TransExpr(ctx->filter); qual)
      plan->qual = lappend(plan->qual, qual);
  }

  ApplyPlanStats(plan, ctx->expr);
  translate_ctxt_base_table_ = nullptr;

  return plan;
}

Var *PlanGenerator::CreateVar(CColRef *colref) {
  Index varno = 0;
  AttrNumber attno = 0;

  Index varnosyn = 0;
  AttrNumber varattnosyn = 0;

  auto vartype = CMDIdGPDB::CastMdid(colref->RetrieveType()->MDId())->Oid();
  auto vartypmod = colref->TypeModifier();
  auto varcollid = gpdb::TypeCollation(vartype);
  auto varlevelsup = 0;

  const uint32_t colid = colref->Id();
  if (translate_ctxt_base_table_) {
    varno = translate_ctxt_base_table_->rte_index;
    attno = translate_ctxt_base_table_->colid_to_attno_map.contains(colid)
                ? (AttrNumber)translate_ctxt_base_table_->colid_to_attno_map.at(colid)
                : 0;
    varnosyn = varno;
    varattnosyn = varnosyn;
  }

  if (varno == 0 && !child_ctx_.empty()) {
    auto [target, target_varrno] = GetChildTarget(colid);
    if (target)
      varno = target_varrno;
    else
      GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, colid);

    attno = target->resno;
    if (IsA(target->expr, Var)) {
      Var *var = (Var *)target->expr;
      varnosyn = var->varnosyn;
      varattnosyn = var->varattnosyn;
    } else {
      varnosyn = varno;
      varattnosyn = attno;
    }
  }

  auto *var = makeVar(varno, attno, vartype, vartypmod, varcollid, varlevelsup);
  var->varnosyn = varnosyn;
  var->varattnosyn = varattnosyn;
  return var;
}

List *PlanGenerator::GeneratePlanTargetList(uint32_t varno, const CColRefSet *pcrsOutput, CColRefArray *colref_array,
                                            bool base_table) {
  List *target_list = NIL;

  std::vector<CColRef *> colrefs;

  if (colref_array) {
    CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);

    for (uint32_t ul = 0; ul < colref_array->Size(); ul++) {
      CColRef *colref = (*colref_array)[ul];

      colrefs.push_back(colref);

      pcrs->Include(colref);
    }

    // add the remaining required columns
    CColRefSetIter crsi(*pcrsOutput);
    while (crsi.Advance()) {
      CColRef *colref = crsi.Pcr();

      if (!pcrs->FMember(colref)) {
        colrefs.push_back(colref);

        pcrs->Include(colref);
      }
    }
    pcrs->Release();
  } else {
    // no order specified
    CColRefSetIter crsi(*pcrsOutput);
    while (crsi.Advance()) {
      CColRef *colref = crsi.Pcr();
      colrefs.push_back(colref);
    }
  }

  uint32_t resno = 1;
  for (auto *colref : colrefs) {
    auto *var = CreateVar(colref);
    char *name = CTranslatorUtils::CreateMultiByteCharStringFromWCString(colref->Name().Pstr()->GetBuffer());
    auto *target_entry = makeTargetEntry((Expr *)var, resno++, name, false);
    if (translate_ctxt_base_table_) {
      target_entry->resorigtbl = translate_ctxt_base_table_->rel_oid;
      target_entry->resorigcol = var->varattno;
    } else {
      if (auto [pteOriginal, _] = GetChildTarget(colref->Id()); pteOriginal) {
        target_entry->resorigtbl = pteOriginal->resorigtbl;
        target_entry->resorigcol = pteOriginal->resorigcol;
      }
    }

    target_list = lappend(target_list, target_entry);
    output_context_->InsertMapping(colref->Id(), target_entry);
  }

  return target_list;
}

void PlanGenerator::ApplyPlanStats(Plan *plan, CExpression *node) {
  const auto *stats = node->Pstats();

  auto *xx = node->Prpp()->PcrsRequired();
  auto colids = xx->ExtractColIds();

  plan->startup_cost = 0;
  plan->total_cost = node->Cost().Get();
  plan->plan_rows = stats->Rows().Get();
  plan->plan_width = stats->Width(colids).Get();
}

uint32_t PlanGenerator::ProcessDXLTblDescr(const CTableDescriptor *ptabdesc, const CColRefArray *colref,
                                           TranslateContextBaseTable &base_ctx) {
  auto oid = CMDIdGPDB::CastMdid(ptabdesc->MDId())->Oid();

  // create a new RTE (and it's alias) and store it at context rtable list
  RangeTblEntry *rte = makeNode(RangeTblEntry);
  rte->rtekind = RTE_RELATION;
  rte->relid = oid;
  rte->rellockmode = ptabdesc->LockMode();

  Alias *alias = makeNode(Alias);
  alias->colnames = NIL;

  // get table alias
  alias->aliasname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(ptabdesc->Name().Pstr()->GetBuffer());

  auto arity = ptabdesc->ColumnCount();
  for (uint32_t ul = 0; ul < arity; ++ul) {
    const auto *pcd = ptabdesc->Pcoldesc(ul);
    auto *colr = (*colref)[ul];

    base_ctx.colid_to_attno_map[colr->Id()] = pcd->AttrNum();

    alias->colnames =
        lappend(alias->colnames,
                makeString(CTranslatorUtils::CreateMultiByteCharStringFromWCString(pcd->Name().Pstr()->GetBuffer())));
  }

  rte->eref = alias;
  rte->alias = alias;

  rtable_ = lappend(rtable_, rte);
  relationOids_ = lappend_oid(relationOids_, oid);

  base_ctx.rel_oid = oid;
  base_ctx.rte_index = list_length(rtable_);

  return base_ctx.rte_index;
}

Const *PlanGenerator::CreateConstFromItem(gpnaucrates::IDatum *datum) {
  int constlen = -1;
  Datum constvalue = 0;
  bool constisnull = datum->IsNull();
  bool constbyval;

  auto datum_type = datum->GetDatumType();
  const IMDType *type = catalog_->RetrieveType(datum->MDId());

  switch (datum_type) {
    case IMDType::EtiGeneric: {
      constlen = type->Length();
      constbyval = type->IsPassedByValue();
      if (constisnull)
        constvalue = 0;
      else if (constbyval)
        memcpy(&constvalue, datum->GetByteArrayValue(), sizeof(Datum));
      else {
        Datum val = PointerGetDatum(datum->GetByteArrayValue());
        Size length = datumGetSize(val, false, constlen);

        char *str = (char *)palloc(length + 1);
        memcpy(str, datum->GetByteArrayValue(), length);
        str[length] = '\0';
        constvalue = PointerGetDatum(str);
      }
    } break;
    case IMDType::EtiInt2: {
      auto *cc = dynamic_cast<CDatumInt2GPDB *>(datum);
      constbyval = true;
      constlen = sizeof(int16);

      if (constisnull)
        constvalue = (Datum)0;
      else
        constvalue = Int16GetDatum(cc->Value());
    } break;
    case IMDType::EtiInt4: {
      auto *cc = dynamic_cast<CDatumInt4GPDB *>(datum);
      constbyval = true;
      constlen = sizeof(int32);

      if (constisnull)
        constvalue = (Datum)0;
      else
        constvalue = Int32GetDatum(cc->Value());
    } break;
    case IMDType::EtiInt8: {
      auto *cc = dynamic_cast<CDatumInt8GPDB *>(datum);
      constbyval = FLOAT8PASSBYVAL;
      constlen = sizeof(int64);

      if (constisnull)
        constvalue = (Datum)0;
      else
        constvalue = DatumGetInt64(cc->Value());
    } break;
    case IMDType::EtiBool: {
      auto *cc = dynamic_cast<CDatumBoolGPDB *>(datum);
      constbyval = true;
      constlen = sizeof(bool);

      if (constisnull)
        constvalue = (Datum)0;
      else
        constvalue = BoolGetDatum(cc->GetValue());
    } break;
    case IMDType::EtiOid: {
      auto *cc = dynamic_cast<CDatumOidGPDB *>(datum);
      constbyval = true;
      constlen = sizeof(Oid);

      if (constisnull)
        constvalue = (Datum)0;
      else
        constvalue = DatumGetObjectId(cc->OidValue());
    } break;
  }
  Oid consttype = CMDIdGPDB::CastMdid(datum->MDId())->Oid();
  return makeConst(consttype, datum->TypeModifier(),
                   datum_type == IMDType::EtiGeneric ? gpdb::TypeCollation(consttype) : InvalidOid, constlen,
                   constvalue, constisnull, datum_type == IMDType::EtiGeneric ? type->IsPassedByValue() : true);
}

List *PlanGenerator::TransExprList(CExpression *expr) {
  if (!expr)
    return nullptr;

  List *result = NIL;
  for (uint32_t ul = 0; ul < expr->Arity(); ul++) {
    CExpression *pexprChild = (*expr)[ul];
    result = lappend(result, TransExpr(pexprChild));
  }
  return result;
}

Expr *PlanGenerator::TransExpr(CExpression *expr) {
  if (!expr)
    return nullptr;

  switch (expr->Pop()->Eopid()) {
    case COperator::EopScalarIdent: {
      CScalarIdent *popScId = CScalarIdent::PopConvert(expr->Pop());
      CColRef *colref = const_cast<CColRef *>(popScId->Pcr());

      return (Expr *)CreateVar(colref);
    }

    case COperator::EopScalarConst: {
      CScalarConst *popScConst = CScalarConst::PopConvert(expr->Pop());
      return (Expr *)CreateConstFromItem(popScConst->GetDatum());
    }

    case COperator::EopScalarOp: {
      CScalarOp *pscop = CScalarOp::PopConvert(expr->Pop());
      OpExpr *op_expr = makeNode(OpExpr);

      op_expr->opno = CMDIdGPDB::CastMdid(pscop->MdIdOp())->Oid();
      const IMDScalarOp *md_scalar_op = catalog_->RetrieveScOp(pscop->MdIdOp());
      op_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();
      op_expr->opresulttype = CMDIdGPDB::CastMdid(pscop->GetReturnTypeMdId())->Oid();
      const IMDFunction *md_func = catalog_->RetrieveFunc(md_scalar_op->FuncMdId());
      op_expr->opretset = md_func->ReturnsSet();

      for (uint32_t ul = 0; ul < expr->Arity(); ul++) {
        Expr *arg = TransExpr((*expr)[ul]);
        op_expr->args = lappend(op_expr->args, arg);
      }
      op_expr->inputcollid = gpdb::ExprCollation((Node *)op_expr->args);
      op_expr->opcollid = gpdb::TypeCollation(op_expr->opresulttype);
      return (Expr *)op_expr;
    }

    case COperator::EopScalarCmp: {
      CScalarCmp *popScCmp = CScalarCmp::PopConvert(expr->Pop());
      OpExpr *op_expr = makeNode(OpExpr);

      op_expr->opno = CMDIdGPDB::CastMdid(popScCmp->MdIdOp())->Oid();
      const IMDScalarOp *md_scalar_op = catalog_->RetrieveScOp(popScCmp->MdIdOp());
      op_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();
      op_expr->opresulttype = CMDIdGPDB::CastMdid(catalog_->PtMDType<IMDTypeBool>()->MDId())->Oid();
      op_expr->opretset = false;

      for (uint32_t ul = 0; ul < expr->Arity(); ul++) {
        Expr *arg = TransExpr((*expr)[ul]);
        op_expr->args = lappend(op_expr->args, arg);
      }
      op_expr->inputcollid = gpdb::ExprCollation((Node *)op_expr->args);
      op_expr->opcollid = gpdb::TypeCollation(op_expr->opresulttype);
      return (Expr *)op_expr;
    }

    case COperator::EopScalarArray: {
      CScalarArray *pop = CScalarArray::PopConvert(expr->Pop());
      ArrayExpr *array_expr = makeNode(ArrayExpr);
      array_expr->element_typeid = CMDIdGPDB::CastMdid(pop->PmdidElem())->Oid();
      array_expr->array_typeid = CMDIdGPDB::CastMdid(pop->PmdidArray())->Oid();
      array_expr->array_collid = gpdb::TypeCollation(array_expr->array_typeid);
      array_expr->multidims = pop->FMultiDimensional();
      const uint32_t arity = CUtils::UlScalarArrayArity(expr);
      for (uint32_t ul = 0; ul < arity; ul++) {
        CExpression *pexprChild = CUtils::PScalarArrayExprChildAt(m_mp, expr, ul);
        array_expr->elements = lappend(array_expr->elements, TransExpr(pexprChild));
        pexprChild->Release();
      }
      return (Expr *)eval_const_expressions(nullptr, (Node *)array_expr);
    }

    case COperator::EopScalarCoerceViaIO: {
      CScalarCoerceViaIO *popScCerce = CScalarCoerceViaIO::PopConvert(expr->Pop());
      CoerceViaIO *coerce = makeNode(CoerceViaIO);
      coerce->resulttype = CMDIdGPDB::CastMdid(popScCerce->MdidType())->Oid();
      coerce->location = popScCerce->Location();
      coerce->coerceformat = (CoercionForm)popScCerce->Ecf();
      coerce->resultcollid = gpdb::TypeCollation(coerce->resulttype);

      CExpression *pexprChild = (*expr)[0];
      coerce->arg = TransExpr(pexprChild);
      return (Expr *)coerce;
    }

    case COperator::EopScalarBoolOp: {
      CScalarBoolOp *popScBoolOp = CScalarBoolOp::PopConvert(expr->Pop());
      BoolExpr *scalar_bool_expr = makeNode(BoolExpr);
      scalar_bool_expr->boolop = (BoolExprType)popScBoolOp->Eboolop();

      for (uint32_t ul = 0; ul < expr->Arity(); ul++) {
        Expr *arg = TransExpr((*expr)[ul]);
        scalar_bool_expr->args = lappend(scalar_bool_expr->args, arg);
      }

      return (Expr *)scalar_bool_expr;
    }

    case COperator::EopScalarNullTest: {
      NullTest *null_test = makeNode(NullTest);
      null_test->nulltesttype = IS_NULL;
      null_test->arg = TransExpr((*expr)[0]);
      return (Expr *)null_test;
    }

    case COperator::EopScalarArrayCmp: {
      CScalarArrayCmp *pop = CScalarArrayCmp::PopConvert(expr->Pop());
      ScalarArrayOpExpr *array_op_expr = makeNode(ScalarArrayOpExpr);
      array_op_expr->opno = CMDIdGPDB::CastMdid(pop->MdIdOp())->Oid();
      const IMDScalarOp *md_scalar_op = catalog_->RetrieveScOp(pop->MdIdOp());
      array_op_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();

      array_op_expr->useOr = pop->Earrcmpt() == CScalarArrayCmp::EarrcmpAny ? true : false;

      for (uint32_t ul = 0; ul < expr->Arity(); ul++) {
        Expr *arg = TransExpr((*expr)[ul]);
        array_op_expr->args = lappend(array_op_expr->args, arg);
      }

      array_op_expr->inputcollid = gpdb::ExprCollation((Node *)array_op_expr->args);

      return (Expr *)array_op_expr;
    }

    case COperator::EopScalarCast: {
      CScalarCast *popScCast = CScalarCast::PopConvert(expr->Pop());
      CExpression *pexprChild = (*expr)[0];
      Expr *child_expr = TransExpr(pexprChild);
      if (IMDId::IsValid(popScCast->FuncMdId())) {
        FuncExpr *func_expr = makeNode(FuncExpr);
        func_expr->funcid = CMDIdGPDB::CastMdid(popScCast->FuncMdId())->Oid();
        const IMDFunction *pmdfunc = catalog_->RetrieveFunc(popScCast->FuncMdId());
        func_expr->funcretset = pmdfunc->ReturnsSet();

        func_expr->funcformat = COERCE_IMPLICIT_CAST;
        func_expr->funcresulttype = CMDIdGPDB::CastMdid(popScCast->MdidType())->Oid();

        func_expr->args = lappend(func_expr->args, child_expr);

        func_expr->inputcollid = gpdb::ExprCollation((Node *)func_expr->args);
        func_expr->funccollid = gpdb::TypeCollation(func_expr->funcresulttype);

        return (Expr *)func_expr;
      }
      RelabelType *relabel_type = makeNode(RelabelType);

      relabel_type->resulttype = CMDIdGPDB::CastMdid(popScCast->MdidType())->Oid();
      relabel_type->arg = child_expr;
      relabel_type->resulttypmod = -1;
      relabel_type->location = -1;
      relabel_type->relabelformat = COERCE_IMPLICIT_CAST;
      relabel_type->resultcollid = gpdb::ExprCollation((Node *)child_expr);

      return (Expr *)relabel_type;
    }

    case COperator::EopScalarAggFunc: {
      CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(expr->Pop());
      Aggref *aggref = makeNode(Aggref);
      aggref->aggfnoid = CMDIdGPDB::CastMdid(popScAggFunc->MDId())->Oid();
      aggref->aggdistinct = NIL;
      aggref->agglevelsup = 0;
      aggref->aggkind = 'n';
      aggref->location = -1;
      aggref->aggtranstype = InvalidOid;
      aggref->aggargtypes = NIL;

      IMDId *resolved_rettype = nullptr;
      if (popScAggFunc->FHasAmbiguousReturnType()) {
        resolved_rettype = popScAggFunc->MdidType();
      }

      EdxlAggrefStage edxlaggstage = EdxlaggstageNormal;

      if (popScAggFunc->FGlobal() && popScAggFunc->FSplit()) {
        edxlaggstage = EdxlaggstageFinal;
      } else if (EaggfuncstageIntermediate == popScAggFunc->Eaggfuncstage()) {
        edxlaggstage = EdxlaggstageIntermediate;
      } else if (!popScAggFunc->FGlobal()) {
        edxlaggstage = EdxlaggstagePartial;
      }

      CMDIdGPDB *agg_mdid = GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, aggref->aggfnoid);
      const IMDAggregate *pmdagg = catalog_->RetrieveAgg(agg_mdid);
      agg_mdid->Release();

      if (nullptr != resolved_rettype) {
        // use resolved type
        aggref->aggtype = CMDIdGPDB::CastMdid(resolved_rettype)->Oid();
      } else if (EdxlaggstageIntermediate == edxlaggstage || EdxlaggstagePartial == edxlaggstage) {
        aggref->aggtype = CMDIdGPDB::CastMdid(pmdagg->GetIntermediateResultTypeMdid())->Oid();
      } else {
        aggref->aggtype = CMDIdGPDB::CastMdid(pmdagg->GetResultTypeMdid())->Oid();
      }

      switch (edxlaggstage) {
        case EdxlaggstageNormal:
          aggref->aggsplit = AGGSPLIT_SIMPLE;
          break;
        case EdxlaggstagePartial:
          aggref->aggsplit = AGGSPLIT_INITIAL_SERIAL;
          break;
        case EdxlaggstageFinal:
          aggref->aggsplit = AGGSPLIT_FINAL_DESERIAL;
          break;
        default:
          return nullptr;
      }

      List *args = TransExprList((*expr)[EdxlscalaraggrefIndexArgs]);

      aggref->aggdirectargs = TransExprList((*expr)[EdxlscalaraggrefIndexDirectArgs]);

      aggref->aggorder = TransExprList((*expr)[EdxlscalaraggrefIndexAggOrder]);

      aggref->aggkind = [](EAggfuncKind kind) {
        switch (kind) {
          case EaggfunckindNormal:
            return 'n';
          case EaggfunckindOrderedSet:
            return 'o';
          case EaggfunckindHypothetical:
            return 'h';
        }
        return 'n';
      }(popScAggFunc->AggKind());

      ListCell *lc;
      int attno = 0;
      foreach (lc, args) {
        attno++;
        TargetEntry *new_target_entry = makeTargetEntry((Expr *)lfirst(lc), attno, nullptr, false);

        aggref->args = lappend(aggref->args, new_target_entry);
      }

      auto *aggtypes = popScAggFunc->GetArgTypes();
      for (uint32_t ul = 0; ul < aggtypes->Size(); ul++) {
        aggref->aggargtypes = gpdb::LAppendOid(aggref->aggargtypes, *(*aggtypes)[ul]);
      }

      Oid aggtranstype;
      Oid inputTypes[FUNC_MAX_ARGS];
      int numArguments;

      aggtranstype = gpdb::GetAggIntermediateResultType(aggref->aggfnoid);

      numArguments = get_aggregate_argtypes(aggref, inputTypes);

      aggtranstype = resolve_aggregate_transtype(aggref->aggfnoid, aggtranstype, inputTypes, numArguments);
      aggref->aggtranstype = aggtranstype;

      aggref->inputcollid = gpdb::ExprCollation((Node *)args);
      aggref->aggcollid = gpdb::TypeCollation(aggref->aggtype);

      return (Expr *)aggref;
    }

    case COperator::EopScalarIsDistinctFrom: {
      CScalarIsDistinctFrom *popScIDF = CScalarIsDistinctFrom::PopConvert(expr->Pop());
      DistinctExpr *dist_expr = makeNode(DistinctExpr);
      dist_expr->opno = CMDIdGPDB::CastMdid(popScIDF->MdIdOp())->Oid();
      const IMDScalarOp *md_scalar_op = catalog_->RetrieveScOp(popScIDF->MdIdOp());
      dist_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();
      dist_expr->opresulttype =
          CMDIdGPDB::CastMdid(catalog_->RetrieveFunc(md_scalar_op->FuncMdId())->GetResultTypeMdid())->Oid();

      dist_expr->args = TransExprList(expr);

      dist_expr->opcollid = gpdb::TypeCollation(dist_expr->opresulttype);
      dist_expr->inputcollid = gpdb::ExprCollation((Node *)dist_expr->args);

      return (Expr *)dist_expr;
    }

    case COperator::EopScalarFunc: {
      CScalarFunc *popScFunc = CScalarFunc::PopConvert(expr->Pop());
      FuncExpr *func_expr = makeNode(FuncExpr);

      IMDId *mdid_func = popScFunc->FuncMdId();
      const IMDFunction *pmdfunc = catalog_->RetrieveFunc(mdid_func);

      func_expr->funcid = CMDIdGPDB::CastMdid(popScFunc->FuncMdId())->Oid();
      func_expr->funcretset = pmdfunc->ReturnsSet();
      func_expr->funcformat = COERCE_EXPLICIT_CALL;
      func_expr->funcresulttype = CMDIdGPDB::CastMdid(popScFunc->MdidType())->Oid();
      func_expr->args = TransExprList(expr);
      func_expr->funcvariadic = popScFunc->IsFuncVariadic();

      // GPDB_91_MERGE_FIXME: collation
      func_expr->inputcollid = gpdb::ExprCollation((Node *)func_expr->args);
      func_expr->funccollid = gpdb::TypeCollation(func_expr->funcresulttype);

      return (Expr *)func_expr;
    }

    default: {
      auto type = expr->Pop()->Eopid();
      (void)type;
      GPOS_ASSERT(!"Invalid expr type");
    }
  }
  return nullptr;
}

List *PlanGenerator::GeneratePlanTargetList(const CExpression *pexprProjList, const CColRefSet *pcrsRequired) {
  CColRefSet *pcrsOutput = GPOS_NEW(m_mp) CColRefSet(m_mp, *pcrsRequired);
  List *target_list = NIL;

  uint32_t ul = 0;
  for (; nullptr != pexprProjList && ul < pexprProjList->Arity(); ul++) {
    CExpression *pexprProjElem = (*pexprProjList)[ul];

    const auto *popScPrEl = CScalarProjectElement::PopConvert(pexprProjElem->Pop())->Pcr();

    char *name = CTranslatorUtils::CreateMultiByteCharStringFromWCString(popScPrEl->Name().Pstr()->GetBuffer());

    auto *expr = TransExpr((*pexprProjElem)[0]);

    auto *target_entry = makeTargetEntry(expr, ul + 1, name, false);
    if (translate_ctxt_base_table_) {
      target_entry->resorigtbl = translate_ctxt_base_table_->rel_oid;
    } else {
      if (auto [pteOriginal, _] = GetChildTarget(popScPrEl->Id()); pteOriginal) {
        target_entry->resorigtbl = pteOriginal->resorigtbl;
        target_entry->resorigcol = pteOriginal->resorigcol;
      }
    }
    target_list = lappend(target_list, target_entry);
    output_context_->InsertMapping(popScPrEl->Id(), target_entry);
    pcrsOutput->Exclude(popScPrEl);
  }

  CColRefSetIter crsi(*pcrsOutput);
  while (crsi.Advance()) {
    CColRef *colref = crsi.Pcr();
    auto *expr = (Expr *)CreateVar(colref);
    char *name = CTranslatorUtils::CreateMultiByteCharStringFromWCString(colref->Name().Pstr()->GetBuffer());
    auto *target_entry = makeTargetEntry(expr, ul++, name, false);
    if (translate_ctxt_base_table_) {
      target_entry->resorigtbl = translate_ctxt_base_table_->rel_oid;
    } else {
      if (auto [pteOriginal, _] = GetChildTarget(colref->Id()); pteOriginal) {
        target_entry->resorigtbl = pteOriginal->resorigtbl;
        target_entry->resorigcol = pteOriginal->resorigcol;
      }
    }
    target_list = lappend(target_list, target_entry);
    output_context_->InsertMapping(colref->Id(), target_entry);
  }

  // cleanup
  pcrsOutput->Release();

  return target_list;
}

List *PlanGenerator::GeneratePlanTargetList(const CExpression *pexprProjList, const CColRefSet *pcrsRequired,
                                            CColRefArray *colref_array) {
  if (!colref_array)
    return GeneratePlanTargetList(pexprProjList, pcrsRequired);

  std::unordered_map<uint32_t, CExpression *> project_map;
  for (uint32_t ul = 0; nullptr != pexprProjList && ul < pexprProjList->Arity(); ul++) {
    CExpression *pexprProjElem = (*pexprProjList)[ul];

    const CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprProjElem->Pop());

    project_map[popScPrEl->Pcr()->Id()] = (*pexprProjElem)[0];
  }

  // add required columns to the project list
  CColRefArray *pdrgpcrCopy = GPOS_NEW(m_mp) CColRefArray(m_mp);
  pdrgpcrCopy->AppendArray(colref_array);
  CColRefSet *pcrsOutput = GPOS_NEW(m_mp) CColRefSet(m_mp);
  pcrsOutput->Include(colref_array);
  CColRefSetIter crsi(*pcrsRequired);
  while (crsi.Advance()) {
    CColRef *colref = crsi.Pcr();
    if (!pcrsOutput->FMember(colref)) {
      pdrgpcrCopy->Append(colref);
    }
  }

  List *target_list = NIL;
  const uint32_t num_cols = pdrgpcrCopy->Size();
  for (uint32_t ul = 0; ul < num_cols; ul++) {
    CColRef *colref = (*pdrgpcrCopy)[ul];
    uint32_t ulKey = colref->Id();

    Expr *expr = nullptr;
    if (project_map.count(ulKey))
      expr = (Expr *)TransExpr(project_map[ulKey]);
    else
      expr = (Expr *)CreateVar(colref);

    char *name = CTranslatorUtils::CreateMultiByteCharStringFromWCString(colref->Name().Pstr()->GetBuffer());

    auto *target_entry = makeTargetEntry(expr, ul + 1, name, false);

    if (translate_ctxt_base_table_) {
      target_entry->resorigtbl = translate_ctxt_base_table_->rel_oid;
    } else {
      if (auto [pteOriginal, _] = GetChildTarget(colref->Id()); pteOriginal) {
        target_entry->resorigtbl = pteOriginal->resorigtbl;
        target_entry->resorigcol = pteOriginal->resorigcol;
      }
    }

    target_list = lappend(target_list, target_entry);
    output_context_->InsertMapping(colref->Id(), target_entry);
  }

  // cleanup
  pdrgpcrCopy->Release();
  pcrsOutput->Release();

  return target_list;
}

}  // namespace gpopt
