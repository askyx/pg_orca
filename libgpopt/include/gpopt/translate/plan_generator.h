#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gpopt/base/CColRef.h"
#include "gpopt/translate/CDXLTranslateContext.h"
#include "naucrates/md/CMDName.h"

class Plan;
class List;
class Node;
class Expr;
class Var;
class Const;
class TargetEntry;

namespace gpnaucrates {
class IDatum;
}

namespace gpopt {

using plan_node_id_t = uint32_t;

class CExpression;
class CMDAccessor;
class CTableDescriptor;
class CColRefSet;

struct PlanResult {
  Plan *plan;
  List *rtable;
  List *relationOids;
};

struct TranslateContextBaseTable {
  uint64_t rel_oid;
  uint32_t rte_index;
  std::unordered_map<uint32_t, int> colid_to_attno_map;
};

/**
 * Plan Generator for generating plans from Operators
 */
class PlanGenerator {
 public:
  PlanGenerator(CMemoryPool *m_mp, CMDAccessor *catalog);

  PlanResult *GeneratePlan(CExpression *pexpr, CColRefArray *colref_array, gpmd::CMDNameArray *names);

 private:
  struct PlanGeneratorContext {
    CExpression *expr{nullptr};
    CColRefArray *out_cols{nullptr};
    gpmd::CMDNameArray *names{nullptr};
    CColRefSet *upper_cols{nullptr};
    CExpression *filter{nullptr};
    CExpression *target{nullptr};
    CDXLTranslateContext *translate_ctxt{nullptr};
  };

  Plan *GeneratePlanInternal(PlanGeneratorContext *ctx);

  Plan *GenerateTVFPlan(PlanGeneratorContext *ctx);
  Plan *GenerateAggPlan(PlanGeneratorContext *ctx);
  Plan *GenerateHashPlan(PlanGeneratorContext *ctx);
  Plan *GenerateSortPlan(PlanGeneratorContext *ctx);
  Plan *GenerateLimitPlan(PlanGeneratorContext *ctx);
  Plan *GenerateNLJoinPlan(PlanGeneratorContext *ctx);
  Plan *GenerateAppendPlan(PlanGeneratorContext *ctx);
  Plan *GenerateFilterPlan(PlanGeneratorContext *ctx);
  Plan *GenerateSeqScanPlan(PlanGeneratorContext *ctx);
  Plan *GenerateHashJoinPlan(PlanGeneratorContext *ctx);
  Plan *GenerateIndexScanPlan(PlanGeneratorContext *ctx);
  Plan *GenerateMaterializePlan(PlanGeneratorContext *ctx);
  Plan *GenerateComputeScalarPlan(PlanGeneratorContext *ctx);
  Plan *GenerateConstTableGetPlan(PlanGeneratorContext *ctx);
  Plan *GenerateCorrelatedNLJoinPlan(PlanGeneratorContext *ctx);

  plan_node_id_t GetNextPlanNodeID() { return plan_id_counter_++; }

  /**
   * The required output property. Note that we have previously enforced
   * properties so this is fulfilled by the current operator
   */

  uint32_t ProcessDXLTblDescr(const CTableDescriptor *table_descr, const CColRefArray *colref,
                              TranslateContextBaseTable &base_ctx);

  void ApplyPlanStats(Plan *plan, CExpression *node);

  List *GeneratePlanTargetList(const CExpression *pexprProjList, const CColRefSet *pcrsOutput,
                               CColRefArray *colref_array);
  List *GeneratePlanTargetList(const CExpression *pexprProjList, const CColRefSet *pcrsOutput);

  List *GeneratePlanTargetList(uint32_t varno, const CColRefSet *pcrsOutput, CColRefArray *colref_array,
                               bool base_table = false);

  Expr *TransExpr(CExpression *expr);
  List *TransExprList(CExpression *expr);

  Expr *BuildSubplans(CExpression *expr, CColRefSet *outer_refs);

  Expr *PdxlnExistentialSubplan(CColRefArray *pdrgpcrInner, CExpression *expr, CColRefSet *outer_refs);

  Var *CreateVar(CColRef *colref);
  Const *CreateConstFromItem(gpnaucrates::IDatum *item);

  auto GetChildTarget(uint32_t colid) {
    const TargetEntry *target = nullptr;

    if (target = child_ctx_[0]->GetTargetEntry(colid); target)
      return std::make_tuple(target, OUTER_VAR);
    else {
      if (child_ctx_.size() < 2)
        return std::make_tuple(target, INNER_VAR);

      target = child_ctx_[1]->GetTargetEntry(colid);
      return std::make_tuple(target, INNER_VAR);

      // TODO: more right?
    }
    return std::make_tuple(target, INNER_VAR);
  }

  CMDAccessor *catalog_;

  List *rtable_{nullptr};
  List *relationOids_{nullptr};

  /**
   * Plan node counter, used to generate plan node ids
   */
  plan_node_id_t plan_id_counter_{0};

  CMemoryPool *m_mp;

  TranslateContextBaseTable *translate_ctxt_base_table_{nullptr};

  CDXLTranslateContext *output_context_{nullptr};
  std::vector<CDXLTranslateContext *> child_ctx_;
};

}  // namespace gpopt
