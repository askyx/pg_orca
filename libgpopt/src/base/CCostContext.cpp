//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCostContext.cpp
//
//	@doc:
//		Implementation of cost context
//---------------------------------------------------------------------------

#include "gpopt/base/CCostContext.h"

#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CDrvdPropCtxtRelational.h"
#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "gpopt/operators/CPhysicalSpool.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::CCostContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostContext::CCostContext(CMemoryPool *mp, COptimizationContext *poc, uint32_t ulOptReq, CGroupExpression *pgexpr)
    : m_mp(mp),
      m_cost(GPOPT_INVALID_COST),
      m_estate(estUncosted),
      m_pgexpr(pgexpr),
      m_pgexprForStats(nullptr),
      m_pdrgpoc(nullptr),
      m_pdpplan(nullptr),
      m_ulOptReq(ulOptReq),
      m_fPruned(false),
      m_pstats(nullptr),
      m_poc(poc) {
  GPOS_ASSERT(nullptr != poc);
  GPOS_ASSERT(nullptr != pgexpr);
  GPOS_ASSERT_IMP(pgexpr->Pop()->FPhysical(), ulOptReq < CPhysical::PopConvert(pgexpr->Pop())->UlOptRequests());

  if (!m_pgexpr->Pop()->FScalar() && !CPhysical::PopConvert(m_pgexpr->Pop())->FPassThruStats()) {
    CGroupExpression *pgexprForStats = m_pgexpr->Pgroup()->PgexprBestPromise(m_mp, m_pgexpr);
    if (nullptr != pgexprForStats) {
      pgexprForStats->AddRef();
      m_pgexprForStats = pgexprForStats;
    }
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::~CCostContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostContext::~CCostContext() {
  CRefCount::SafeRelease(m_pgexpr);
  CRefCount::SafeRelease(m_pgexprForStats);
  CRefCount::SafeRelease(m_poc);
  CRefCount::SafeRelease(m_pdrgpoc);
  CRefCount::SafeRelease(m_pdpplan);
  CRefCount::SafeRelease(m_pstats);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FOwnsStats
//
//	@doc:
//		Check if new stats are owned by this context
//
//---------------------------------------------------------------------------
bool CCostContext::FOwnsStats() const {
  GPOS_ASSERT(nullptr != m_pstats);

  // new stats are owned if context holds stats different from group stats
  return (m_pstats != m_pgexpr->Pgroup()->Pstats());
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FNeedsNewStats
//
//	@doc:
//		Check if we need to derive new stats for this context,
//		by default a cost context inherits stats from the owner group,
//		the only current exception is when part of the plan below cost
//		context is affected by partition elimination done by partition
//		selection in some other part of the plan
//
//---------------------------------------------------------------------------
bool CCostContext::FNeedsNewStats() const {
  COperator *pop = m_pgexpr->Pop();
  if (pop->FScalar()) {
    // return false if scalar operator
    return false;
  }

  if (!m_poc->Prpp()->Pepp()->PppsRequired()->ContainsAnyConsumers()) {
    // All partition selectors have been resolved at this level.
    // No need to use DPE stats for the common ancestor join and
    // nodes above it, that aren't affected by the partition selector.
    return false;
  }

  if (GPOS_FTRACE(EopttraceDeriveStatsForDPE) && CUtils::FPhysicalScan(pop) &&
      CPhysicalScan::PopConvert(pop)->FDynamicScan()) {
    // context is attached to a dynamic scan that went through
    // partition elimination in another part of the plan
    return true;
  }

  // we need to derive stats if any child has modified stats
  bool fDeriveStats = false;
  const uint32_t arity = Pdrgpoc()->Size();
  for (uint32_t ul = 0; !fDeriveStats && ul < arity; ul++) {
    COptimizationContext *pocChild = (*Pdrgpoc())[ul];
    CCostContext *pccChild = pocChild->PccBest();
    GPOS_ASSERT(nullptr != pccChild);

    fDeriveStats = pccChild->FOwnsStats();
  }

  return fDeriveStats;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DeriveStats
//
//	@doc:
//		Derive stats of owner group expression
//
//---------------------------------------------------------------------------
void CCostContext::DeriveStats() {
  GPOS_ASSERT(nullptr != m_pgexpr);
  GPOS_ASSERT(nullptr != m_poc);

  if (nullptr != m_pstats) {
    // stats are already derived
    return;
  }

  if (m_pgexpr->Pop()->FScalar()) {
    // exit if scalar operator
    return;
  }

  CExpressionHandle exprhdl(m_mp);
  exprhdl.Attach(this);
  exprhdl.DeriveCostContextStats();
  if (nullptr == exprhdl.Pstats()) {
    GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoStats, GPOS_WSZ_LIT("CCostContext"));
  }

  exprhdl.Pstats()->AddRef();
  m_pstats = exprhdl.Pstats();
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DerivePlanProps
//
//	@doc:
//		Derive properties of the plan carried by cost context
//
//---------------------------------------------------------------------------
void CCostContext::DerivePlanProps(CMemoryPool *mp) {
  GPOS_ASSERT(nullptr != m_pdrgpoc);

  if (nullptr == m_pdpplan) {
    // derive properties of the plan carried by cost context
    CExpressionHandle exprhdl(mp);
    exprhdl.Attach(this);
    exprhdl.DerivePlanPropsForCostContext();
    CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(exprhdl.Pdp());
    GPOS_ASSERT(nullptr != pdpplan);

    // set derived plan properties
    pdpplan->AddRef();
    m_pdpplan = pdpplan;
    GPOS_ASSERT(nullptr != m_pdpplan);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::operator ==
//
//	@doc:
//		Comparison operator
//
//---------------------------------------------------------------------------
bool CCostContext::operator==(const CCostContext &cc) const {
  return Equals(cc, *this);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::IsValid
//
//	@doc:
//		Check validity by comparing derived and required properties
//
//---------------------------------------------------------------------------
bool CCostContext::IsValid(CMemoryPool *mp) {
  GPOS_ASSERT(nullptr != m_poc);
  GPOS_ASSERT(nullptr != m_pdrgpoc);

  // obtain relational properties from group
  CDrvdPropRelational *pdprel = CDrvdPropRelational::GetRelationalProperties(Pgexpr()->Pgroup()->Pdp());
  GPOS_ASSERT(nullptr != pdprel);

  // derive plan properties
  DerivePlanProps(mp);

  // checking for required properties satisfaction
  bool fValid = Poc()->Prpp()->FSatisfied(pdprel, m_pdpplan);

#ifdef GPOS_DEBUG
  if (COptCtxt::FAllEnforcersEnabled() && !fValid) {
    CAutoTrace at(mp);
    IOstream &os = at.Os();

    os << std::endl << "PROPERTY MISMATCH:" << std::endl;
    os << std::endl << "GROUP ID: " << Pgexpr()->Pgroup()->Id() << std::endl;
    os << std::endl << "GEXPR:" << std::endl;
    Pgexpr()->OsPrint(at.Os());
    os << std::endl << "REQUIRED PROPERTIES:" << std::endl << *(m_poc->Prpp());
    os << std::endl << "DERIVED PROPERTIES:" << std::endl << *pdprel << std::endl << *m_pdpplan;
  }
#endif  // GPOS_DEBUG

  return fValid;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBreakCostTiesForJoinPlan
//
//	@doc:
//		For two cost contexts with join plans of the same cost, break the
//		tie in cost values based on join depth,
//		if tie-resolution succeeded, store a pointer to preferred cost
//		context in output argument
//
//---------------------------------------------------------------------------
void CCostContext::BreakCostTiesForJoinPlans(const CCostContext *pccFst, const CCostContext *pccSnd,
                                             CONST_COSTCTXT_PTR *ppccPrefered,  // output: preferred cost context
                                             bool *pfTiesResolved  // output: if true, tie resolution has succeeded
) {
  GPOS_ASSERT(nullptr != pccFst);
  GPOS_ASSERT(nullptr != pccSnd);
  GPOS_ASSERT(nullptr != ppccPrefered);
  GPOS_ASSERT(nullptr != pfTiesResolved);
  GPOS_ASSERT(*(pccFst->Poc()) == *(pccSnd->Poc()));
  GPOS_ASSERT(estCosted == pccFst->Est());
  GPOS_ASSERT(estCosted == pccSnd->Est());
  GPOS_ASSERT(pccFst->Cost() == pccSnd->Cost());
  GPOS_ASSERT(CUtils::FPhysicalJoin(pccFst->Pgexpr()->Pop()));
  GPOS_ASSERT(CUtils::FPhysicalJoin(pccSnd->Pgexpr()->Pop()));

  // for two join plans with the same estimated rows in both children,
  // prefer the plan that has smaller tree depth on the inner side,
  // this is because a smaller tree depth means that row estimation
  // errors are not grossly amplified,
  // since we build a hash table/broadcast the inner side, we need
  // to have more reliable statistics on this side

  *pfTiesResolved = false;
  *ppccPrefered = nullptr;
  CDouble dRowsOuterFst = (*pccFst->Pdrgpoc())[0]->PccBest()->Pstats()->Rows();
  CDouble dRowsInnerFst = (*pccFst->Pdrgpoc())[1]->PccBest()->Pstats()->Rows();
  if (dRowsOuterFst != dRowsInnerFst) {
    // two children of first plan have different row estimates
    return;
  }

  CDouble dRowsOuterSnd = (*pccSnd->Pdrgpoc())[0]->PccBest()->Pstats()->Rows();
  CDouble dRowsInnerSnd = (*pccSnd->Pdrgpoc())[1]->PccBest()->Pstats()->Rows();
  if (dRowsOuterSnd != dRowsInnerSnd) {
    // two children of second plan have different row estimates
    return;
  }

  if (dRowsInnerFst != dRowsInnerSnd) {
    // children of first plan have different row estimates compared to second plan
    return;
  }

  // both plans have equal estimated rows for both children, break tie based on join depth
  *pfTiesResolved = true;
  uint32_t ulOuterJoinDepthFst =
      CDrvdPropRelational::GetRelationalProperties((*pccFst->Pgexpr())[0]->Pdp())->GetJoinDepth();
  uint32_t ulInnerJoinDepthFst =
      CDrvdPropRelational::GetRelationalProperties((*pccFst->Pgexpr())[1]->Pdp())->GetJoinDepth();
  if (ulInnerJoinDepthFst < ulOuterJoinDepthFst) {
    *ppccPrefered = pccFst;
  } else {
    *ppccPrefered = pccSnd;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBetterThan
//
//	@doc:
//		Is current context better than the given equivalent context
//		based on cost?
//
//---------------------------------------------------------------------------
bool CCostContext::FBetterThan(const CCostContext *pcc) const {
  GPOS_ASSERT(nullptr != pcc);
  GPOS_ASSERT(*m_poc == *(pcc->Poc()));
  GPOS_ASSERT(estCosted == m_estate);
  GPOS_ASSERT(estCosted == pcc->Est());

  // if three stage scalar dqa flag is enforced we should mark plans containing
  // alternatives generated by CXformSplitDQA with three stages aggs as better
  // this should be the top most rule as we want to override cost based check between
  // 3-stage scalar aggs and 2-stage scalar aggs.
  if (GPOS_FTRACE(EopttraceForceThreeStageScalarDQA)) {
    if (CUtils::FPhysicalAgg(Pgexpr()->Pop()) && CUtils::FPhysicalAgg(pcc->Pgexpr()->Pop())) {
      // we are only interested in aggs generated by CXformSplitDQA. If the trace flag is turned on
      // we want to favor 3-stage agg over 2-stage scalar DQA agg. So whenever there is comparison
      // between 3-stage and 2-stage cost context, we mark 2-stage as less optimal.
      // for 2-stage vs 2-stage or 3-stage vs 3-stage, we let costing decide.
      // single stage agg do not get optimized when multi-stage aggs are present,
      // refer to COptimizationContext::FOptimizeAgg.
      if (IsTwoStageScalarDQACostCtxt(this) && IsThreeStageScalarDQACostCtxt(pcc)) {
        return false;
      }
      // if the comparison is between 3-stage agg and 2-stage scalar DQA aggs generated from CXformSplitDQA,
      // always mark 3-stage agg as having the better cost context.
      // note: CXformSplitDQA will never generate a mix of scalar and non-scalar DQAs.
      if (IsThreeStageScalarDQACostCtxt(this) && IsTwoStageScalarDQACostCtxt(pcc)) {
        return true;
      }
    }
  }

  double dCostDiff = (Cost().Get() - pcc->Cost().Get());
  if (dCostDiff < 0.0) {
    // if current context has a strictly smaller cost, then it is preferred
    return true;
  }

  if (dCostDiff > 0.0) {
    // if current context has a strictly larger cost, then it is not preferred
    return false;
  }

  // RULE 3: break ties in cost of join plans,
  // if both plans have the same estimated rows for both children, prefer
  // the plan with deeper outer child
  if (CUtils::FPhysicalJoin(Pgexpr()->Pop()) && CUtils::FPhysicalJoin(pcc->Pgexpr()->Pop())) {
    CONST_COSTCTXT_PTR pccPrefered = nullptr;
    bool fSuccess = false;
    BreakCostTiesForJoinPlans(this, pcc, &pccPrefered, &fSuccess);
    if (fSuccess) {
      return (this == pccPrefered);
    }
  }

  if (COperator::EopPhysicalSpool == pcc->Pgexpr()->Pop()->Eopid() &&
      COperator::EopPhysicalSpool == Pgexpr()->Pop()->Eopid()) {
    CPhysicalSpool *current_best_ctxt = CPhysicalSpool::PopConvert(Pgexpr()->Pop());
    CPhysicalSpool *new_ctxt = CPhysicalSpool::PopConvert(pcc->Pgexpr()->Pop());

    if (new_ctxt->FEager() && !current_best_ctxt->FEager()) {
      return true;
    }
  }

  return false;
}

bool CCostContext::IsTwoStageScalarDQACostCtxt(const CCostContext *pcc) {
  if (CUtils::FPhysicalAgg(pcc->Pgexpr()->Pop())) {
    CPhysicalAgg *popAgg = CPhysicalAgg::PopConvert(pcc->Pgexpr()->Pop());
    // 2 stage scalar agg are only generated by split dqa xform
    GPOS_ASSERT_IMP(popAgg->IsTwoStageScalarDQA(), popAgg->IsAggFromSplitDQA());
    return (popAgg->IsAggFromSplitDQA() && popAgg->IsTwoStageScalarDQA());
  }

  return false;
}

bool CCostContext::IsThreeStageScalarDQACostCtxt(const CCostContext *pcc) {
  if (CUtils::FPhysicalAgg(pcc->Pgexpr()->Pop())) {
    CPhysicalAgg *popAgg = CPhysicalAgg::PopConvert(pcc->Pgexpr()->Pop());
    // 3 stage scalar agg are only generated by split dqa xform
    GPOS_ASSERT_IMP(popAgg->IsThreeStageScalarDQA(), popAgg->IsAggFromSplitDQA());
    return (popAgg->IsAggFromSplitDQA() && popAgg->IsThreeStageScalarDQA());
  }

  return false;
}

bool CCostContext::IsMultiStageAggCostCtxt(const CCostContext *pcc) {
  if (CUtils::FPhysicalAgg(pcc->Pgexpr()->Pop())) {
    CPhysicalAgg *popAgg = CPhysicalAgg::PopConvert(pcc->Pgexpr()->Pop());
    return popAgg->FMultiStage();
  }

  return false;
}

bool CCostContext::IsSingleStageAggCostCtxt(const CCostContext *pcc) {
  if (CUtils::FPhysicalAgg(pcc->Pgexpr()->Pop())) {
    CPhysicalAgg *popAgg = CPhysicalAgg::PopConvert(pcc->Pgexpr()->Pop());
    return !popAgg->FMultiStage();
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::CostCompute
//
//	@doc:
//		Compute cost of current context,
//
//		the function extracts cardinality and row width of owner operator
//		and child operators, and then adjusts row estimate obtained from
//		statistics based on data distribution obtained from plan properties,
//
//		statistics row estimate is computed on logical expressions by
//		estimating the size of the whole relation regardless data
//		distribution, on the other hand, optimizer's cost model computes
//		the cost of a plan instance on some segment,
//
//		when a plan produces tuples distributed to multiple segments, we
//		need to divide statistics row estimate by the number segments to
//		provide a per-segment row estimate for cost computation,
//
//		Note that this scaling of row estimate cannot happen during
//		statistics derivation since plans are not created yet at this point
//
// 		this function also extracts number of rebinds of owner operator child
//		operators, if statistics are computed using predicates with external
//		parameters (outer references), number of rebinds is the total number
//		of external parameters' values
//
//---------------------------------------------------------------------------
CCost CCostContext::CostCompute(CMemoryPool *mp, CCostArray *pdrgpcostChildren) {
  // derive context stats
  DeriveStats();

  uint32_t arity = 0;
  if (nullptr != m_pdrgpoc) {
    arity = Pdrgpoc()->Size();
  }

  m_pstats->AddRef();
  ICostModel::SCostingInfo ci(mp, arity, GPOS_NEW(mp) ICostModel::CCostingStats(m_pstats));

  ICostModel *pcm = COptCtxt::PoctxtFromTLS()->GetCostModel();

  CExpressionHandle exprhdl(mp);
  exprhdl.Attach(this);

  // extract local costing info
  double rows = m_pstats->Rows().Get();

  ci.SetRows(rows);

  double width = m_pstats->Width(mp, m_poc->Prpp()->PcrsRequired()).Get();
  ci.SetWidth(width);

  double num_rebinds = m_pstats->NumRebinds().Get();
  ci.SetRebinds(num_rebinds);
  GPOS_ASSERT_IMP(!exprhdl.HasOuterRefs(), GPOPT_DEFAULT_REBINDS == (uint32_t)(num_rebinds) &&
                                               "invalid number of rebinds when there are no outer references");

  // extract children costing info
  for (uint32_t ul = 0; ul < arity; ul++) {
    COptimizationContext *pocChild = (*m_pdrgpoc)[ul];
    CCostContext *pccChild = pocChild->PccBest();
    GPOS_ASSERT(nullptr != pccChild);

    IStatistics *child_stats = pccChild->Pstats();

    child_stats->AddRef();
    ci.SetChildStats(ul, GPOS_NEW(mp) ICostModel::CCostingStats(child_stats));

    double dRowsChild = child_stats->Rows().Get();

    ci.SetChildRows(ul, dRowsChild);

    double dWidthChild = child_stats->Width(mp, pocChild->Prpp()->PcrsRequired()).Get();
    ci.SetChildWidth(ul, dWidthChild);

    double dRebindsChild = child_stats->NumRebinds().Get();
    ci.SetChildRebinds(ul, dRebindsChild);
    GPOS_ASSERT_IMP(!exprhdl.HasOuterRefs(ul), GPOPT_DEFAULT_REBINDS == (uint32_t)(dRebindsChild) &&
                                                   "invalid number of rebinds when there are no outer references");

    double dCostChild = (*pdrgpcostChildren)[ul]->Get();
    ci.SetChildCost(ul, dCostChild);
  }

  // compute cost using the underlying cost model
  return pcm->Cost(exprhdl, &ci);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DRowsPerHost
//
//	@doc:
//		Return the number of rows per host
//
//---------------------------------------------------------------------------
CDouble CCostContext::DRowsPerHost() const {
  double rows = Pstats()->Rows().Get();

  return CDouble(rows);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::OsPrint
//
//	@doc:
//		Print function;
//
//---------------------------------------------------------------------------
IOstream &CCostContext::OsPrint(IOstream &os) const {
  os << "main ctxt (stage " << m_poc->UlSearchStageIndex() << ")" << m_poc->Id() << "." << m_ulOptReq;

  if (nullptr != m_pdrgpoc) {
    os << ", child ctxts:[";
    uint32_t arity = m_pdrgpoc->Size();
    if (0 < arity) {
      for (uint32_t i = 0; i < arity - 1; i++) {
        os << (*m_pdrgpoc)[i]->Id();
        os << ", ";
      }
      os << (*m_pdrgpoc)[arity - 1]->Id();
    }
    os << "]";
  }

  if (nullptr != m_pstats) {
    os << ", rows:" << m_pstats->Rows();
    if (FOwnsStats()) {
      os << " (owned)";
    } else {
      os << " (group)";
    }
  }

  if (m_fPruned) {
    os << ", cost lower bound: " << this->Cost() << "\t PRUNED";
  } else {
    os << ", cost: " << this->Cost();
  }

  return os << std::endl;
}

// EOF
