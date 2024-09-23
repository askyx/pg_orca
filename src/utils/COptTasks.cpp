//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Greenplum, Inc.
//
//	@filename:
//		COptTasks.cpp
//
//	@doc:
//		Routines to perform optimization related tasks using the gpos framework
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/utils/COptTasks.h"

extern "C" {

#include "utils/fmgroids.h"
#include "utils/guc.h"
}

#include "gpdbcost/CCostModelGPDB.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/config/CConfigParamMapping.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/eval/CConstExprEvaluatorDXL.h"
#include "gpopt/exception.h"
#include "gpopt/gpdbwrappers.h"
#include "gpopt/hints/CPlanHint.h"
#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/optimizer/COptimizer.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/relcache/CMDProviderRelcache.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"
#include "gpopt/translate/CTranslatorQueryToDXL.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/plan_generator.h"
#include "gpopt/utils/CConstExprEvaluatorProxy.h"
#include "gpopt/utils/gpdbdefs.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpos/_api.h"
#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CException.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/set.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/exception.h"
#include "naucrates/init.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDRelStats.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;
using namespace gpdbcost;

// size of error buffer
#define GPOPT_ERROR_BUFFER_SIZE 10 * 1024 * 1024

// definition of default AutoMemoryPool
#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc)

// default id for the source system
const CSystemId default_sysid(IMDId::EmdidGeneral, GPOS_WSZ_STR_LENGTH("GPDB"));

//---------------------------------------------------------------------------
//	@function:
//		SOptContext::SOptContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
SOptContext::SOptContext() = default;

//---------------------------------------------------------------------------
//	@function:
//		SOptContext::Free
//
//	@doc:
//		Free all members except those pointed to by either input or
//		output
//
//---------------------------------------------------------------------------
void SOptContext::Free(SOptContext::EPin input, SOptContext::EPin output) const {
  if (nullptr != m_query_dxl && epinQueryDXL != input && epinQueryDXL != output) {
    gpdb::GPDBFree(m_query_dxl);
  }

  if (nullptr != m_query && epinQuery != input && epinQuery != output) {
    gpdb::GPDBFree(m_query);
  }

  if (nullptr != m_plan_dxl && epinPlanDXL != input && epinPlanDXL != output) {
    gpdb::GPDBFree(m_plan_dxl);
  }

  if (nullptr != m_plan_stmt && epinPlStmt != input && epinPlStmt != output) {
    gpdb::GPDBFree(m_plan_stmt);
  }

  if (nullptr != m_error_msg && epinErrorMsg != input && epinErrorMsg != output) {
    gpdb::GPDBFree(m_error_msg);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		SOptContext::CloneErrorMsg
//
//	@doc:
//		Clone m_error_msg to given memory context. Return NULL if there is no
//		error message.
//
//---------------------------------------------------------------------------
CHAR *SOptContext::CloneErrorMsg(MemoryContext context) const {
  if (nullptr == context || nullptr == m_error_msg) {
    return nullptr;
  }
  return gpdb::MemCtxtStrdup(context, m_error_msg);
}

//---------------------------------------------------------------------------
//	@function:
//		SOptContext::Cast
//
//	@doc:
//		Casting function
//
//---------------------------------------------------------------------------
SOptContext *SOptContext::Cast(void *ptr) {
  GPOS_ASSERT(nullptr != ptr);

  return reinterpret_cast<SOptContext *>(ptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CreateMultiByteCharStringFromWCString
//
//	@doc:
//		Return regular string from wide-character string
//
//---------------------------------------------------------------------------
CHAR *COptTasks::CreateMultiByteCharStringFromWCString(const WCHAR *wcstr) {
  GPOS_ASSERT(nullptr != wcstr);

  const uint32_t input_len = GPOS_WSZ_LENGTH(wcstr);
  const uint32_t wchar_size = GPOS_SIZEOF(WCHAR);
  const uint32_t max_len = (input_len + 1) * wchar_size;

  CHAR *str = (CHAR *)gpdb::GPDBAlloc(max_len);

  gpos::clib::Wcstombs(str, const_cast<WCHAR *>(wcstr), max_len);
  str[max_len - 1] = '\0';

  return str;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::Execute
//
//	@doc:
//		Execute a task using GPOS. TODO extend gpos to provide
//		this functionality
//
//---------------------------------------------------------------------------
void COptTasks::Execute(void *(*func)(void *), void *func_arg) {
  Assert(func);

  CHAR *err_buf = (CHAR *)palloc(GPOPT_ERROR_BUFFER_SIZE);
  err_buf[0] = '\0';

  // initialize DXL support
  InitDXL();

  bool abort_flag = false;

  CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);

  auto *xx = (SOptContext *)func_arg;

  gpos_exec_params params;
  params.func = func;
  params.arg = func_arg;
  params.stack_start = &params;
  params.config = xx->config;
  params.error_buffer = err_buf;
  params.error_buffer_size = GPOPT_ERROR_BUFFER_SIZE;
  params.abort_requested = &abort_flag;

  // execute task and send log message to server log
  GPOS_TRY {
    (void)gpos_exec(&params);
  }
  GPOS_CATCH_EX(ex) {
    LogExceptionMessageAndDelete(err_buf);
    GPOS_RETHROW(ex);
  }
  GPOS_CATCH_END;
  LogExceptionMessageAndDelete(err_buf);
}

void COptTasks::LogExceptionMessageAndDelete(CHAR *err_buf) {
  if ('\0' != err_buf[0]) {
    elog(LOG, "%s", CreateMultiByteCharStringFromWCString((WCHAR *)err_buf));
  }

  pfree(err_buf);
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::ConvertToPlanStmtFromDXL
//
//	@doc:
//		Translate a DXL tree into a planned statement
//
//---------------------------------------------------------------------------
PlannedStmt *COptTasks::ConvertToPlanStmtFromDXL(CMemoryPool *mp, CMDAccessor *md_accessor, const Query *orig_query,
                                                 const CDXLNode *dxlnode, bool can_set_tag) {
  GPOS_ASSERT(nullptr != md_accessor);
  GPOS_ASSERT(nullptr != dxlnode);

  CContextDXLToPlStmt dxl_to_plan_stmt_ctxt;

  // translate DXL -> PlannedStmt
  CTranslatorDXLToPlStmt dxl_to_plan_stmt_translator(mp, md_accessor, &dxl_to_plan_stmt_ctxt);
  return dxl_to_plan_stmt_translator.GetPlannedStmtFromDXL(dxlnode, orig_query, can_set_tag);
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::CreateOptimizerConfig
//
//	@doc:
//		Create the optimizer configuration
//
//---------------------------------------------------------------------------
COptimizerConfig *COptTasks::CreateOptimizerConfig(CMemoryPool *mp, ICostModel *cost_model, CPlanHint *plan_hints) {
  // get chosen plan number, cost threshold
  ULLONG plan_id = (ULLONG)optimizer_plan_id;
  ULLONG num_samples = (ULLONG)optimizer_samples_number;
  DOUBLE cost_threshold = (DOUBLE)0;

  DOUBLE damping_factor_filter = (DOUBLE)optimizer_damping_factor_filter;
  DOUBLE damping_factor_join = (DOUBLE)optimizer_damping_factor_join;
  DOUBLE damping_factor_groupby = (DOUBLE)optimizer_damping_factor_groupby;

  uint32_t cte_inlining_cutoff = (uint32_t)optimizer_cte_inlining_bound;
  uint32_t join_arity_for_associativity_commutativity = (uint32_t)optimizer_join_arity_for_associativity_commutativity;
  uint32_t array_expansion_threshold = (uint32_t)optimizer_array_expansion_threshold;
  uint32_t join_order_threshold = (uint32_t)optimizer_join_order_threshold;
  uint32_t broadcast_threshold = (uint32_t)100000;
  uint32_t push_group_by_below_setop_threshold = (uint32_t)optimizer_push_group_by_below_setop_threshold;
  uint32_t xform_bind_threshold = (uint32_t)optimizer_xform_bind_threshold;
  uint32_t skew_factor = (uint32_t)optimizer_skew_factor;

  return GPOS_NEW(mp) COptimizerConfig(
      GPOS_NEW(mp) CEnumeratorConfig(mp, plan_id, num_samples, cost_threshold),
      GPOS_NEW(mp)
          CStatisticsConfig(mp, damping_factor_filter, damping_factor_join, damping_factor_groupby, MAX_STATS_BUCKETS),
      GPOS_NEW(mp) CCTEConfig(cte_inlining_cutoff), cost_model,
      GPOS_NEW(mp) CHint(join_arity_for_associativity_commutativity, array_expansion_threshold, join_order_threshold,
                         broadcast_threshold, false, /* don't create Assert nodes for constraints, we'll
                                                      * enforce them ourselves in the executor */
                         push_group_by_below_setop_threshold, xform_bind_threshold, skew_factor),
      plan_hints, GPOS_NEW(mp) CWindowOids(OID(0), OID(0)));
}

//---------------------------------------------------------------------------
//		@function:
//			COptTasks::SetCostModelParams
//
//      @doc:
//			Set cost model parameters
//
//---------------------------------------------------------------------------
void COptTasks::SetCostModelParams(ICostModel *cost_model) {
  GPOS_ASSERT(nullptr != cost_model);

  if (1024 > 1.0) {
    // change NLJ cost factor
    ICostModelParams::SCostParam *cost_param =
        cost_model->GetCostModelParams()->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor);
    CDouble nlj_factor(1024);
    cost_model->GetCostModelParams()->SetParam(cost_param->Id(), nlj_factor, nlj_factor - 0.5, nlj_factor + 0.5);
  }

  if (1 > 1.0 || 1 < 1.0) {
    // change sort cost factor
    ICostModelParams::SCostParam *cost_param =
        cost_model->GetCostModelParams()->PcpLookup(CCostModelParamsGPDB::EcpSortTupWidthCostUnit);

    CDouble sort_factor(1);
    cost_model->GetCostModelParams()->SetParam(cost_param->Id(), cost_param->Get() * 1,
                                               cost_param->GetLowerBoundVal() * 1, cost_param->GetUpperBoundVal() * 1);
  }
}

//---------------------------------------------------------------------------
//      @function:
//			COptTasks::GetCostModel
//
//      @doc:
//			Generate an instance of optimizer cost model
//
//---------------------------------------------------------------------------
ICostModel *COptTasks::GetCostModel(CMemoryPool *mp, uint32_t num_segments) {
  ICostModel *cost_model = GPOS_NEW(mp) CCostModelGPDB(mp);

  SetCostModelParams(cost_model);

  return cost_model;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::OptimizeTask
//
//	@doc:
//		task that does the optimizes query to physical DXL
//
//---------------------------------------------------------------------------
void *COptTasks::OptimizeTask(void *ptr) {
  GPOS_ASSERT(nullptr != ptr);
  SOptContext *opt_ctxt = SOptContext::Cast(ptr);

  GPOS_ASSERT(nullptr != opt_ctxt->m_query);
  GPOS_ASSERT(nullptr == opt_ctxt->m_plan_dxl);
  GPOS_ASSERT(nullptr == opt_ctxt->m_plan_stmt);

  AUTO_MEM_POOL(amp);
  CMemoryPool *mp = amp.Pmp();

  // Does the metadatacache need to be reset?
  //
  // On the first call, before the cache has been initialized, we
  // don't care about the return value of MDCacheNeedsReset(). But
  // we need to call it anyway, to give it a chance to initialize
  // the invalidation mechanism.
  bool reset_mdcache = gpdb::MDCacheNeedsReset();

  // initialize metadata cache, or purge if needed, or change size if requested
  if (!CMDCache::FInitialized()) {
    CMDCache::Init();
    CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
  } else if (reset_mdcache) {
    CMDCache::Reset();
    CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
  } else if (CMDCache::ULLGetCacheQuota() != (ULLONG)optimizer_mdcache_size * 1024L) {
    CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
  }

  CSearchStageArray *search_strategy_arr = nullptr;

  CBitSet *trace_flags = nullptr;
  CBitSet *enabled_trace_flags = nullptr;
  CBitSet *disabled_trace_flags = nullptr;
  void *plan_dxl = nullptr;
  bool flag = GPOS_CONDIF(enable_new_planner_generation);

  IMdIdArray *col_stats = nullptr;
  MdidHashSet *rel_stats = nullptr;

  GPOS_TRY {
    // set trace flags
    trace_flags = CConfigParamMapping::PackConfigParamInBitset(mp, CXform::ExfSentinel);
    SetTraceflags(mp, trace_flags, &enabled_trace_flags, &disabled_trace_flags);

    // set up relcache MD provider
    CMDProviderRelcache *relcache_provider = GPOS_NEW(mp) CMDProviderRelcache();

    {
      // scope for MD accessor
      CMDAccessor mda(mp, CMDCache::Pcache(), default_sysid, relcache_provider);

      uint32_t num_segments = gpdb::GetGPSegmentCount();
      uint32_t num_segments_for_costing = 0;
      if (0 == num_segments_for_costing) {
        num_segments_for_costing = num_segments;
      }

      CAutoP<CTranslatorQueryToDXL> query_to_dxl_translator;
      query_to_dxl_translator = CTranslatorQueryToDXL::QueryToDXLInstance(mp, &mda, (Query *)opt_ctxt->m_query);

      ICostModel *cost_model = GetCostModel(mp, num_segments_for_costing);
      CPlanHint *plan_hints = nullptr;
      COptimizerConfig *optimizer_config = CreateOptimizerConfig(mp, cost_model, plan_hints);
      CConstExprEvaluatorProxy expr_eval_proxy(mp, &mda);
      IConstExprEvaluator *expr_evaluator = GPOS_NEW(mp) CConstExprEvaluatorDXL(mp, &mda, &expr_eval_proxy);

      CDXLNode *query_dxl = query_to_dxl_translator->TranslateQueryToDXL();
      CDXLNodeArray *query_output_dxlnode_array = query_to_dxl_translator->GetQueryOutputCols();
      CDXLNodeArray *cte_dxlnode_array = query_to_dxl_translator->GetCTEs();
      GPOS_ASSERT(nullptr != query_output_dxlnode_array);

      // See NoteDistributionPolicyOpclasses() in src/backend/gpopt/translate/CTranslatorQueryToDXL.cpp
      CAutoTraceFlag atf2(EopttraceUseLegacyOpfamilies, false);
      CAutoTraceFlag atf3(EopttracePrintQuery, true);
      CAutoTraceFlag atf4(EopttracePrintPlan, true);
      // CAutoTraceFlag atf5(EopttracePrintMemoAfterExploration, true);
      // CAutoTraceFlag atf6(EopttracePrintMemoAfterImplementation, true);
      // CAutoTraceFlag atf7(EopttracePrintMemoAfterOptimization, true);
      // CAutoTraceFlag atf8(EopttracePrintMemoEnforcement, true);
      // CAutoTraceFlag atf9(EopttracePrintGroupProperties, true);
      // CAutoTraceFlag atfa(EopttracePrintExpressionProperties, true);
      // CAutoTraceFlag atfb(EopttracePrintOptimizationContext, true);
      // CAutoTraceFlag atfc(EopttracePrintXformPattern, true);
      // CAutoTraceFlag atfd(EopttracePrintRequiredColumns, true);
      // CAutoTraceFlag atfe(EopttracePrintXform, true);
      // CAutoTraceFlag atff(EopttracePrintXformResults, true);

      plan_dxl = COptimizer::PdxlnOptimize(mp, &mda, query_dxl, query_output_dxlnode_array, cte_dxlnode_array,
                                           expr_evaluator, search_strategy_arr, optimizer_config);

      if (flag) {
        auto *plan = (PlanResult *)plan_dxl;
        auto *plan_stmt = makeNode(PlannedStmt);
        plan_stmt->planTree = plan->plan;
        plan_stmt->rtable = plan->rtable;
        plan_stmt->relationOids = plan->relationOids;
        plan_stmt->commandType = CMD_SELECT;

        opt_ctxt->m_plan_stmt = plan_stmt;
      } else {
        // translate DXL->PlStmt only when needed
        if (opt_ctxt->m_should_generate_plan_stmt) {
          // always use opt_ctxt->m_query->can_set_tag as the query_to_dxl_translator->Pquery() is a mutated Query
          // object that may not have the correct can_set_tag
          opt_ctxt->m_plan_stmt = (PlannedStmt *)gpdb::CopyObject(ConvertToPlanStmtFromDXL(
              mp, &mda, opt_ctxt->m_query, (CDXLNode *)plan_dxl, opt_ctxt->m_query->canSetTag));
        }
      }

      CStatisticsConfig *stats_conf = optimizer_config->GetStatsConf();
      col_stats = GPOS_NEW(mp) IMdIdArray(mp);
      stats_conf->CollectMissingStatsColumns(col_stats);

      rel_stats = GPOS_NEW(mp) MdidHashSet(mp);
      PrintMissingStatsWarning(mp, &mda, col_stats, rel_stats);

      rel_stats->Release();
      col_stats->Release();

      expr_evaluator->Release();
      query_dxl->Release();
      optimizer_config->Release();
      if (!flag)
        ((CDXLNode *)plan_dxl)->Release();
    }
  }
  GPOS_CATCH_EX(ex) {
    ResetTraceflags(enabled_trace_flags, disabled_trace_flags);
    CRefCount::SafeRelease(rel_stats);
    CRefCount::SafeRelease(col_stats);
    CRefCount::SafeRelease(enabled_trace_flags);
    CRefCount::SafeRelease(disabled_trace_flags);
    CRefCount::SafeRelease(trace_flags);
    if (!flag)
      CRefCount::SafeRelease(((CDXLNode *)plan_dxl));
    CMDCache::Shutdown();

    IErrorContext *errctxt = CTask::Self()->GetErrCtxt();

    opt_ctxt->m_is_unexpected_failure = IsLoggableFailure(ex);
    opt_ctxt->m_error_msg = CreateMultiByteCharStringFromWCString(errctxt->GetErrorMsg());

    GPOS_RETHROW(ex);
  }
  GPOS_CATCH_END;

  // cleanup
  ResetTraceflags(enabled_trace_flags, disabled_trace_flags);
  CRefCount::SafeRelease(enabled_trace_flags);
  CRefCount::SafeRelease(disabled_trace_flags);
  CRefCount::SafeRelease(trace_flags);
  if (!optimizer_metadata_caching) {
    CMDCache::Shutdown();
  }

  return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PrintMissingStatsWarning
//
//	@doc:
//		Print warning messages for columns with missing statistics
//
//---------------------------------------------------------------------------
void COptTasks::PrintMissingStatsWarning(CMemoryPool *mp, CMDAccessor *md_accessor, IMdIdArray *col_stats,
                                         MdidHashSet *rel_stats) {
  GPOS_ASSERT(nullptr != md_accessor);
  GPOS_ASSERT(nullptr != col_stats);
  GPOS_ASSERT(nullptr != rel_stats);

  CWStringDynamic wcstr(mp);
  COstreamString oss(&wcstr);

  const uint32_t num_missing_col_stats = col_stats->Size();
  for (uint32_t ul = 0; ul < num_missing_col_stats; ul++) {
    IMDId *mdid = (*col_stats)[ul];
    CMDIdColStats *mdid_col_stats = CMDIdColStats::CastMdid(mdid);

    IMDId *rel_mdid = mdid_col_stats->GetRelMdId();
    const uint32_t pos = mdid_col_stats->Position();
    const IMDRelation *rel = md_accessor->RetrieveRel(rel_mdid);

    if (IMDRelation::ErelstorageForeign != rel->RetrieveRelStorageType()) {
      if (!rel_stats->Contains(rel_mdid)) {
        if (0 != ul) {
          oss << ", ";
        }

        rel_mdid->AddRef();
        rel_stats->Insert(rel_mdid);
        oss << rel->Mdname().GetMDName()->GetBuffer();
      }

      CMDName mdname = rel->GetMdCol(pos)->Mdname();

      char msgbuf[NAMEDATALEN * 2 + 100];
      snprintf(msgbuf, sizeof(msgbuf), "Missing statistics for column: %s.%s",
               CreateMultiByteCharStringFromWCString(rel->Mdname().GetMDName()->GetBuffer()),
               CreateMultiByteCharStringFromWCString(mdname.GetMDName()->GetBuffer()));
      GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION, LOG, msgbuf, nullptr);
    }
  }

  if (0 < rel_stats->Size()) {
    int length = NAMEDATALEN * rel_stats->Size() + 200;
    char msgbuf[length];
    snprintf(msgbuf, sizeof(msgbuf), "One or more columns in the following table(s) do not have statistics: %s",
             CreateMultiByteCharStringFromWCString(wcstr.GetBuffer()));
    GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION, NOTICE, msgbuf,
                "For non-partitioned tables, run analyze <table_name>(<column_list>)."
                " For partitioned tables, run analyze rootpartition <table_name>(<column_list>)."
                " See log for columns missing statistics.");
  }
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::Optimize
//
//	@doc:
//		optimizes a query to physical DXL
//
//---------------------------------------------------------------------------
char *COptTasks::Optimize(Query *query) {
  Assert(query);

  SOptContext gpopt_context;
  gpopt_context.m_query = query;
  Execute(&OptimizeTask, &gpopt_context);

  // clean up context
  gpopt_context.Free(gpopt_context.epinQuery, gpopt_context.epinPlanDXL);

  return gpopt_context.m_plan_dxl;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::GPOPTOptimizedPlan
//
//	@doc:
//		optimizes a query to plannedstmt
//
//---------------------------------------------------------------------------
PlannedStmt *COptTasks::GPOPTOptimizedPlan(Query *query, SOptContext *gpopt_context) {
  Assert(query);
  Assert(gpopt_context);

  gpopt_context->m_query = query;
  gpopt_context->m_should_generate_plan_stmt = true;
  Execute(&OptimizeTask, gpopt_context);
  return gpopt_context->m_plan_stmt;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SetXform
//
//	@doc:
//		Enable/Disable a given xform
//
//---------------------------------------------------------------------------
bool COptTasks::SetXform(char *xform_str, bool should_disable) {
  CXform *xform = CXformFactory::Pxff()->Pxf(xform_str);
  if (nullptr != xform) {
    optimizer_xforms[xform->Exfid()] = should_disable;

    return true;
  }

  return false;
}

// EOF
