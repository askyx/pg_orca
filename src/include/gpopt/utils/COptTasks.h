//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Greenplum, Inc.
//
//	@filename:
//		COptTasks.h
//
//	@doc:
//		Tasks that will perform optimization and related tasks
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef COptTasks_H
#define COptTasks_H

#include "gpopt/base/CColRef.h"
#include "gpopt/search/CSearchStage.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpos/error/CException.h"

extern bool optimizer_print_query;
extern bool optimizer_print_plan;
extern bool optimizer_print_xform;
extern bool optimizer_print_memo_after_exploration;
extern bool optimizer_print_memo_after_implementation;
extern bool optimizer_print_memo_after_optimization;
extern bool optimizer_print_job_scheduler;
extern bool optimizer_print_expression_properties;
extern bool optimizer_print_group_properties;
extern bool optimizer_print_optimization_context;
extern bool optimizer_print_optimization_stats;
extern bool optimizer_print_xform_results;

extern bool optimizer;
extern bool optimizer_log;
extern int optimizer_log_failure;
extern bool optimizer_control;
extern bool optimizer_trace_fallback;
extern bool optimizer_partition_selection_log;
extern int optimizer_minidump;
extern int optimizer_cost_model;
extern bool optimizer_metadata_caching;
extern int optimizer_mdcache_size;
extern bool optimizer_use_gpdb_allocators;

extern bool optimizer_enable_nljoin;
extern bool optimizer_enable_indexjoin;

extern bool optimizer_enable_sort;
extern bool optimizer_enable_materialize;
extern bool optimizer_enable_partition_propagation;
extern bool optimizer_enable_partition_selection;
extern bool optimizer_enable_outerjoin_rewrite;
extern bool optimizer_enable_multiple_distinct_aggs;
extern bool optimizer_enable_direct_dispatch;
extern bool optimizer_enable_hashjoin_redistribute_broadcast_children;
extern bool optimizer_enable_broadcast_nestloop_outer_child;
extern bool optimizer_discard_redistribute_hashjoin;
extern bool optimizer_enable_streaming_material;
extern bool optimizer_enable_gather_on_segment_for_dml;
extern bool optimizer_enable_assert_maxonerow;
extern bool optimizer_enable_constant_expression_evaluation;
extern bool optimizer_enable_bitmapscan;
extern bool optimizer_enable_outerjoin_to_unionall_rewrite;
extern bool optimizer_enable_ctas;
extern bool optimizer_enable_dml;
extern bool optimizer_enable_dml_constraints;
extern bool optimizer_enable_coordinator_only_queries;
extern bool optimizer_enable_hashjoin;
extern bool optimizer_enable_indexscan;
extern bool optimizer_enable_indexonlyscan;
extern bool optimizer_enable_tablescan;
extern bool optimizer_enable_hashagg;
extern bool optimizer_enable_groupagg;
extern bool optimizer_expand_fulljoin;
extern bool optimizer_enable_mergejoin;
extern bool optimizer_enable_redistribute_nestloop_loj_inner_child;
extern bool optimizer_force_comprehensive_join_implementation;
extern bool optimizer_enable_replicated_table;
extern bool optimizer_enable_foreign_table;
extern bool optimizer_enable_right_outer_join;
extern bool optimizer_enable_query_parameter;

extern bool optimizer_extract_dxl_stats;
extern bool optimizer_extract_dxl_stats_all_nodes;
extern bool optimizer_print_missing_stats;
extern double optimizer_damping_factor_filter;
extern double optimizer_damping_factor_join;
extern double optimizer_damping_factor_groupby;
extern bool optimizer_dpe_stats;
extern bool optimizer_enable_derive_stats_all_groups;

extern bool optimizer_enumerate_plans;
extern bool optimizer_sample_plans;
extern int optimizer_plan_id;
extern int optimizer_samples_number;

extern int optimizer_join_arity_for_associativity_commutativity;
extern int optimizer_array_expansion_threshold;
extern int optimizer_join_order_threshold;
extern int optimizer_join_order;
extern int optimizer_cte_inlining_bound;
extern int optimizer_push_group_by_below_setop_threshold;
extern int optimizer_xform_bind_threshold;
extern int optimizer_skew_factor;
extern bool optimizer_force_multistage_agg;
extern bool optimizer_force_three_stage_scalar_dqa;
extern bool optimizer_force_expanded_distinct_aggs;
extern bool optimizer_force_agg_skew_avoidance;
extern bool optimizer_penalize_skew;
extern bool optimizer_prune_computed_columns;
extern bool optimizer_push_requirements_from_consumer_to_producer;
extern bool optimizer_enforce_subplans;
extern bool optimizer_use_external_constant_expression_evaluation_for_ints;
extern bool optimizer_apply_left_outer_to_union_all_disregarding_stats;
extern bool optimizer_remove_order_below_dml;
extern bool optimizer_multilevel_partitioning;
extern bool optimizer_parallel_union;
extern bool optimizer_array_constraints;
extern bool optimizer_cte_inlining;
extern bool optimizer_enable_space_pruning;
extern bool optimizer_enable_associativity;
extern bool optimizer_enable_eageragg;
extern bool optimizer_enable_range_predicate_dpe;
extern bool optimizer_enable_push_join_below_union_all;
extern bool optimizer_enable_orderedagg;

#define OPTIMIZER_XFORMS_COUNT 400
extern bool optimizer_xforms[OPTIMIZER_XFORMS_COUNT];

// fwd decl
namespace gpos {
class CMemoryPool;
class CBitSet;
}  // namespace gpos

namespace gpdxl {
class CDXLNode;
}

namespace gpopt {
class CExpression;
class CMDAccessor;
class CQueryContext;
class COptimizerConfig;
class ICostModel;
class CPlanHint;
}  // namespace gpopt

struct PlannedStmt;
struct Query;
struct List;
struct MemoryContextData;

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

// context of optimizer input and output objects
struct SOptContext {
  // mark which pointer member should NOT be released
  // when calling Free() function
  enum EPin {
    epinQueryDXL,  // keep m_query_dxl
    epinQuery,     // keep m_query
    epinPlanDXL,   // keep m_plan_dxl
    epinPlStmt,    // keep m_plan_stmt
    epinErrorMsg   // keep m_error_msg
  };

  // query object serialized to DXL
  CHAR *m_query_dxl{nullptr};

  // query object
  Query *m_query{nullptr};

  // plan object serialized to DXL
  CHAR *m_plan_dxl{nullptr};

  // plan object
  PlannedStmt *m_plan_stmt{nullptr};

  // is generating a plan object required ?
  BOOL m_should_generate_plan_stmt{false};

  // did the optimizer fail unexpectedly?
  BOOL m_is_unexpected_failure{false};

  // buffer for optimizer error messages
  CHAR *m_error_msg{nullptr};

  // ctor
  SOptContext();

  // If there is an error print as warning and throw exception to abort
  // plan generation
  void HandleError(BOOL *had_unexpected_failure);

  // free all members except input and output pointers
  void Free(EPin input, EPin epinOutput) const;

  // Clone the error message in given context.
  CHAR *CloneErrorMsg(struct MemoryContextData *context) const;

  // casting function
  static SOptContext *Cast(void *ptr);

};  // struct SOptContext

class COptTasks {
 private:
  // execute a task given the argument
  static void Execute(void *(*func)(void *), void *func_arg);

  // map GPOS log severity level to GPDB, print error and delete the given error buffer
  static void LogExceptionMessageAndDelete(CHAR *err_buf);

  // create optimizer configuration object
  static COptimizerConfig *CreateOptimizerConfig(CMemoryPool *mp, ICostModel *cost_model, CPlanHint *plan_hints);

  // optimize a query to a physical DXL
  static void *OptimizeTask(void *ptr);

  // translate a DXL tree into a planned statement
  static PlannedStmt *ConvertToPlanStmtFromDXL(CMemoryPool *mp, CMDAccessor *md_accessor, const Query *orig_query,
                                               const CDXLNode *dxlnode, bool can_set_tag,
                                               DistributionHashOpsKind distribution_hashops);

  // helper for converting wide character string to regular string
  static CHAR *CreateMultiByteCharStringFromWCString(const WCHAR *wcstr);

  // set cost model parameters
  static void SetCostModelParams(ICostModel *cost_model);

  // generate an instance of optimizer cost model
  static ICostModel *GetCostModel(CMemoryPool *mp, ULONG num_segments);

  // print warning messages for columns with missing statistics
  static void PrintMissingStatsWarning(CMemoryPool *mp, CMDAccessor *md_accessor, IMdIdArray *col_stats,
                                       MdidHashSet *phsmdidRel);

 public:
  // convert Query->DXL->LExpr->Optimize->PExpr->DXL
  static char *Optimize(Query *query);

  // optimize Query->DXL->LExpr->Optimize->PExpr->DXL->PlannedStmt
  static PlannedStmt *GPOPTOptimizedPlan(Query *query, SOptContext *gpopt_context);

  // enable/disable a given xforms
  static bool SetXform(char *xform_str, bool should_disable);
};

#endif  // COptTasks_H

// EOF
