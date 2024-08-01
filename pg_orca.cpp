

#include <iostream>

#include "gpopt/CGPOptimizer.h"

extern "C" {

#include <postgres.h>
#include <fmgr.h>

#include <commands/explain.h>
#include <optimizer/planner.h>
#include <utils/elog.h>
#include <utils/guc.h>
}

static bool pg_orca_enable_pg_orca = false;
static bool init = false;

static planner_hook_type prev_planner_hook = nullptr;
static ExplainOneQuery_hook_type prev_explain_hook = nullptr;

namespace optimizer {

static PlannedStmt *pg_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
  if (!pg_orca_enable_pg_orca)
    return standard_planner(parse, query_string, cursorOptions, boundParams);

  if (!init) {
    InitGPOPT();
    init = true;
  }
  switch (parse->commandType) {
    case CMD_SELECT:
      try {
        bool error;
        return GPOPTOptimizedPlan(parse, &error);
      } catch (const std::exception &e) {
        elog(WARNING, "pg_orca Failed to plan query, get error: %s", e.what());
        return standard_planner(parse, query_string, cursorOptions, boundParams);
      }
      break;

    case CMD_INSERT:
    case CMD_UPDATE:
    case CMD_DELETE:
    case CMD_MERGE:
    case CMD_UTILITY:
    case CMD_NOTHING:
    case CMD_UNKNOWN:
      return standard_planner(parse, query_string, cursorOptions, boundParams);
      break;

    default:
      elog(ERROR, "unkonwn command type: %d", parse->commandType);
      break;
  }
}

static void ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
                            const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv) {
  prev_explain_hook(query, cursorOptions, into, es, queryString, params, queryEnv);
  if (pg_orca_enable_pg_orca)
    ExplainPropertyText("Optimizer", "pg_orca", es);
}
}  // namespace optimizer

extern "C" {

PG_MODULE_MAGIC;

void _PG_init(void) {
  DefineCustomBoolVariable("pg_orca.enable_orca", "use orca planner.", NULL, &pg_orca_enable_pg_orca, true, PGC_SUSET,
                           0, NULL, NULL, NULL);

  prev_planner_hook = planner_hook;
  planner_hook = optimizer::pg_planner;

  prev_explain_hook = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
  ExplainOneQuery_hook = optimizer::ExplainOneQuery;
}
}