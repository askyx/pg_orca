

#include <iostream>

#include "gpopt/CGPOptimizer.h"
#include "gpopt/config/config.h"

extern "C" {

#include <postgres.h>
#include <fmgr.h>

#include <commands/explain.h>
#include <optimizer/planner.h>
#include <utils/elog.h>
#include <utils/guc.h>
}

static bool init = false;

static planner_hook_type prev_planner_hook = nullptr;
static ExplainOneQuery_hook_type prev_explain_hook = nullptr;

namespace optimizer {

gpdxl::OptConfig config;

static PlannedStmt *pg_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
  if (!config.enable_optimizer)
    return standard_planner(parse, query_string, cursorOptions, boundParams);

  if (!init) {
    InitGPOPT();
    init = true;
  }
  switch (parse->commandType) {
    case CMD_SELECT:
      try {
        return CGPOptimizer::GPOPTOptimizedPlan(parse, &config);
      } catch (const std::exception &e) {
        elog(WARNING, "pg_orca Failed to plan query, get error: %s", e.what());
        return standard_planner(parse, query_string, cursorOptions, boundParams);
      } catch (...) {
        elog(WARNING, "pg_orca Failed to plan query, get unknown error");
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
  if (config.enable_optimizer)
    ExplainPropertyText("Optimizer", "pg_orca", es);
}
}  // namespace optimizer

extern "C" {

PG_MODULE_MAGIC;

void _PG_init(void) {
  // clang-format off
  DefineCustomBoolVariable(
    "pg_orca.enable_orca",
    "use orca planner.",
    NULL,
    &optimizer::config.enable_optimizer,
    false,
    PGC_SUSET,
    0,
    NULL,
    NULL,
    NULL
  );

  DefineCustomBoolVariable(
    "pg_orca.enable_new_planner",
    "use orca planner.",
    NULL,
    &optimizer::config.enable_new_planner_generation,
    false,
    PGC_SUSET,
    0,
    NULL,
    NULL,
    NULL
  );
  // clang-format on

  prev_planner_hook = planner_hook;
  planner_hook = optimizer::pg_planner;

  prev_explain_hook = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
  ExplainOneQuery_hook = optimizer::ExplainOneQuery;
}
}