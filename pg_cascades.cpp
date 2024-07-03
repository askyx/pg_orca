

#include <iostream>

#include "gpopt/CGPOptimizer.h"

extern "C" {

#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"
#include "utils/elog.h"
#include "utils/guc.h"
}

static bool pg_cascades_enable_pg_cascades = false;

namespace optimizer {

static PlannedStmt *pg_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
  if (!pg_cascades_enable_pg_cascades)
    return standard_planner(parse, query_string, cursorOptions, boundParams);

  InitGPOPT();
  switch (parse->commandType) {
    case CMD_SELECT:
      try {
        bool error;
        return GPOPTOptimizedPlan(parse, &error);
      } catch (const std::exception &e) {
        elog(WARNING, "pg_cascades Failed to plan query, get error: %s", e.what());
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
}  // namespace optimizer

extern "C" {

PG_MODULE_MAGIC;

static planner_hook_type prev_planner_hook = NULL;

void _PG_init(void) {
  DefineCustomBoolVariable("pg_cascades.enable_pg_cascades", "use cascade planner.", NULL,
                           &pg_cascades_enable_pg_cascades, true, PGC_SUSET, 0, NULL, NULL, NULL);

  prev_planner_hook = planner_hook;
  planner_hook = optimizer::pg_planner;
}
}