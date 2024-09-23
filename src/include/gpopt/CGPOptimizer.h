//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CGPOptimizer.h
//
//	@doc:
//		Entry point to GP optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef CGPOptimizer_H
#define CGPOptimizer_H

extern "C" {
#include <postgres.h>

#include <nodes/params.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
}

namespace gpdxl {
class OptConfig;
}

class CGPOptimizer {
 public:
  // optimize given query using GP optimizer
  static PlannedStmt *GPOPTOptimizedPlan(Query *query, gpdxl::OptConfig *config);

  // gpopt initialize and terminate
  static void InitGPOPT();

  static void TerminateGPOPT();
};

extern "C" {

extern void InitGPOPT();
extern void TerminateGPOPT();
}

#endif  // CGPOptimizer_H
