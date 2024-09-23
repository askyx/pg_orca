//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformPushGbBelowUnionAll.h
//
//	@doc:
//		Push grouping below UnionAll operation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbBelowUnionAll_H
#define GPOPT_CXformPushGbBelowUnionAll_H

#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/xforms/CXformPushGbBelowSetOp.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushGbBelowUnionAll
//
//	@doc:
//		Push grouping below UnionAll operation
//
//---------------------------------------------------------------------------
class CXformPushGbBelowUnionAll : public CXformPushGbBelowSetOp<CLogicalUnionAll> {
 private:
 public:
  CXformPushGbBelowUnionAll(const CXformPushGbBelowUnionAll &) = delete;

  // ctor
  explicit CXformPushGbBelowUnionAll(CMemoryPool *mp) : CXformPushGbBelowSetOp<CLogicalUnionAll>(mp) {}

  // dtor
  ~CXformPushGbBelowUnionAll() override = default;

  // Compatibility function
  bool FCompatible(CXform::EXformId exfid) override { return ExfPushGbBelowUnionAll != exfid; }

  // ident accessors
  EXformId Exfid() const override { return ExfPushGbBelowUnionAll; }

  const char *SzId() const override { return "CXformPushGbBelowUnionAll"; }

};  // class CXformPushGbBelowUnionAll

}  // namespace gpopt

#endif  // !GPOPT_CXformPushGbBelowUnionAll_H

// EOF
