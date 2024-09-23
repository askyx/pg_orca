//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformPushGbBelowUnion.h
//
//	@doc:
//		Push grouping below Union operation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbBelowUnion_H
#define GPOPT_CXformPushGbBelowUnion_H

#include "gpopt/operators/CLogicalUnion.h"
#include "gpopt/xforms/CXformPushGbBelowSetOp.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushGbBelowUnion
//
//	@doc:
//		Push grouping below Union operation
//
//---------------------------------------------------------------------------
class CXformPushGbBelowUnion : public CXformPushGbBelowSetOp<CLogicalUnion> {
 private:
 public:
  CXformPushGbBelowUnion(const CXformPushGbBelowUnion &) = delete;

  // ctor
  explicit CXformPushGbBelowUnion(CMemoryPool *mp) : CXformPushGbBelowSetOp<CLogicalUnion>(mp) {}

  // dtor
  ~CXformPushGbBelowUnion() override = default;

  // Compatibility function
  bool FCompatible(CXform::EXformId exfid) override { return ExfPushGbBelowUnion != exfid; }

  // ident accessors
  EXformId Exfid() const override { return ExfPushGbBelowUnion; }

  const char *SzId() const override { return "CXformPushGbBelowUnion"; }

};  // class CXformPushGbBelowUnion

}  // namespace gpopt

#endif  // !GPOPT_CXformPushGbBelowUnion_H

// EOF
