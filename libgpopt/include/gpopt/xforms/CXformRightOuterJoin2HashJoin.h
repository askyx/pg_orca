//---------------------------------------------------------------------------
//	Greenplum Database
//  Copyright (c) 2020 VMware, Inc.
//
//	@filename:
//		CXformRightOuterJoin2HashJoin.h
//
//	@doc:
//		Transform right outer join to right outer hash join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformRightOuterJoin2HashJoin_H
#define GPOPT_CXformRightOuterJoin2HashJoin_H

#include "gpopt/xforms/CXformImplementation.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformRightOuterJoin2HashJoin
//
//	@doc:
//		Transform right outer join to right outer hash join
//
//---------------------------------------------------------------------------
class CXformRightOuterJoin2HashJoin : public CXformImplementation {
 private:
 public:
  CXformRightOuterJoin2HashJoin(const CXformRightOuterJoin2HashJoin &) = delete;

  // ctor
  explicit CXformRightOuterJoin2HashJoin(CMemoryPool *mp);

  // dtor
  ~CXformRightOuterJoin2HashJoin() override = default;

  // ident accessors
  EXformId Exfid() const override { return ExfRightOuterJoin2HashJoin; }

  // return a string for xform name
  const char *SzId() const override { return "CXformRightOuterJoin2HashJoin"; }

  // compute xform promise for a given expression handle
  EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

  // actual transform
  void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const override;

};  // class CXformRightOuterJoin2HashJoin

}  // namespace gpopt

#endif  // !GPOPT_CXformRightOuterJoin2HashJoin_H

// EOF
