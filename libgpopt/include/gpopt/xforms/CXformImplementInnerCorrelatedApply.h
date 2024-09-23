//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementInnerCorrelatedApply.h
//
//	@doc:
//		Transform inner correlated apply to physical inner correlated apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementInnerCorrelatedApply_H
#define GPOPT_CXformImplementInnerCorrelatedApply_H

#include "gpopt/operators/CLogicalInnerCorrelatedApply.h"
#include "gpopt/operators/CPhysicalCorrelatedInnerNLJoin.h"
#include "gpopt/xforms/CXformImplementCorrelatedApply.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//-------------------------------------------------------------------------
//	@class:
//		CXformImplementInnerCorrelatedApply
//
//	@doc:
//		Transform inner correlated apply to physical inner correlated apply
//
//-------------------------------------------------------------------------
class CXformImplementInnerCorrelatedApply
    : public CXformImplementCorrelatedApply<CLogicalInnerCorrelatedApply, CPhysicalCorrelatedInnerNLJoin> {
 private:
 public:
  CXformImplementInnerCorrelatedApply(const CXformImplementInnerCorrelatedApply &) = delete;

  // ctor
  explicit CXformImplementInnerCorrelatedApply(CMemoryPool *mp)
      : CXformImplementCorrelatedApply<CLogicalInnerCorrelatedApply, CPhysicalCorrelatedInnerNLJoin>(mp) {}

  // dtor
  ~CXformImplementInnerCorrelatedApply() override = default;

  // ident accessors
  EXformId Exfid() const override { return ExfImplementInnerCorrelatedApply; }

  const char *SzId() const override { return "CXformImplementInnerCorrelatedApply"; }

};  // class CXformImplementInnerCorrelatedApply

}  // namespace gpopt

#endif  // !GPOPT_CXformImplementInnerCorrelatedApply_H

// EOF
