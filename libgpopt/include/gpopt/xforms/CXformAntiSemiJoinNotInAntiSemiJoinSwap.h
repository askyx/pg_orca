//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinNotInAntiSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join (NotIn) and anti semi-join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinNotInAntiSemiJoinSwap_H
#define GPOPT_CXformAntiSemiJoinNotInAntiSemiJoinSwap_H

#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/xforms/CXformJoinSwap.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinNotInAntiSemiJoinSwap
//
//	@doc:
//		Swap cascaded anti semi-join (NotIn) and anti semi-join
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinNotInAntiSemiJoinSwap
    : public CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftAntiSemiJoin> {
 private:
 public:
  CXformAntiSemiJoinNotInAntiSemiJoinSwap(const CXformAntiSemiJoinNotInAntiSemiJoinSwap &) = delete;

  // ctor
  explicit CXformAntiSemiJoinNotInAntiSemiJoinSwap(CMemoryPool *mp)
      : CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftAntiSemiJoin>(mp) {}

  // dtor
  ~CXformAntiSemiJoinNotInAntiSemiJoinSwap() override = default;

  // Compatibility function
  bool FCompatible(CXform::EXformId exfid) override { return ExfAntiSemiJoinAntiSemiJoinNotInSwap != exfid; }

  // ident accessors
  EXformId Exfid() const override { return ExfAntiSemiJoinNotInAntiSemiJoinSwap; }

  const char *SzId() const override { return "CXformAntiSemiJoinNotInAntiSemiJoinSwap"; }

};  // class CXformAntiSemiJoinNotInAntiSemiJoinSwap

}  // namespace gpopt

#endif  // !GPOPT_CXformAntiSemiJoinNotInAntiSemiJoinSwap_H

// EOF
