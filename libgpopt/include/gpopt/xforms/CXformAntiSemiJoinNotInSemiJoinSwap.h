//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinNotInSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join (NotIn) and semi-join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinNotInSemiJoinSwap_H
#define GPOPT_CXformAntiSemiJoinNotInSemiJoinSwap_H

#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/xforms/CXformJoinSwap.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinNotInSemiJoinSwap
//
//	@doc:
//		Swap cascaded anti semi-join (NotIn) and semi-join
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinNotInSemiJoinSwap : public CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftSemiJoin> {
 private:
 public:
  CXformAntiSemiJoinNotInSemiJoinSwap(const CXformAntiSemiJoinNotInSemiJoinSwap &) = delete;

  // ctor
  explicit CXformAntiSemiJoinNotInSemiJoinSwap(CMemoryPool *mp)
      : CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftSemiJoin>(mp) {}

  // dtor
  ~CXformAntiSemiJoinNotInSemiJoinSwap() override = default;

  // Compatibility function
  bool FCompatible(CXform::EXformId exfid) override { return ExfSemiJoinAntiSemiJoinNotInSwap != exfid; }

  // ident accessors
  EXformId Exfid() const override { return ExfAntiSemiJoinNotInSemiJoinSwap; }

  const char *SzId() const override { return "CXformAntiSemiJoinNotInSemiJoinSwap"; }

};  // class CXformAntiSemiJoinNotInSemiJoinSwap

}  // namespace gpopt

#endif  // !GPOPT_CXformAntiSemiJoinNotInSemiJoinSwap_H

// EOF
