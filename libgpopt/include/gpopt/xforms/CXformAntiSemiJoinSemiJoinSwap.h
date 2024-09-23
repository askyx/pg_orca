//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join and semi-join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinSemiJoinSwap_H
#define GPOPT_CXformAntiSemiJoinSemiJoinSwap_H

#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/xforms/CXformJoinSwap.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinSemiJoinSwap
//
//	@doc:
//		Swap cascaded anti semi-join and semi-join
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinSemiJoinSwap : public CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalLeftSemiJoin> {
 private:
 public:
  CXformAntiSemiJoinSemiJoinSwap(const CXformAntiSemiJoinSemiJoinSwap &) = delete;

  // ctor
  explicit CXformAntiSemiJoinSemiJoinSwap(CMemoryPool *mp)
      : CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalLeftSemiJoin>(mp) {}

  // dtor
  ~CXformAntiSemiJoinSemiJoinSwap() override = default;

  // Compatibility function
  bool FCompatible(CXform::EXformId exfid) override { return ExfSemiJoinAntiSemiJoinSwap != exfid; }

  // ident accessors
  EXformId Exfid() const override { return ExfAntiSemiJoinSemiJoinSwap; }

  const char *SzId() const override { return "CXformAntiSemiJoinSemiJoinSwap"; }

};  // class CXformAntiSemiJoinSemiJoinSwap

}  // namespace gpopt

#endif  // !GPOPT_CXformAntiSemiJoinSemiJoinSwap_H

// EOF
