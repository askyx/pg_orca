//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSemiJoinSemiJoinSwap.h
//
//	@doc:
//		Swap two cascaded semi-joins
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSemiJoinSemiJoinSwap_H
#define GPOPT_CXformSemiJoinSemiJoinSwap_H

#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/xforms/CXformJoinSwap.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSemiJoinSemiJoinSwap
//
//	@doc:
//		Swap two cascaded semi-joins
//
//---------------------------------------------------------------------------
class CXformSemiJoinSemiJoinSwap : public CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalLeftSemiJoin> {
 private:
 public:
  CXformSemiJoinSemiJoinSwap(const CXformSemiJoinSemiJoinSwap &) = delete;

  // ctor
  explicit CXformSemiJoinSemiJoinSwap(CMemoryPool *mp)
      : CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalLeftSemiJoin>(mp) {}

  // dtor
  ~CXformSemiJoinSemiJoinSwap() override = default;

  // Compatibility function
  bool FCompatible(CXform::EXformId exfid) override { return ExfSemiJoinSemiJoinSwap != exfid; }

  // ident accessors
  EXformId Exfid() const override { return ExfSemiJoinSemiJoinSwap; }

  const char *SzId() const override { return "CXformSemiJoinSemiJoinSwap"; }

};  // class CXformSemiJoinSemiJoinSwap

}  // namespace gpopt

#endif  // !GPOPT_CXformSemiJoinSemiJoinSwap_H

// EOF
