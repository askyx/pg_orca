//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPatternLeaf.h
//
//	@doc:
//		Pattern that matches a single leaf
//---------------------------------------------------------------------------
#ifndef GPOPT_CPatternLeaf_H
#define GPOPT_CPatternLeaf_H

#include "gpopt/operators/CPattern.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CPatternLeaf
//
//	@doc:
//		Pattern that matches a single leaf
//
//---------------------------------------------------------------------------
class CPatternLeaf : public CPattern {
 private:
 public:
  CPatternLeaf(const CPatternLeaf &) = delete;

  // ctor
  explicit CPatternLeaf(CMemoryPool *mp) : CPattern(mp) {}

  // dtor
  ~CPatternLeaf() override = default;

  // check if operator is a pattern leaf
  bool FLeaf() const override { return true; }

  // ident accessors
  EOperatorId Eopid() const override { return EopPatternLeaf; }

  // return a string for operator name
  const char *SzId() const override { return "CPatternLeaf"; }

};  // class CPatternLeaf

}  // namespace gpopt

#endif  // !GPOPT_CPatternLeaf_H

// EOF
