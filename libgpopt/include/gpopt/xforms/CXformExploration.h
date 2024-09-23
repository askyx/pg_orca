//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformExploration.h
//
//	@doc:
//		Base class for exploration transforms
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExploration_H
#define GPOPT_CXformExploration_H

#include "gpopt/xforms/CXform.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformExploration
//
//	@doc:
//		Base class for all explorations
//
//---------------------------------------------------------------------------
class CXformExploration : public CXform {
 private:
 public:
  CXformExploration(const CXformExploration &) = delete;

  // ctor
  explicit CXformExploration(CExpression *pexpr);

  // dtor
  ~CXformExploration() override;

  // type of operator
  bool FExploration() const override {
    GPOS_ASSERT(!FSubstitution() && !FImplementation());
    return true;
  }

  // is transformation a subquery unnesting (Subquery To Apply) xform?
  virtual bool FSubqueryUnnesting() const { return false; }

  // is transformation an Apply decorrelation (Apply To Join) xform?
  virtual bool FApplyDecorrelating() const { return false; }

  // do stats need to be computed before applying xform?
  virtual bool FNeedsStats() const { return false; }

  // conversion function
  static CXformExploration *Pxformexp(CXform *pxform) {
    GPOS_ASSERT(nullptr != pxform);
    GPOS_ASSERT(pxform->FExploration());

    return dynamic_cast<CXformExploration *>(pxform);
  }

};  // class CXformExploration

}  // namespace gpopt

#endif  // !GPOPT_CXformExploration_H

// EOF
