//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDefaultComparator.h
//
//	@doc:
//		Default comparator for IDatum instances
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CDefaultComparator_H
#define GPOPT_CDefaultComparator_H

#include "gpopt/base/IComparator.h"
#include "gpos/base.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/traceflags/traceflags.h"

namespace gpmd {
// fwd declarations
class IMDId;
}  // namespace gpmd

namespace gpnaucrates {
// fwd declarations
class IDatum;
}  // namespace gpnaucrates

namespace gpopt {
using namespace gpmd;
using namespace gpnaucrates;
using namespace gpos;

// fwd declarations
class IConstExprEvaluator;

//---------------------------------------------------------------------------
//	@class:
//		CDefaultComparator
//
//	@doc:
//		Default comparator for IDatum instances. It is a singleton accessed
//		via CompGetInstance.
//
//---------------------------------------------------------------------------
class CDefaultComparator : public IComparator {
 private:
  // constant expression evaluator
  IConstExprEvaluator *m_pceeval;

  // construct a comparison expression from the given components and evaluate it
  bool FEvalComparison(CMemoryPool *mp, const IDatum *datum1, const IDatum *datum2, IMDType::ECmpType cmp_type) const;

  // return true iff we should use the internal (stats-based) evaluation
  static bool FUseInternalEvaluator(const IDatum *datum1, const IDatum *datum2, bool *can_use_external_evaluator);

 public:
  CDefaultComparator(const CDefaultComparator &) = delete;

  // ctor
  CDefaultComparator(IConstExprEvaluator *pceeval);

  // dtor
  ~CDefaultComparator() override = default;

  // tests if the two arguments are equal
  bool Equals(const IDatum *datum1, const IDatum *datum2) const override;

  // tests if the first argument is less than the second
  bool IsLessThan(const IDatum *datum1, const IDatum *datum2) const override;

  // tests if the first argument is less or equal to the second
  bool IsLessThanOrEqual(const IDatum *datum1, const IDatum *datum2) const override;

  // tests if the first argument is greater than the second
  bool IsGreaterThan(const IDatum *datum1, const IDatum *datum2) const override;

  // tests if the first argument is greater or equal to the second
  bool IsGreaterThanOrEqual(const IDatum *datum1, const IDatum *datum2) const override;

};  // CDefaultComparator
}  // namespace gpopt

#endif  // !CDefaultComparator_H

// EOF
