//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IDatum.cpp
//
//	@doc:
//
//---------------------------------------------------------------------------

#include "naucrates/base/IDatum.h"

#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpnaucrates;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreEqual
//
//	@doc:
//		Equality based on mapping to int64_t or CDouble
//
//---------------------------------------------------------------------------
bool IDatum::StatsAreEqual(const IDatum *datum) const {
  GPOS_ASSERT(nullptr != datum);

  // datums can be compared based on either int64_t or Doubles or BYTEA values
#ifdef GPOS_DEBUG
  bool is_double_comparison = this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif  // GPOS_DEBUG
  bool is_lint_comparison = this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

  GPOS_ASSERT(is_double_comparison || is_lint_comparison);

  if (this->IsNull()) {
    // nulls are equal from stats point of view
    return datum->IsNull();
  }

  if (datum->IsNull()) {
    return false;
  }

  if (is_lint_comparison) {
    int64_t l1 = this->GetLINTMapping();
    int64_t l2 = datum->GetLINTMapping();
    return l1 == l2;
  }

  GPOS_ASSERT(is_double_comparison);

  CDouble d1 = this->GetDoubleMapping();
  CDouble d2 = datum->GetDoubleMapping();
  CDouble diff = d1 - d2;
  return diff.Absolute() <= CStatistics::Epsilon;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreLessThan
//
//	@doc:
//		Less-than based on mapping to int64_t or CDouble
//
//---------------------------------------------------------------------------
bool IDatum::StatsAreLessThan(const IDatum *datum) const {
  GPOS_ASSERT(nullptr != datum);

  // datums can be compared based on either int64_t or Doubles or BYTEA values
#ifdef GPOS_DEBUG
  bool is_double_comparison = this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif  // GPOS_DEBUG
  bool is_lint_comparison = this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

  GPOS_ASSERT(is_double_comparison || is_lint_comparison);

  if (this->IsNull()) {
    // nulls are less than everything else except nulls
    return !(datum->IsNull());
  }

  if (datum->IsNull()) {
    return false;
  }

  if (is_lint_comparison) {
    int64_t l1 = this->GetLINTMapping();
    int64_t l2 = datum->GetLINTMapping();
    return l1 < l2;
  }

  GPOS_ASSERT(is_double_comparison);

  CDouble d1 = this->GetDoubleMapping();
  CDouble d2 = datum->GetDoubleMapping();
  CDouble diff = d2 - d1;
  return diff > CStatistics::Epsilon;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::GetStatsDistanceFrom
//
//	@doc:
//		Distance function based on mapping to int64_t or CDouble
//
//---------------------------------------------------------------------------
CDouble IDatum::GetStatsDistanceFrom(const IDatum *datum) const {
  GPOS_ASSERT(nullptr != datum);

  // datums can be compared based on either int64_t or Doubles or BYTEA values
#ifdef GPOS_DEBUG
  bool is_double_comparison = this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif  // GPOS_DEBUG
  bool is_lint_comparison = this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

  GPOS_ASSERT(is_double_comparison || is_lint_comparison);

  if (this->IsNull()) {
    // nulls are equal from stats point of view
    return datum->IsNull();
  }

  if (datum->IsNull()) {
    return false;
  }

  if (is_lint_comparison) {
    int64_t l1 = this->GetLINTMapping();
    int64_t l2 = datum->GetLINTMapping();
    return l1 - l2;
  }

  GPOS_ASSERT(is_double_comparison);

  CDouble d1 = this->GetDoubleMapping();
  CDouble d2 = datum->GetDoubleMapping();
  return d1 - d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::GetValAsDouble
//
//	@doc:
//		 Return double representation of mapping value
//
//---------------------------------------------------------------------------
CDouble IDatum::GetValAsDouble() const {
  if (IsNull()) {
    return CDouble(0.0);
  }

  if (IsDatumMappableToLINT()) {
    return CDouble(GetLINTMapping());
  }

  return CDouble(GetDoubleMapping());
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreComparable
//
//	@doc:
//		Check if the given pair of datums are stats comparable
//
//---------------------------------------------------------------------------
bool IDatum::StatsAreComparable(const IDatum *datum) const {
  GPOS_ASSERT(nullptr != datum);

  // datums can be compared based on either int64_t or Doubles or BYTEA values
  bool is_double_comparison = this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
  bool is_lint_comparison = this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

  return is_double_comparison || is_lint_comparison;
}

// EOF
