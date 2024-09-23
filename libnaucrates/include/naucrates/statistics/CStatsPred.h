//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPred.h
//
//	@doc:
//		Filter on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPred_H
#define GPNAUCRATES_CStatsPred_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

namespace gpnaucrates {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPred
//
//	@doc:
//		Filter on statistics
//---------------------------------------------------------------------------
class CStatsPred : public CRefCount {
 public:
  enum EStatsPredType {
    EsptPoint,        // filter with literals
    EsptArrayCmp,     // filter with = ANY/ALL(ARRAY[...])
    EsptConj,         // conjunctive filter
    EsptDisj,         // disjunctive filter
    EsptLike,         // LIKE filter
    EsptUnsupported,  // unsupported filter for statistics calculation

    EsptSentinel
  };

  // comparison types for stats computation
  enum EStatsCmpType {
    EstatscmptEq,       // equals
    EstatscmptNEq,      // not equals
    EstatscmptL,        // less than
    EstatscmptLEq,      // less or equal to
    EstatscmptG,        // greater than
    EstatscmptGEq,      // greater or equal to
    EstatscmptIDF,      // is distinct from
    EstatscmptINDF,     // is not distinct from
    EstatscmptLike,     // LIKE predicate comparison
    EstatscmptNotLike,  // NOT LIKE predicate comparison
    // NDV comparison for equality predicate on columns with functions, ex f(a) = b or a = f(b)
    EstatscmptEqNDV,
    EstatscmptOther
  };

 private:
 protected:
  // column id
  uint32_t m_colid;

  // CStatsPred is recursively traversed to compute cardinality estimates for
  // extended stat. This prevents infinite loop or double count in recursion.
  bool m_is_estimated{false};

 public:
  CStatsPred &operator=(CStatsPred &) = delete;

  CStatsPred(const CStatsPred &) = delete;

  // ctor
  explicit CStatsPred(uint32_t colid) : m_colid(colid) {}

  // dtor
  ~CStatsPred() override = default;

  // accessors
  virtual uint32_t GetColId() const { return m_colid; }

  bool IsAlreadyUsedInScaleFactorEstimation() const { return m_is_estimated; }

  void SetEstimated() { m_is_estimated = true; }

  // type id
  virtual EStatsPredType GetPredStatsType() const = 0;

  // comparison function
  static inline int32_t StatsPredSortCmpFunc(const void *val1, const void *val2);
};  // class CStatsPred

// array of filters
using CStatsPredPtrArry = CDynamicPtrArray<CStatsPred, CleanupRelease>;

// comparison function for sorting predicates
int32_t CStatsPred::StatsPredSortCmpFunc(const void *val1, const void *val2) {
  const CStatsPred *stats_pred1 = *(const CStatsPred **)val1;
  const CStatsPred *stats_pred2 = *(const CStatsPred **)val2;

  return (int32_t)stats_pred1->GetColId() - (int32_t)stats_pred2->GetColId();
}

}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_CStatsPred_H

// EOF
