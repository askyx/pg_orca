//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CStatsPredJoin.h
//
//	@doc:
//		Join predicate used for join cardinality estimation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredJoin_H
#define GPNAUCRATES_CStatsPredJoin_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPredJoin
//
//	@doc:
//		Join predicate used for join cardinality estimation
//---------------------------------------------------------------------------
class CStatsPredJoin : public CRefCount {
 private:
  // column id
  uint32_t m_colidOuter;

  // comparison type
  CStatsPred::EStatsCmpType m_stats_cmp_type;

  // column id
  uint32_t m_colidInner;

 public:
  CStatsPredJoin &operator=(CStatsPredJoin &) = delete;

  CStatsPredJoin(const CStatsPredJoin &) = delete;

  // c'tor
  CStatsPredJoin(uint32_t colid1, CStatsPred::EStatsCmpType stats_cmp_type, uint32_t colid2)
      : m_colidOuter(colid1), m_stats_cmp_type(stats_cmp_type), m_colidInner(colid2) {}

  // accessors
  bool HasValidColIdOuter() const { return UINT32_MAX != m_colidOuter; }

  uint32_t ColIdOuter() const { return m_colidOuter; }

  // comparison type
  CStatsPred::EStatsCmpType GetCmpType() const { return m_stats_cmp_type; }

  bool HasValidColIdInner() const { return UINT32_MAX != m_colidInner; }

  uint32_t ColIdInner() const { return m_colidInner; }

  // d'tor
  ~CStatsPredJoin() override = default;

};  // class CStatsPredJoin

// array of filters
using CStatsPredJoinArray = CDynamicPtrArray<CStatsPredJoin, CleanupRelease>;
}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_CStatsPredJoin_H

// EOF
