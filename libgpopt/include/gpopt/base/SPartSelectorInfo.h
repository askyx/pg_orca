//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2020-Present VMware, Inc. or its affiliates
//---------------------------------------------------------------------------

#ifndef GPOPT_CPartSelectorInfo_H
#define GPOPT_CPartSelectorInfo_H

#include <gpopt/operators/CExpression.h>
#include <naucrates/statistics/IStatistics.h>

namespace gpopt {
struct SPartSelectorInfoEntry {
  // selector id
  uint32_t m_selector_id;

  // filter stored in the partition selector
  CExpression *m_filter_expr;

  // statistics of the subtree of the partition selector
  IStatistics *m_stats;

  SPartSelectorInfoEntry(uint32_t mSelectorId, CExpression *mFilterExpr, IStatistics *mStats)
      : m_selector_id(mSelectorId), m_filter_expr(mFilterExpr), m_stats(mStats) {}

  ~SPartSelectorInfoEntry() {
    m_filter_expr->Release();
    m_stats->Release();
  }
};

using SPartSelectorInfo = CHashMap<uint32_t, SPartSelectorInfoEntry, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                   CleanupDelete<uint32_t>, CleanupDelete<SPartSelectorInfoEntry>>;

}  // namespace gpopt
#endif  // !GPOPT_CPartSelectorInfo_H

// EOF
