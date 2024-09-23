//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLeftSemiJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Semi Joins
//---------------------------------------------------------------------------

#include "naucrates/statistics/CLeftSemiJoinStatsProcessor.h"

#include "naucrates/statistics/CGroupByStatsProcessor.h"

using namespace gpopt;

// return statistics object after performing LSJ operation
CStatistics *CLeftSemiJoinStatsProcessor::CalcLSJoinStatsStatic(CMemoryPool *mp, const IStatistics *outer_stats_input,
                                                                const IStatistics *inner_stats_input,
                                                                CStatsPredJoinArray *join_preds_stats) {
  GPOS_ASSERT(nullptr != outer_stats_input);
  GPOS_ASSERT(nullptr != inner_stats_input);
  GPOS_ASSERT(nullptr != join_preds_stats);

  const uint32_t length = join_preds_stats->Size();

  // iterate over all inner columns and perform a group by to remove duplicates
  std::vector<uint32_t> inner_colids;
  for (uint32_t ul = 0; ul < length; ul++) {
    if ((*join_preds_stats)[ul]->HasValidColIdInner()) {
      uint32_t colid = ((*join_preds_stats)[ul])->ColIdInner();
      inner_colids.push_back(colid);
    }
  }

  IStatistics *inner_stats = CGroupByStatsProcessor::CalcGroupByStats(
      mp, dynamic_cast<const CStatistics *>(inner_stats_input), inner_colids, {},
      nullptr  // keys: no keys, use all grouping cols
  );

  const CStatistics *outer_stats = dynamic_cast<const CStatistics *>(outer_stats_input);
  CStatistics *semi_join_stats = CJoinStatsProcessor::SetResultingJoinStats(
      mp, outer_stats->GetStatsConfig(), outer_stats, inner_stats, join_preds_stats,
      IStatistics::EsjtLeftSemiJoin /* esjt */, true /* DoIgnoreLASJHistComputation */
  );

  inner_stats->Release();

  return semi_join_stats;
}

// EOF
