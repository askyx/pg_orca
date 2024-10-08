//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CStatistics.cpp
//
//	@doc:
//		Histograms based statistics
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatistics.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpos/common/CBitSet.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/statistics/CInnerJoinStatsProcessor.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftOuterJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CScaleFactorUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;

// default number of rows in relation
const CDouble CStatistics::DefaultRelationRows(1000.0);

// epsilon to be used for various computations
const CDouble CStatistics::Epsilon(0.00001);

// minimum number of rows in relation
const CDouble CStatistics::MinRows(1.0);

// default column width
const CDouble CStatistics::DefaultColumnWidth(8.0);

// default number of distinct values
const CDouble CStatistics::DefaultDistinctValues(1000.0);

// the default value for operators that have no cardinality estimation risk
const uint32_t CStatistics::no_card_est_risk_default_val = 1;

// ctor
CStatistics::CStatistics(CMemoryPool *mp, UlongToHistogramMap *col_histogram_mapping,
                         UlongToDoubleMap *colid_width_mapping, CDouble rows, bool is_empty, uint32_t num_predicates)
    : m_colid_histogram_mapping(col_histogram_mapping),
      m_colid_width_mapping(colid_width_mapping),
      m_rows(rows),
      m_stats_estimation_risk(no_card_est_risk_default_val),
      m_empty(is_empty),
      m_relpages(0),
      m_relallvisible(0),
      m_num_rebinds(1.0),  // by default, a stats object is rebound to parameters only once
      m_num_predicates(num_predicates),
      m_src_upper_bound_NDVs(nullptr),
      m_ext_stats(nullptr),
      m_colid_to_attno_mapping(GPOS_NEW(mp) UlongToIntMap(mp)) {
  GPOS_ASSERT(nullptr != m_colid_histogram_mapping);
  GPOS_ASSERT(nullptr != m_colid_width_mapping);
  GPOS_ASSERT(CDouble(0.0) <= m_rows);

  // hash map for source id -> max source cardinality mapping
  m_src_upper_bound_NDVs = GPOS_NEW(mp) CUpperBoundNDVPtrArray(mp);

  m_stats_conf = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetStatsConf();
}

CStatistics::CStatistics(CMemoryPool *mp, UlongToHistogramMap *col_histogram_mapping,
                         UlongToDoubleMap *colid_width_mapping, CDouble rows, bool is_empty, uint32_t relpages,
                         uint32_t relallvisible, CDouble rebinds, uint32_t num_predicates,
                         const IMDExtStatsInfo *extstats, UlongToIntMap *colid_to_attno_mapping)
    : m_colid_histogram_mapping(col_histogram_mapping),
      m_colid_width_mapping(colid_width_mapping),
      m_rows(rows),
      m_stats_estimation_risk(no_card_est_risk_default_val),
      m_empty(is_empty),
      m_relpages(relpages),
      m_relallvisible(relallvisible),
      m_num_rebinds(rebinds),
      m_num_predicates(num_predicates),
      m_src_upper_bound_NDVs(nullptr),
      m_ext_stats(extstats),
      m_colid_to_attno_mapping(colid_to_attno_mapping) {
  GPOS_ASSERT(nullptr != m_colid_histogram_mapping);
  GPOS_ASSERT(nullptr != m_colid_width_mapping);
  GPOS_ASSERT(CDouble(0.0) <= m_rows);

  // hash map for source id -> max source cardinality mapping
  m_src_upper_bound_NDVs = GPOS_NEW(mp) CUpperBoundNDVPtrArray(mp);

  m_stats_conf = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetStatsConf();
}

// Dtor
CStatistics::~CStatistics() {
  m_colid_histogram_mapping->Release();
  m_colid_width_mapping->Release();
  m_src_upper_bound_NDVs->Release();
  m_colid_to_attno_mapping->Release();
}

// look up the width of a particular column
const CDouble *CStatistics::GetWidth(uint32_t colid) const {
  return m_colid_width_mapping->Find(&colid);
}

//	cap the total number of distinct values (NDVs) in buckets to the number of rows
void CStatistics::CapNDVs(CDouble rows, UlongToHistogramMap *col_histogram_mapping) {
  GPOS_ASSERT(nullptr != col_histogram_mapping);
  UlongToHistogramMapIter col_hist_mapping(col_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    CHistogram *histogram = const_cast<CHistogram *>(col_hist_mapping.Value());
    histogram->CapNDVs(rows);
  }
}

// helper print function
IOstream &CStatistics::OsPrint(IOstream &os) const {
  os << "{" << std::endl;
  os << "Rows = " << Rows() << std::endl;
  os << "Rebinds = " << NumRebinds() << std::endl;

  UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    uint32_t colid = *(col_hist_mapping.Key());
    os << "Col" << colid << ":" << std::endl;
    const CHistogram *histogram = col_hist_mapping.Value();
    histogram->OsPrint(os);
    os << std::endl;
  }

  UlongToDoubleMapIter col_width_map_iterator(m_colid_width_mapping);
  while (col_width_map_iterator.Advance()) {
    uint32_t colid = *(col_width_map_iterator.Key());
    os << "Col" << colid << ":" << std::endl;
    const CDouble *width = col_width_map_iterator.Value();
    os << " width " << (*width) << std::endl;
  }

  const uint32_t length = m_src_upper_bound_NDVs->Size();
  for (uint32_t i = 0; i < length; i++) {
    const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
    upper_bound_NDVs->OsPrint(os);
  }
  os << "StatsEstimationRisk = " << StatsEstimationRisk() << std::endl;
  os << "}" << std::endl;

  return os;
}

//	return the total number of rows for this statistics object
CDouble CStatistics::Rows() const {
  return m_rows;
}

// return the estimated skew of the given column
CDouble CStatistics::GetSkew(uint32_t colid) const {
  CHistogram *histogram = m_colid_histogram_mapping->Find(&colid);
  if (nullptr == histogram) {
    return CDouble(1.0);
  }

  return histogram->GetSkew();
}

// return total width in bytes
CDouble CStatistics::Width() const {
  CDouble total_width(0.0);
  UlongToDoubleMapIter col_width_map_iterator(m_colid_width_mapping);
  while (col_width_map_iterator.Advance()) {
    const CDouble *width = col_width_map_iterator.Value();
    total_width = total_width + (*width);
  }
  return total_width.Ceil();
}

// return the width in bytes of a set of columns
CDouble CStatistics::Width(const std::vector<uint32_t> &colids) const {
  CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
  CDouble total_width(0.0);

  for (auto colid : colids) {
    CDouble *width = m_colid_width_mapping->Find(&colid);
    if (nullptr != width) {
      total_width = total_width + (*width);
    } else {
      CColRef *colref = col_factory->LookupColRef(colid);
      GPOS_ASSERT(nullptr != colref);

      total_width = total_width + CStatisticsUtils::DefaultColumnWidth(colref->RetrieveType());
    }
  }
  return total_width.Ceil();
}

// return width in bytes of a set of columns
CDouble CStatistics::Width(CMemoryPool *mp, CColRefSet *colrefs) const {
  GPOS_ASSERT(nullptr != colrefs);

  auto colids = colrefs->ExtractColIds();

  CDouble width = Width(colids);

  return width;
}

// return dummy statistics object
CStatistics *CStatistics::MakeDummyStats(CMemoryPool *mp, const std::vector<uint32_t> &colids, CDouble rows) {
  // hash map from colid -> histogram for resultant structure
  UlongToHistogramMap *col_histogram_mapping = GPOS_NEW(mp) UlongToHistogramMap(mp);

  // hashmap from colid -> width (double)
  UlongToDoubleMap *colid_width_mapping = GPOS_NEW(mp) UlongToDoubleMap(mp);

  CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

  bool is_empty = (CStatistics::Epsilon >= rows);
  CHistogram::AddDummyHistogramAndWidthInfo(mp, col_factory, col_histogram_mapping, colid_width_mapping, colids,
                                            is_empty);

  CStatistics *stats = GPOS_NEW(mp) CStatistics(mp, col_histogram_mapping, colid_width_mapping, rows, is_empty);
  CreateAndInsertUpperBoundNDVs(mp, stats, colids, rows);

  return stats;
}

// add upper bound ndvs information for a given set of columns
void CStatistics::CreateAndInsertUpperBoundNDVs(CMemoryPool *mp, CStatistics *stats,
                                                const std::vector<uint32_t> &colids, CDouble rows) {
  GPOS_ASSERT(nullptr != stats);

  CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
  CColRefSet *colrefs = GPOS_NEW(mp) CColRefSet(mp);
  for (auto colid : colids) {
    const CColRef *colref = col_factory->LookupColRef(colid);
    if (nullptr != colref) {
      colrefs->Include(colref);
    }
  }

  if (0 < colrefs->Size()) {
    stats->AddCardUpperBound(GPOS_NEW(mp) CUpperBoundNDVs(colrefs, rows));
  } else {
    colrefs->Release();
  }
}

//	return dummy statistics object
CStatistics *CStatistics::MakeDummyStats(CMemoryPool *mp, const std::vector<uint32_t> &col_histogram_mapping,
                                         ULongPtrArray *col_width_mapping, CDouble rows) {
  GPOS_ASSERT(nullptr != col_width_mapping);

  bool is_empty = (CStatistics::Epsilon >= rows);
  CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

  // hash map from colid -> histogram for resultant structure
  UlongToHistogramMap *result_col_histogram_mapping = GPOS_NEW(mp) UlongToHistogramMap(mp);

  for (auto colid : col_histogram_mapping) {
    CColRef *colref = col_factory->LookupColRef(colid);
    GPOS_ASSERT(nullptr != colref);

    // empty histogram
    CHistogram *histogram = CHistogram::MakeDefaultHistogram(mp, colref, is_empty);
    result_col_histogram_mapping->Insert(GPOS_NEW(mp) uint32_t(colid), histogram);
  }

  // hashmap from colid -> width (double)
  UlongToDoubleMap *colid_width_mapping = GPOS_NEW(mp) UlongToDoubleMap(mp);

  const uint32_t num_col_width = col_width_mapping->Size();
  for (uint32_t ul = 0; ul < num_col_width; ul++) {
    uint32_t colid = *(*col_width_mapping)[ul];

    CColRef *colref = col_factory->LookupColRef(colid);
    GPOS_ASSERT(nullptr != colref);

    CDouble width = CStatisticsUtils::DefaultColumnWidth(colref->RetrieveType());
    colid_width_mapping->Insert(GPOS_NEW(mp) uint32_t(colid), GPOS_NEW(mp) CDouble(width));
  }

  CStatistics *stats =
      GPOS_NEW(mp) CStatistics(mp, result_col_histogram_mapping, colid_width_mapping, rows, false /* is_empty */);
  CreateAndInsertUpperBoundNDVs(mp, stats, col_histogram_mapping, rows);

  return stats;
}

//	check if the input statistics from join statistics computation empty
bool CStatistics::IsEmptyJoin(const CStatistics *outer_stats, const CStatistics *inner_side_stats, bool IsLASJ) {
  GPOS_ASSERT(nullptr != outer_stats);
  GPOS_ASSERT(nullptr != inner_side_stats);

  if (IsLASJ) {
    return outer_stats->IsEmpty();
  }

  return outer_stats->IsEmpty() || inner_side_stats->IsEmpty();
}

// Currently, Pstats[Join type] are thin wrappers the C[Join type]StatsProcessor class's method
// for deriving the stat objects for the corresponding join operator

//	return statistics object after performing LOJ operation with another statistics structure
IStatistics *CStatistics::CalcLOJoinStats(CMemoryPool *mp, const IStatistics *other_stats,
                                          CStatsPredJoinArray *join_preds_stats) const {
  return CLeftOuterJoinStatsProcessor::CalcLOJoinStatsStatic(mp, this, other_stats, join_preds_stats);
}

//	return statistics object after performing semi-join with another statistics structure
IStatistics *CStatistics::CalcLSJoinStats(CMemoryPool *mp, const IStatistics *inner_side_stats,
                                          CStatsPredJoinArray *join_preds_stats) const {
  return CLeftSemiJoinStatsProcessor::CalcLSJoinStatsStatic(mp, this, inner_side_stats, join_preds_stats);
}

// return statistics object after performing inner join
IStatistics *CStatistics::CalcInnerJoinStats(CMemoryPool *mp, const IStatistics *other_stats,
                                             CStatsPredJoinArray *join_preds_stats) const {
  return CInnerJoinStatsProcessor::CalcInnerJoinStatsStatic(mp, this, other_stats, join_preds_stats);
}

// return statistics object after performing LASJ
IStatistics *CStatistics::CalcLASJoinStats(CMemoryPool *mp, const IStatistics *other_stats,
                                           CStatsPredJoinArray *join_preds_stats,
                                           bool DoIgnoreLASJHistComputation) const {
  return CLeftAntiSemiJoinStatsProcessor::CalcLASJoinStatsStatic(mp, this, other_stats, join_preds_stats,
                                                                 DoIgnoreLASJHistComputation);
}

//	helper method to copy statistics on columns that are not excluded by bitset
void CStatistics::AddNotExcludedHistograms(CMemoryPool *mp, CBitSet *excluded_cols,
                                           UlongToHistogramMap *col_histogram_mapping) const {
  GPOS_ASSERT(nullptr != excluded_cols);
  GPOS_ASSERT(nullptr != col_histogram_mapping);

  UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    uint32_t colid = *(col_hist_mapping.Key());
    if (!excluded_cols->Get(colid)) {
      const CHistogram *histogram = col_hist_mapping.Value();
      CStatisticsUtils::AddHistogram(mp, colid, histogram, col_histogram_mapping);
    }

    GPOS_CHECK_ABORT;
  }
}

UlongToDoubleMap *CStatistics::CopyWidths(CMemoryPool *mp) const {
  UlongToDoubleMap *widths_copy = GPOS_NEW(mp) UlongToDoubleMap(mp);
  CStatisticsUtils::AddWidthInfo(mp, m_colid_width_mapping, widths_copy);

  return widths_copy;
}

void CStatistics::CopyWidthsInto(CMemoryPool *mp, UlongToDoubleMap *colid_width_mapping) const {
  CStatisticsUtils::AddWidthInfo(mp, m_colid_width_mapping, colid_width_mapping);
}

UlongToHistogramMap *CStatistics::CopyHistograms(CMemoryPool *mp) const {
  // create hash map from colid -> histogram for resultant structure
  UlongToHistogramMap *histograms_copy = GPOS_NEW(mp) UlongToHistogramMap(mp);

  bool is_empty = IsEmpty();

  UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    uint32_t colid = *(col_hist_mapping.Key());
    const CHistogram *histogram = col_hist_mapping.Value();
    CHistogram *histogram_copy = nullptr;
    if (is_empty) {
      histogram_copy = GPOS_NEW(mp) CHistogram(mp, false /* is_well_defined */);
    } else {
      histogram_copy = histogram->CopyHistogram();
    }

    histograms_copy->Insert(GPOS_NEW(mp) uint32_t(colid), histogram_copy);
  }

  return histograms_copy;
}

//	return required props associated with statistics object
CReqdPropRelational *CStatistics::GetReqdRelationalProps(CMemoryPool *mp) const {
  CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
  GPOS_ASSERT(nullptr != col_factory);

  CColRefSet *colrefs = GPOS_NEW(mp) CColRefSet(mp);

  // add columns from histogram map
  UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    uint32_t colid = *(col_hist_mapping.Key());
    CColRef *colref = col_factory->LookupColRef(colid);
    GPOS_ASSERT(nullptr != colref);

    colrefs->Include(colref);
  }

  return GPOS_NEW(mp) CReqdPropRelational(colrefs);
}

// append given statistics to current object
void CStatistics::AppendStats(CMemoryPool *mp, IStatistics *input_stats) {
  CStatistics *stats = CStatistics::CastStats(input_stats);

  CHistogram::AddHistograms(mp, stats->m_colid_histogram_mapping, m_colid_histogram_mapping);
  GPOS_CHECK_ABORT;

  CStatisticsUtils::AddWidthInfo(mp, stats->m_colid_width_mapping, m_colid_width_mapping);
  GPOS_CHECK_ABORT;
}

// copy statistics object
IStatistics *CStatistics::CopyStats(CMemoryPool *mp) const {
  return ScaleStats(mp, CDouble(1.0) /*factor*/);
}

// return a copy of this statistics object scaled by a given factor
IStatistics *CStatistics::ScaleStats(CMemoryPool *mp, CDouble factor) const {
  UlongToHistogramMap *histograms_new = GPOS_NEW(mp) UlongToHistogramMap(mp);
  UlongToDoubleMap *widths_new = GPOS_NEW(mp) UlongToDoubleMap(mp);

  CHistogram::AddHistograms(mp, m_colid_histogram_mapping, histograms_new);
  GPOS_CHECK_ABORT;

  CStatisticsUtils::AddWidthInfo(mp, m_colid_width_mapping, widths_new);
  GPOS_CHECK_ABORT;

  CDouble scaled_num_rows = m_rows * factor;

  m_colid_to_attno_mapping->AddRef();

  // create a scaled stats object
  CStatistics *scaled_stats =
      GPOS_NEW(mp) CStatistics(mp, histograms_new, widths_new, scaled_num_rows, IsEmpty(), RelPages(), RelAllVisible(),
                               NumRebinds(), m_num_predicates, m_ext_stats, m_colid_to_attno_mapping);

  // In the output statistics object, the upper bound source cardinality of the scaled column
  // cannot be greater than the the upper bound source cardinality information maintained in the input
  // statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
  // the minimum of the cardinality upper bound of the source column (in the input hash map)
  // and estimated output cardinality.

  // modify source id to upper bound card information
  CStatisticsUtils::ComputeCardUpperBounds(mp, this, scaled_stats, scaled_num_rows,
                                           CStatistics::EcbmMin /* card_bounding_method */);

  return scaled_stats;
}

//	copy statistics object with re-mapped column ids
IStatistics *CStatistics::CopyStatsWithRemap(CMemoryPool *mp, UlongToColRefMap *colref_mapping, bool must_exist) const {
  GPOS_ASSERT(nullptr != colref_mapping);
  UlongToHistogramMap *histograms_new = GPOS_NEW(mp) UlongToHistogramMap(mp);
  UlongToDoubleMap *widths_new = GPOS_NEW(mp) UlongToDoubleMap(mp);
  UlongToIntMap *attnos_new = GPOS_NEW(mp) UlongToIntMap(mp);

  AddHistogramsWithRemap(mp, m_colid_histogram_mapping, histograms_new, colref_mapping, must_exist);
  AddWidthInfoWithRemap(mp, m_colid_width_mapping, widths_new, colref_mapping, must_exist);
  AddAttnoInfoWithRemap(mp, m_colid_to_attno_mapping, attnos_new, colref_mapping, must_exist);

  // create a copy of the stats object
  CStatistics *stats_copy =
      GPOS_NEW(mp) CStatistics(mp, histograms_new, widths_new, m_rows, IsEmpty(), RelPages(), RelAllVisible(),
                               NumRebinds(), m_num_predicates, m_ext_stats, attnos_new);

  // In the output statistics object, the upper bound source cardinality of the join column
  // cannot be greater than the the upper bound source cardinality information maintained in the input
  // statistics object.

  // copy the upper bound ndv information
  const uint32_t length = m_src_upper_bound_NDVs->Size();
  for (uint32_t i = 0; i < length; i++) {
    const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
    CUpperBoundNDVs *upper_bound_NDVs_copy = upper_bound_NDVs->CopyUpperBoundNDVWithRemap(mp, colref_mapping);

    if (nullptr != upper_bound_NDVs_copy) {
      stats_copy->AddCardUpperBound(upper_bound_NDVs_copy);
    }
  }

  return stats_copy;
}

//	return the column identifiers of all columns whose statistics are
//	maintained by the statistics object
ULongPtrArray *CStatistics::GetColIdsWithStats(CMemoryPool *mp) const {
  ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);

  UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    uint32_t colid = *(col_hist_mapping.Key());
    colids->Append(GPOS_NEW(mp) uint32_t(colid));
  }

  return colids;
}

// return the set of column references we have statistics for
CColRefSet *CStatistics::GetColRefSet(CMemoryPool *mp) const {
  CColRefSet *colrefs = GPOS_NEW(mp) CColRefSet(mp);
  CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

  UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    uint32_t colid = *(col_hist_mapping.Key());
    CColRef *colref = col_factory->LookupColRef(colid);
    GPOS_ASSERT(nullptr != colref);

    colrefs->Include(colref);
  }

  return colrefs;
}

//	append given histograms to current object where the column ids have been re-mapped
void CStatistics::AddHistogramsWithRemap(CMemoryPool *mp, UlongToHistogramMap *src_histograms,
                                         UlongToHistogramMap *dest_histograms, UlongToColRefMap *colref_mapping,
                                         bool
#ifdef GPOS_DEBUG
                                             must_exist
#endif  // GPOS_DEBUG
) {
  UlongToColRefMapIter colref_iterator(colref_mapping);
  while (colref_iterator.Advance()) {
    uint32_t src_colid = *(colref_iterator.Key());
    const CColRef *dest_colref = colref_iterator.Value();
    GPOS_ASSERT_IMP(must_exist, nullptr != dest_colref);

    uint32_t dest_colid = dest_colref->Id();

    const CHistogram *src_histogram = src_histograms->Find(&src_colid);
    if (nullptr != src_histogram) {
      CStatisticsUtils::AddHistogram(mp, dest_colid, src_histogram, dest_histograms);
    }
  }
}

// add width information where the column ids have been re-mapped
void CStatistics::AddWidthInfoWithRemap(CMemoryPool *mp, UlongToDoubleMap *src_width, UlongToDoubleMap *dest_width,
                                        UlongToColRefMap *colref_mapping, bool must_exist) {
  UlongToDoubleMapIter col_width_map_iterator(src_width);
  while (col_width_map_iterator.Advance()) {
    uint32_t colid = *(col_width_map_iterator.Key());
    CColRef *new_colref = colref_mapping->Find(&colid);
    if (must_exist && nullptr == new_colref) {
      continue;
    }

    if (nullptr != new_colref) {
      colid = new_colref->Id();
    }

    if (nullptr == dest_width->Find(&colid)) {
      const CDouble *width = col_width_map_iterator.Value();
      CDouble *width_copy = GPOS_NEW(mp) CDouble(*width);
      bool result GPOS_ASSERTS_ONLY = dest_width->Insert(GPOS_NEW(mp) uint32_t(colid), width_copy);
      GPOS_ASSERT(result);
    }
  }
}

// add attno information where the column ids have been re-mapped
void CStatistics::AddAttnoInfoWithRemap(CMemoryPool *mp, UlongToIntMap *src_attno, UlongToIntMap *dest_attno,
                                        UlongToColRefMap *colref_mapping, bool must_exist) {
  UlongToIntMapIter col_attno_map_iterator(src_attno);
  while (col_attno_map_iterator.Advance()) {
    uint32_t colid = *(col_attno_map_iterator.Key());
    CColRef *new_colref = colref_mapping->Find(&colid);
    if (must_exist && nullptr == new_colref) {
      continue;
    }

    if (nullptr != new_colref) {
      colid = new_colref->Id();
    }

    if (nullptr == dest_attno->Find(&colid)) {
      const int32_t *attno = col_attno_map_iterator.Value();
      int32_t *attno_copy = GPOS_NEW(mp) int32_t(*attno);
      bool result GPOS_ASSERTS_ONLY = dest_attno->Insert(GPOS_NEW(mp) uint32_t(colid), attno_copy);
      GPOS_ASSERT(result);
    }
  }
}

// return the index of the array of upper bound ndvs to which column reference belongs
uint32_t CStatistics::GetIndexUpperBoundNDVs(const CColRef *colref) {
  GPOS_ASSERT(nullptr != colref);

  const uint32_t length = m_src_upper_bound_NDVs->Size();
  for (uint32_t i = 0; i < length; i++) {
    const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
    if (upper_bound_NDVs->IsPresent(colref)) {
      return i;
    }
  }

  return UINT32_MAX;
}

// add upper bound of source cardinality
void CStatistics::AddCardUpperBound(CUpperBoundNDVs *upper_bound_NDVs) {
  GPOS_ASSERT(nullptr != upper_bound_NDVs);

  m_src_upper_bound_NDVs->Append(upper_bound_NDVs);
}

// return the dxl representation of the statistics object
CDXLStatsDerivedRelation *CStatistics::GetDxlStatsDrvdRelation(CMemoryPool *mp, CMDAccessor *md_accessor) const {
  CDXLStatsDerivedColumnArray *dxl_stats_derived_col_array = GPOS_NEW(mp) CDXLStatsDerivedColumnArray(mp);

  UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
  while (col_hist_mapping.Advance()) {
    uint32_t colid = *(col_hist_mapping.Key());
    const CHistogram *histogram = col_hist_mapping.Value();

    CDouble *width = m_colid_width_mapping->Find(&colid);
    GPOS_ASSERT(width);

    CDXLStatsDerivedColumn *dxl_derived_col_stats =
        histogram->TranslateToDXLDerivedColumnStats(md_accessor, colid, *width);
    dxl_stats_derived_col_array->Append(dxl_derived_col_stats);
  }

  return GPOS_NEW(mp) CDXLStatsDerivedRelation(m_rows, IsEmpty(), dxl_stats_derived_col_array);
}

// return the upper bound of ndvs for a column reference
CDouble CStatistics::GetColUpperBoundNDVs(const CColRef *colref) {
  GPOS_ASSERT(nullptr != colref);

  const uint32_t length = m_src_upper_bound_NDVs->Size();
  for (uint32_t i = 0; i < length; i++) {
    const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
    if (upper_bound_NDVs->IsPresent(colref)) {
      return upper_bound_NDVs->UpperBoundNDVs();
    }
  }

  return DefaultDistinctValues;
}

// look up the number of distinct values of a particular column
CDouble CStatistics::GetNDVs(const CColRef *colref) {
  uint32_t colid = colref->Id();
  CHistogram *col_histogram = m_colid_histogram_mapping->Find(&colid);
  if (nullptr != col_histogram) {
    return std::min(col_histogram->GetNumDistinct(), GetColUpperBoundNDVs(colref));
  }

#ifdef GPOS_DEBUG
  {
    // the case of no histogram available for requested column signals
    // something wrong with computation of required statistics columns,
    // we print a debug message to log this case

    CAutoMemoryPool amp;
    CAutoTrace at(amp.Pmp());

    at.Os() << "\nREQUESTED NDVs FOR COL (" << colref->Id() << ") WITH A MISSING HISTOGRAM";
  }
#endif  // GPOS_DEBUG

  // if no histogram is available for required column, we use
  // the number of rows as NDVs estimate
  return std::min(m_rows, GetColUpperBoundNDVs(colref));
}

// Compute stats of a given column
IStatistics *CStatistics::ComputeColStats(CMemoryPool *mp, CColRef *colref, IMDId *rel_mdid) {
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != colref);
  GPOS_ASSERT(nullptr != rel_mdid);

  CColRefSet *pcrsHist = GPOS_NEW(mp) CColRefSet(mp);
  pcrsHist->Include(colref);

  CColRefSet *pcrsWidth = GPOS_NEW(mp) CColRefSet(mp);
  pcrsWidth->Include(colref);

  CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
  IStatistics *stats = md_accessor->Pstats(mp, rel_mdid, pcrsHist, pcrsWidth);

  pcrsHist->Release();
  pcrsWidth->Release();
  return stats;
}

// look up the fraction of null values of a particular column
CDouble CStatistics::GetNullFreq(const CColRef *colref) {
  uint32_t colid = colref->Id();
  CHistogram *col_histogram = m_colid_histogram_mapping->Find(&colid);
  if (nullptr != col_histogram) {
    return col_histogram->GetNullFreq();
  }

  // if no histogram is available for required column, we assume no nulls
  return 0;
}

// EOF
