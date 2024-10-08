//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CHistogram.h
//
//	@doc:
//		Histogram implementation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CHistogram_H
#define GPNAUCRATES_CHistogram_H

#include "gpopt/base/CKHeap.h"
#include "gpos/base.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CStatsPred.h"

namespace gpopt {
class CColRef;
class CStatisticsConfig;
class CColumnFactory;
}  // namespace gpopt

namespace gpnaucrates {
// type definitions
// array of doubles
using CDoubleArray = CDynamicPtrArray<CDouble, CleanupDelete>;

//---------------------------------------------------------------------------
//	@class:
//		CHistogram
//
//	@doc:
//
//---------------------------------------------------------------------------
class CHistogram {
  // hash map from column id to a histogram
  using UlongToHistogramMap = CHashMap<uint32_t, CHistogram, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                       CleanupDelete<uint32_t>, CleanupDelete<CHistogram>>;

  // iterator
  using UlongToHistogramMapIter = CHashMapIter<uint32_t, CHistogram, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                               CleanupDelete<uint32_t>, CleanupDelete<CHistogram>>;

  // hash map from column uint32_t to CDouble
  using UlongToDoubleMap = CHashMap<uint32_t, CDouble, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                    CleanupDelete<uint32_t>, CleanupDelete<CDouble>>;

  // iterator
  using UlongToDoubleMapIter = CHashMapIter<uint32_t, CDouble, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                            CleanupDelete<uint32_t>, CleanupDelete<CDouble>>;

 private:
  // shared memory pool
  CMemoryPool *m_mp;

  // all the buckets in the histogram. This is shared among histograms,
  // so must not be modified unless we first make a new copy. We do not copy
  // histograms unless required, as it is an expensive operation in memory and time.
  CBucketArray *m_histogram_buckets;

  // well-defined histogram. if false, then bounds are unknown
  bool m_is_well_defined;

  // null fraction
  CDouble m_null_freq;

  // ndistinct of tuples not covered in the buckets
  CDouble m_distinct_remaining;

  // frequency of tuples not covered in the buckets
  CDouble m_freq_remaining;

  // has histogram skew been measures
  bool m_skew_was_measured;

  // skew estimate
  CDouble m_skew;

  // was the NDVs in histogram scaled
  bool m_NDVs_were_scaled;

  // is column statistics missing in the database
  bool m_is_col_stats_missing;

  // return an array buckets after applying equality filter on the histogram buckets
  CBucketArray *MakeBucketsWithEqualityFilter(CPoint *point) const;

  // return an array buckets after applying non equality filter on the histogram buckets
  CBucketArray *MakeBucketsWithInequalityFilter(CPoint *point) const;

  // less than or less than equal filter
  CHistogram *MakeHistogramLessThanOrLessThanEqualFilter(CStatsPred::EStatsCmpType stats_cmp_type, CPoint *point) const;

  // greater than or greater than equal filter
  CHistogram *MakeHistogramGreaterThanOrGreaterThanEqualFilter(CStatsPred::EStatsCmpType stats_cmp_type,
                                                               CPoint *point) const;

  // equal filter
  CHistogram *MakeHistogramEqualFilter(CPoint *point) const;

  // not equal filter
  CHistogram *MakeHistogramInequalityFilter(CPoint *point) const;

  // IDF filter
  CHistogram *MakeHistogramIDFFilter(CPoint *point) const;

  // INDF filter
  CHistogram *MakeHistogramINDFFilter(CPoint *point) const;

  // equality join
  CHistogram *MakeJoinHistogramEqualityFilter(const CHistogram *histogram) const;

  // generate histogram based on NDV
  CHistogram *MakeNDVBasedJoinHistogramEqualityFilter(const CHistogram *histogram) const;

  // construct a new histogram for an INDF join predicate
  CHistogram *MakeJoinHistogramINDFFilter(const CHistogram *histogram) const;

  // accessor for n-th bucket
  CBucket *operator[](uint32_t) const;

  // Populate sample ratio within each bucket
  void GetSampleRate(double left, double right, double *sample_rate, uint32_t index);

  // compute skew estimate
  void ComputeSkew();

  // helper to add buckets from one histogram to another
  static void AddBuckets(CMemoryPool *mp, const CBucketArray *src_buckets, CBucketArray *dest_buckets, CDouble rows_old,
                         CDouble rows_new, uint32_t begin, uint32_t end);

  static void AddBuckets(CMemoryPool *mp, const CBucketArray *src_buckets, CBucketArray *dest_buckets, CDouble rows,
                         CDoubleArray *dest_bucket_freqs, uint32_t begin, uint32_t end);

  // helper to combine histogram buckets to reduce total buckets
  static CBucketArray *CombineBuckets(CMemoryPool *mp, CBucketArray *buckets, uint32_t desired_num_buckets);

  // check if we can compute NDVRemain for JOIN histogram for the given input histograms
  static bool CanComputeJoinNDVRemain(const CHistogram *histogram1, const CHistogram *histogram2);

  // compute the effects of the NDV and frequency of the tuples not captured
  // by the histogram
  static void ComputeJoinNDVRemainInfo(
      const CHistogram *histogram1, const CHistogram *histogram2,
      const CBucketArray *join_buckets,  // join buckets
      CDouble hist1_buckets_freq,        // frequency of the buckets in input1 that contributed to the join
      CDouble hist2_buckets_freq,        // frequency of the buckets in input2 that contributed to the join
      CDouble *result_distinct_remain, CDouble *result_freq_remain);

  // check if the cardinality estimation should be done only via NDVs
  static bool NeedsNDVBasedCardEstimationForEq(const CHistogram *histogram);

  bool IsHistogramForTextRelatedTypes() const;

  // add residual union all buckets after the merge
  uint32_t AddResidualUnionAllBucket(CBucketArray *histogram_buckets, CBucket *bucket, CDouble rows_old,
                                     CDouble rows_new, bool bucket_is_residual, uint32_t index) const;

  // add residual union buckets after the merge
  uint32_t AddResidualUnionBucket(CBucketArray *histogram_buckets, CBucket *bucket, CDouble rows,
                                  bool bucket_is_residual, uint32_t index, CDoubleArray *dest_bucket_freqs) const;

  // used to keep track of adjacent stats buckets and how similar
  // they are in terms of distribution
  struct SAdjBucketBoundary {
    // boundary_index 0 refers to boundary between b[0] and b[1]
    uint32_t m_boundary_index;
    // similarity factor between two adjacent buckets calculated as (freq0/ndv0 - freq1/ndv1) + (freq0/width0 -
    // freq1/width1)
    CDouble m_similarity_factor;

    SAdjBucketBoundary(uint32_t index, CDouble similarity_factor)
        : m_boundary_index(index), m_similarity_factor(similarity_factor) {}

    // used for sorting in the binary heap
    CDouble DCost() const { return m_similarity_factor; }
    CDouble GetCostForHeap() const { return DCost(); }
  };
  using SAdjBucketBoundaryArray = CDynamicPtrArray<SAdjBucketBoundary, CleanupDelete<SAdjBucketBoundary>>;

 public:
  CHistogram &operator=(const CHistogram &) = delete;

  CHistogram(const CHistogram &) = delete;

  // ctors
  explicit CHistogram(CMemoryPool *mp, CBucketArray *histogram_buckets, bool is_well_defined = true);

  explicit CHistogram(CMemoryPool *mp, bool is_well_defined = true);

  CHistogram(CMemoryPool *mp, CBucketArray *histogram_buckets, bool is_well_defined, CDouble null_freq,
             CDouble distinct_remaining, CDouble freq_remaining, bool is_col_stats_missing = false);

  // set null frequency
  void SetNullFrequency(CDouble null_freq);

  // set information about the scaling of NDVs
  void SetNDVScaled() { m_NDVs_were_scaled = true; }

  // have the NDVs been scaled
  bool WereNDVsScaled() const { return m_NDVs_were_scaled; }

  // filter by comparing with point
  CHistogram *MakeHistogramFilter(CStatsPred::EStatsCmpType stats_cmp_type, CPoint *point) const;

  // filter by comparing with point and normalize
  CHistogram *MakeHistogramFilterNormalize(CStatsPred::EStatsCmpType stats_cmp_type, CPoint *point,
                                           CDouble *scale_factor) const;

  // join with another histogram
  CHistogram *MakeJoinHistogram(CStatsPred::EStatsCmpType stats_cmp_type, const CHistogram *histogram) const;

  // LASJ with another histogram
  CHistogram *MakeLASJHistogram(CStatsPred::EStatsCmpType stats_cmp_type, const CHistogram *histogram) const;

  // join with another histogram and normalize it.
  // If the join is not an equality join the function returns an empty histogram
  CHistogram *MakeJoinHistogramNormalize(CStatsPred::EStatsCmpType stats_cmp_type, CDouble rows,
                                         const CHistogram *other_histogram, CDouble rows_other,
                                         CDouble *scale_factor) const;

  // scale factor of inequality (!=) join
  CDouble GetInequalityJoinScaleFactor(CDouble rows, const CHistogram *other_histogram, CDouble rows_other) const;

  // left anti semi join with another histogram and normalize
  CHistogram *MakeLASJHistogramNormalize(
      CStatsPred::EStatsCmpType stats_cmp_type, CDouble rows, const CHistogram *other_histogram, CDouble *scale_factor,
      bool DoIgnoreLASJHistComputation  // except for the case of LOJ cardinality estimation this flag is always
                                        // "true" since LASJ stats computation is very aggressive
  ) const;

  // group by and normalize
  CHistogram *MakeGroupByHistogramNormalize(CDouble rows, CDouble *scale_factor) const;

  // union all and normalize
  CHistogram *MakeUnionAllHistogramNormalize(CDouble rows, const CHistogram *other_histogram, CDouble rows_other) const;

  // union and normalize
  CHistogram *MakeUnionHistogramNormalize(CDouble rows, const CHistogram *other_histogram, CDouble rows_other,
                                          CDouble *num_output_rows) const;

  // cleanup residual buckets
  static void CleanupResidualBucket(CBucket *bucket, bool bucket_is_residual);

  // get the next bucket for union / union all
  static CBucket *GetNextBucket(const CHistogram *histogram, CBucket *new_bucket, bool *target_bucket_is_residual,
                                uint32_t *current_bucket_index);

  // number of buckets
  uint32_t GetNumBuckets() const {
    GPOS_ASSERT(m_histogram_buckets != nullptr);
    return m_histogram_buckets->Size();
  }

  // buckets accessor
  const CBucketArray *GetBuckets() const { return m_histogram_buckets; }

  // well defined
  bool IsWellDefined() const { return m_is_well_defined; }

  bool ContainsOnlySingletonBuckets() const;

  // is the column statistics missing in the database
  bool IsColStatsMissing() const { return m_is_col_stats_missing; }

  // print function
  IOstream &OsPrint(IOstream &os) const;

  // total frequency from buckets and null fraction
  CDouble GetFrequency() const;

  // total number of distinct values
  CDouble GetNumDistinct() const;

  // is histogram well formed
  bool IsValid() const;

  // return copy of histogram
  CHistogram *CopyHistogram() const;

  // destructor
  virtual ~CHistogram() { m_histogram_buckets->Release(); }

  // normalize histogram and return scaling factor
  CDouble NormalizeHistogram();

  // is histogram normalized
  bool IsNormalized() const;

  // translate the histogram into a derived column stats
  CDXLStatsDerivedColumn *TranslateToDXLDerivedColumnStats(CMDAccessor *md_accessor, uint32_t colid,
                                                           CDouble width) const;

  // randomly pick a bucket index
  uint32_t GetRandomBucketIndex(uint32_t *seed) const;

  // estimate of data skew
  CDouble GetSkew() {
    if (!m_skew_was_measured) {
      ComputeSkew();
    }

    return m_skew;
  }

  // accessor of null fraction
  CDouble GetNullFreq() const { return m_null_freq; }

  // accessor of remaining number of tuples
  CDouble GetDistinctRemain() const { return m_distinct_remaining; }

  // accessor of remaining frequency
  CDouble GetFreqRemain() const { return m_freq_remaining; }

  // check if histogram is empty
  bool IsEmpty() const;

  // cap the total number of distinct values (NDVs) in buckets to the number of rows
  void CapNDVs(CDouble rows);

  // is comparison type supported for filters for text columns
  static bool IsOpSupportedForTextFilter(CStatsPred::EStatsCmpType stats_cmp_type);

  // is comparison type supported for filters
  static bool IsOpSupportedForFilter(CStatsPred::EStatsCmpType stats_cmp_type);

  // is the join predicate's comparison type supported
  static bool JoinPredCmpTypeIsSupported(CStatsPred::EStatsCmpType stats_cmp_type);

  // create the default histogram for a given column reference
  static CHistogram *MakeDefaultHistogram(CMemoryPool *mp, CColRef *col_ref, bool is_empty);

  // create the default non empty histogram for a boolean column
  static CHistogram *MakeDefaultBoolHistogram(CMemoryPool *mp);

  // helper method to append histograms from one map to the other
  static void AddHistograms(CMemoryPool *mp, UlongToHistogramMap *src_histograms, UlongToHistogramMap *dest_histograms);

  // add dummy histogram buckets and column width for the array of columns
  static void AddDummyHistogramAndWidthInfo(CMemoryPool *mp, CColumnFactory *col_factory,
                                            UlongToHistogramMap *output_histograms, UlongToDoubleMap *output_col_widths,
                                            const std::vector<uint32_t> &columns, bool is_empty);

  // add dummy histogram buckets for the columns in the input histogram
  static void AddEmptyHistogram(CMemoryPool *mp, UlongToHistogramMap *output_histograms,
                                UlongToHistogramMap *input_histograms);

  // create a deep copy of m_histogram_buckets
  static CBucketArray *DeepCopyHistogramBuckets(CMemoryPool *mp, const CBucketArray *buckets);

  // default histogram selectivity
  static const CDouble DefaultSelectivity;

  // minimum number of distinct values in a column
  static const CDouble MinDistinct;

  // default scale factor when there is no filtering of input
  static const CDouble NeutralScaleFactor;

  // default Null frequency
  static const CDouble DefaultNullFreq;

  // default NDV remain
  static const CDouble DefaultNDVRemain;

  // default frequency of NDV remain
  static const CDouble DefaultNDVFreqRemain;
};  // class CHistogram

}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_CHistogram_H

// EOF
