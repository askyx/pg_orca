//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLUtils.cpp
//
//	@doc:
//		Implementation of the utility methods for parsing and searializing DXL.
//---------------------------------------------------------------------------

#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpos/base.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CAutoRg.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/io/CFileReader.h"
#include "gpos/io/COstreamString.h"
#include "gpos/io/ioutils.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CTraceFlagIter.h"
#include "gpos/task/CWorker.h"
#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"
#include "naucrates/md/CMDRequest.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToOptimizerStatisticObjArray
//
//	@doc:
//		Translate the array of dxl statistics objects to an array of
//		optimizer statistics object.
//---------------------------------------------------------------------------
CStatisticsArray *CDXLUtils::ParseDXLToOptimizerStatisticObjArray(
    CMemoryPool *mp, CMDAccessor *md_accessor, CDXLStatsDerivedRelationArray *dxl_derived_rel_stats_array) {
  GPOS_ASSERT(nullptr != dxl_derived_rel_stats_array);

  CStatisticsArray *statistics_array = GPOS_NEW(mp) CStatisticsArray(mp);
  const ULONG ulRelStat = dxl_derived_rel_stats_array->Size();
  for (ULONG ulIdxRelStat = 0; ulIdxRelStat < ulRelStat; ulIdxRelStat++) {
    // create hash map from colid -> histogram
    UlongToHistogramMap *column_id_histogram_map = GPOS_NEW(mp) UlongToHistogramMap(mp);

    // width hash map
    UlongToDoubleMap *column_id_width_map = GPOS_NEW(mp) UlongToDoubleMap(mp);

    CDXLStatsDerivedRelation *stats_derived_relation_dxl = (*dxl_derived_rel_stats_array)[ulIdxRelStat];
    const CDXLStatsDerivedColumnArray *derived_column_stats_array =
        stats_derived_relation_dxl->GetDXLStatsDerivedColArray();

    const ULONG num_of_columns = derived_column_stats_array->Size();
    for (ULONG column_id_idx = 0; column_id_idx < num_of_columns; column_id_idx++) {
      CDXLStatsDerivedColumn *dxl_derived_col_stats = (*derived_column_stats_array)[column_id_idx];

      ULONG column_id = dxl_derived_col_stats->GetColId();
      CDouble width = dxl_derived_col_stats->Width();
      CDouble null_freq = dxl_derived_col_stats->GetNullFreq();
      CDouble distinct_remaining = dxl_derived_col_stats->GetDistinctRemain();
      CDouble freq_remaining = dxl_derived_col_stats->GetFreqRemain();

      CBucketArray *stats_buckets_array = CDXLUtils::ParseDXLToBucketsArray(mp, md_accessor, dxl_derived_col_stats);
      CHistogram *histogram = GPOS_NEW(mp)
          CHistogram(mp, stats_buckets_array, true /*is_well_defined*/, null_freq, distinct_remaining, freq_remaining);

      column_id_histogram_map->Insert(GPOS_NEW(mp) ULONG(column_id), histogram);
      column_id_width_map->Insert(GPOS_NEW(mp) ULONG(column_id), GPOS_NEW(mp) CDouble(width));
    }

    CDouble rows = stats_derived_relation_dxl->Rows();
    CStatistics *stats =
        GPOS_NEW(mp) CStatistics(mp, column_id_histogram_map, column_id_width_map, rows, false /* is_empty */
        );
    // stats->AddCardUpperBound(mp, ulIdxRelStat, rows);

    statistics_array->Append(stats);
  }

  return statistics_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToBucketsArray
//
//	@doc:
//		Extract the array of optimizer buckets from the dxl representation of
//		dxl buckets in the dxl derived column statistics object.
//---------------------------------------------------------------------------
CBucketArray *CDXLUtils::ParseDXLToBucketsArray(CMemoryPool *mp, CMDAccessor *md_accessor,
                                                CDXLStatsDerivedColumn *dxl_derived_col_stats) {
  CBucketArray *stats_buckets_array = GPOS_NEW(mp) CBucketArray(mp);

  const CDXLBucketArray *dxl_bucket_array = dxl_derived_col_stats->TransformHistogramToDXLBucketArray();
  const ULONG num_of_buckets = dxl_bucket_array->Size();
  for (ULONG ul = 0; ul < num_of_buckets; ul++) {
    CDXLBucket *dxl_bucket = (*dxl_bucket_array)[ul];

    // translate the lower and upper bounds of the bucket
    IDatum *datum_lower_bound = GetDatum(mp, md_accessor, dxl_bucket->GetDXLDatumLower());
    CPoint *point_lower_bound = GPOS_NEW(mp) CPoint(datum_lower_bound);

    IDatum *datum_upper_bound = GetDatum(mp, md_accessor, dxl_bucket->GetDXLDatumUpper());
    CPoint *point_upper_bound = GPOS_NEW(mp) CPoint(datum_upper_bound);

    CBucket *bucket =
        GPOS_NEW(mp) CBucket(point_lower_bound, point_upper_bound, dxl_bucket->IsLowerClosed(),
                             dxl_bucket->IsUpperClosed(), dxl_bucket->GetFrequency(), dxl_bucket->GetNumDistinct());

    stats_buckets_array->Append(bucket);
  }

  return stats_buckets_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::GetDatum
//
//	@doc:
//		Translate the optimizer datum from dxl datum object
//
//---------------------------------------------------------------------------
IDatum *CDXLUtils::GetDatum(CMemoryPool *mp, CMDAccessor *md_accessor, const CDXLDatum *dxl_datum) {
  IMDId *mdid = dxl_datum->MDId();
  return md_accessor->RetrieveType(mdid)->GetDatumForDXLDatum(mp, dxl_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::CreateDynamicStringFromCharArray
//
//	@doc:
//		Create a GPOS string object from a character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CWStringDynamic *CDXLUtils::CreateDynamicStringFromCharArray(CMemoryPool *mp, const CHAR *c) {
  GPOS_ASSERT(nullptr != c);

  CAutoP<CWStringDynamic> string_var(GPOS_NEW(mp) CWStringDynamic(mp));
  string_var->AppendFormat(GPOS_WSZ_LIT("%s"), c);
  return string_var.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::CreateMDNameFromCharArray
//
//	@doc:
//		Create a GPOS string object from a character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CMDName *CDXLUtils::CreateMDNameFromCharArray(CMemoryPool *mp, const CHAR *c) {
  GPOS_ASSERT(nullptr != c);

  // The CMDName will take ownership of the buffer. This ensures we minimize allocations
  // and improves performance for this very hot code path
  const CWStringConst *str = GPOS_NEW(mp) CWStringConst(mp, c);

  CMDName *mdname = GPOS_NEW(mp) CMDName(str, true /* owns_memory */);

  return mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::Serialize
//
//	@doc:
//		Serialize a list of unsigned integers into a comma-separated string
//
//---------------------------------------------------------------------------
CWStringDynamic *CDXLUtils::Serialize(CMemoryPool *mp, const ULongPtr2dArray *array_2D) {
  const ULONG len = array_2D->Size();
  CWStringDynamic *keys_buffer = GPOS_NEW(mp) CWStringDynamic(mp);
  for (ULONG ul = 0; ul < len; ul++) {
    ULongPtrArray *pdrgpul = (*array_2D)[ul];
    CWStringDynamic *key_set_string = CDXLUtils::Serialize(mp, pdrgpul);

    keys_buffer->Append(key_set_string);

    if (ul < len - 1) {
      keys_buffer->AppendFormat(GPOS_WSZ_LIT("%ls"), GPOS_WSZ_LIT(";"));
    }

    GPOS_DELETE(key_set_string);
  }

  return keys_buffer;
}

// Serialize a list of chars into a comma-separated string
CWStringDynamic *CDXLUtils::SerializeToCommaSeparatedString(CMemoryPool *mp, const CharPtrArray *char_ptr_array) {
  CWStringDynamic *dxl_string = GPOS_NEW(mp) CWStringDynamic(mp);

  ULONG length = char_ptr_array->Size();
  for (ULONG ul = 0; ul < length; ul++) {
    CHAR value = *((*char_ptr_array)[ul]);
    if (ul == length - 1) {
      // last element: do not print a comma
      dxl_string->AppendFormat(GPOS_WSZ_LIT("%c"), value);
    } else {
      dxl_string->AppendFormat(GPOS_WSZ_LIT("%c%ls"), value, CDXLTokens::GetDXLTokenStr(EdxltokenComma)->GetBuffer());
    }
  }

  return dxl_string;
}

// Serialize a list of chars into a comma-separated string
CWStringDynamic *CDXLUtils::SerializeToCommaSeparatedString(CMemoryPool *mp, const StringPtrArray *str_ptr_array) {
  CWStringDynamic *dxl_string = GPOS_NEW(mp) CWStringDynamic(mp);

  ULONG length = str_ptr_array->Size();
  for (ULONG ul = 0; ul < length; ul++) {
    CWStringBase *value = (*str_ptr_array)[ul];
    if (ul == length - 1) {
      // last element: do not print a comma
      dxl_string->AppendFormat(GPOS_WSZ_LIT("%ls"), value->GetBuffer());
    } else {
      dxl_string->AppendFormat(GPOS_WSZ_LIT("%ls%ls"), value->GetBuffer(),
                               CDXLTokens::GetDXLTokenStr(EdxltokenComma)->GetBuffer());
    }
  }

  return dxl_string;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::CreateMultiByteCharStringFromWCString
//
//	@doc:
//		Converts a wide character string into a character array in the provided memory pool
//
//---------------------------------------------------------------------------
CHAR *CDXLUtils::CreateMultiByteCharStringFromWCString(CMemoryPool *mp, const WCHAR *wc) {
  GPOS_ASSERT(nullptr != wc);

  ULONG max_length = GPOS_WSZ_LENGTH(wc) * GPOS_SIZEOF(WCHAR) + 1;
  CHAR *c = GPOS_NEW_ARRAY(mp, CHAR, max_length);
  CAutoRg<CHAR> char_wrapper(c);

#ifdef GPOS_DEBUG
  INT i = (INT)
#endif
      wcstombs(c, wc, max_length);
  GPOS_ASSERT(0 <= i);

  char_wrapper[max_length - 1] = '\0';

  return char_wrapper.RgtReset();
}

//---------------------------------------------------------------------------
//		CDXLUtils::Read
//
//	@doc:
//		Read a given text file in a character buffer.
//		The function allocates memory from the provided memory pool, and it is
//		the responsibility of the caller to deallocate it.
//
//---------------------------------------------------------------------------
CHAR *CDXLUtils::Read(CMemoryPool *mp, const CHAR *filename) {
  GPOS_TRACE_FORMAT("opening file %s", filename);

  CFileReader fr;
  fr.Open(filename);

  ULONG_PTR file_size = (ULONG_PTR)fr.FileSize();
  CAutoRg<CHAR> read_buffer(GPOS_NEW_ARRAY(mp, CHAR, file_size + 1));

  ULONG_PTR read_bytes = fr.ReadBytesToBuffer((BYTE *)read_buffer.Rgt(), file_size);
  fr.Close();

  GPOS_ASSERT(read_bytes == file_size);

  read_buffer[read_bytes] = '\0';

  return read_buffer.RgtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeBooleanArray
//
//	@doc:
//		Serialize a boolean array by mapping 1 to true_value
//		and 0 to false_value
//
//---------------------------------------------------------------------------
CWStringDynamic *CDXLUtils::SerializeBooleanArray(CMemoryPool *mp, ULongPtrArray *dynamic_ptr_array,
                                                  const CWStringConst *true_value, const CWStringConst *false_value) {
  CAutoP<CWStringDynamic> string_var(GPOS_NEW(mp) CWStringDynamic(mp));

  if (nullptr == dynamic_ptr_array) {
    return string_var.Reset();
  }

  ULONG length = dynamic_ptr_array->Size();
  for (ULONG ul = 0; ul < length; ul++) {
    ULONG value = *((*dynamic_ptr_array)[ul]);
    GPOS_ASSERT(value == 0 || value == 1);
    const CWStringConst *string_repr;
    string_repr = (value == 1) ? true_value : false_value;

    if (ul == length - 1) {
      // last element: do not print a comma
      string_var->AppendFormat(string_repr->GetBuffer());
    } else {
      string_var->AppendFormat(GPOS_WSZ_LIT("%ls%ls"), string_repr->GetBuffer(),
                               CDXLTokens::GetDXLTokenStr(EdxltokenComma)->GetBuffer());
    }
  }

  return string_var.Reset();
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::DebugPrintMDIdArray
//
//	@doc:
//		Print an array of mdids
//
//---------------------------------------------------------------------------
void CDXLUtils::DebugPrintMDIdArray(IOstream &os, IMdIdArray *mdid_array) {
  ULONG len = mdid_array->Size();
  for (ULONG ul = 0; ul < len; ul++) {
    const IMDId *mdid = (*mdid_array)[ul];
    mdid->OsPrint(os);
    os << " ";
  }

  os << std::endl;
}
#endif

// EOF
