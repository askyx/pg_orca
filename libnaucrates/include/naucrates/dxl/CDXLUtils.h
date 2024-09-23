//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLUtils.h
//
//	@doc:
//		Entry point for parsing and serializing DXL documents.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLUtils_H
#define GPDXL_CDXLUtils_H

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CEnumSet.h"
#include "gpos/common/CEnumSetIter.h"
#include "gpos/io/IOstream.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/statistics/CStatistics.h"

namespace gpmd {
class CMDRequest;
}

namespace gpopt {
class CEnumeratorConfig;
class CStatisticsConfig;
class COptimizerConfig;
class ICostModel;
}  // namespace gpopt

namespace gpdxl {
using namespace gpos;
using namespace gpmd;
using namespace gpnaucrates;

// fwd decl
class CQueryToDXLResult;

using CStatisticsArray = CDynamicPtrArray<CStatistics, CleanupRelease>;

//---------------------------------------------------------------------------
//	@class:
//		CDXLUtils
//
//	@doc:
//		Entry point for parsing and serializing DXL documents.
//
//---------------------------------------------------------------------------
class CDXLUtils {
 public:
  // translate the dxl statistics object to optimizer statistics object
  static CStatisticsArray *ParseDXLToOptimizerStatisticObjArray(
      CMemoryPool *mp, CMDAccessor *md_accessor, CDXLStatsDerivedRelationArray *dxl_derived_rel_stats_array);

  // extract the array of optimizer buckets from the dxl representation of
  // dxl buckets in the dxl derived column statistics object
  static CBucketArray *ParseDXLToBucketsArray(CMemoryPool *mp, CMDAccessor *md_accessor,
                                              CDXLStatsDerivedColumn *dxl_derived_col_stats);

  // create a GPOS dynamic string from a regular character array
  static CWStringDynamic *CreateDynamicStringFromCharArray(CMemoryPool *mp, const char *c);

  // create an MD name from a character array
  static CMDName *CreateMDNameFromCharArray(CMemoryPool *mp, const char *c);

  // serialize a list of integers into a comma-separate string
  template <typename T, void (*CleanupFn)(T *)>
  static CWStringDynamic *Serialize(CMemoryPool *mp, const CDynamicPtrArray<T, CleanupFn> *arr);

  template <typename T, uint32_t sentinel_index>
  static CWStringDynamic *Serialize(CMemoryPool *mp, const CEnumSet<T, sentinel_index> *dynamic_set,
                                    const wchar_t *(*func)(T));

  // serialize a list of lists of integers into a comma-separate string
  static CWStringDynamic *Serialize(CMemoryPool *mp, const ULongPtr2dArray *pdrgpul);

  // serialize a list of chars into a comma-separate string
  static CWStringDynamic *SerializeToCommaSeparatedString(CMemoryPool *mp, const CharPtrArray *pdrgpsz);

  // serialize a list of strings into a comma-separate string
  static CWStringDynamic *SerializeToCommaSeparatedString(CMemoryPool *mp, const StringPtrArray *pdrgpsz);

  static char *Read(CMemoryPool *mp, const char *filename);

  // create a multi-byte character string from a wide character string
  static char *CreateMultiByteCharStringFromWCString(CMemoryPool *mp, const wchar_t *wc_string);

  // translate the optimizer datum from dxl datum object
  static IDatum *GetDatum(CMemoryPool *mp, CMDAccessor *md_accessor, const CDXLDatum *dxl_datum);

  static CWStringDynamic *SerializeBooleanArray(CMemoryPool *mp, ULongPtrArray *dynamic_ptr_array,
                                                const CWStringConst *true_value, const CWStringConst *false_value);

#ifdef GPOS_DEBUG
  // debug print of the metadata relation
  static void DebugPrintMDIdArray(IOstream &os, IMdIdArray *mdid_array);
#endif
};

// serialize a list of integers into a comma-separate string
template <typename T, void (*CleanupFn)(T *)>
CWStringDynamic *CDXLUtils::Serialize(CMemoryPool *mp, const CDynamicPtrArray<T, CleanupFn> *dynamic_ptr_array) {
  CAutoP<CWStringDynamic> string_var(GPOS_NEW(mp) CWStringDynamic(mp));

  if (nullptr == dynamic_ptr_array) {
    return string_var.Reset();
  }

  uint32_t length = dynamic_ptr_array->Size();
  for (uint32_t ul = 0; ul < length; ul++) {
    T value = *((*dynamic_ptr_array)[ul]);
    if (ul == length - 1) {
      // last element: do not print a comma
      string_var->AppendFormat(GPOS_WSZ_LIT("%d"), value);
    } else {
      string_var->AppendFormat(GPOS_WSZ_LIT("%d%ls"), value, CDXLTokens::GetDXLTokenStr(EdxltokenComma)->GetBuffer());
    }
  }

  return string_var.Reset();
}

template <typename T, uint32_t sentinel_index>
CWStringDynamic *CDXLUtils::Serialize(CMemoryPool *mp, const CEnumSet<T, sentinel_index> *dynamic_set,
                                      const wchar_t *(*func)(T)) {
  CAutoP<CWStringDynamic> str(GPOS_NEW(mp) CWStringDynamic(mp));

  CEnumSetIter<T, sentinel_index> iter(*dynamic_set);
  uint32_t idx = 0;
  while (iter.Advance()) {
    const wchar_t *index = func(iter.TBit());
    str->AppendWideCharArray(index);
    if (idx != dynamic_set->Size() - 1) {
      str->AppendFormat(GPOS_WSZ_LIT("%ls"), CDXLTokens::GetDXLTokenStr(EdxltokenComma)->GetBuffer());
    }
    idx += 1;
  }

  return str.Reset();
}

}  // namespace gpdxl

#endif  // GPDXL_CDXLUtils_H

// EOF
