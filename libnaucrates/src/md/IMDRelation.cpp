//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		IMDRelation.cpp
//
//	@doc:
//		Implementation
//---------------------------------------------------------------------------

#include "naucrates/md/IMDRelation.h"

#include "gpos/string/CWStringDynamic.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::GetStorageTypeStr
//
//	@doc:
//		Return name of storage type
//
//---------------------------------------------------------------------------
const CWStringConst *IMDRelation::GetStorageTypeStr(IMDRelation::Erelstoragetype rel_storage_type) {
  switch (rel_storage_type) {
    case ErelstorageHeap:
      return CDXLTokens::GetDXLTokenStr(EdxltokenRelStorageHeap);
    case ErelstorageAppendOnlyCols:
      return CDXLTokens::GetDXLTokenStr(EdxltokenRelStorageAppendOnlyCols);
    case ErelstorageAppendOnlyRows:
      return CDXLTokens::GetDXLTokenStr(EdxltokenRelStorageAppendOnlyRows);
    case ErelstorageForeign:
      return CDXLTokens::GetDXLTokenStr(EdxltokenRelStorageForeign);
    case ErelstorageMixedPartitioned:
      return CDXLTokens::GetDXLTokenStr(EdxltokenRelStorageMixedPartitioned);
    case ErelstorageCompositeType:
      return CDXLTokens::GetDXLTokenStr(EdxltokenRelStorageCompositeType);
    default:
      return nullptr;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::ColumnsToStr
//
//	@doc:
//		Serialize an array of column ids into a comma-separated string
//
//---------------------------------------------------------------------------
CWStringDynamic *IMDRelation::ColumnsToStr(CMemoryPool *mp, ULongPtrArray *colid_array) {
  CWStringDynamic *str = GPOS_NEW(mp) CWStringDynamic(mp);

  uint32_t length = colid_array->Size();
  for (uint32_t ul = 0; ul < length; ul++) {
    uint32_t id = *((*colid_array)[ul]);
    if (ul == length - 1) {
      // last element: do not print a comma
      str->AppendFormat(GPOS_WSZ_LIT("%d"), id);
    } else {
      str->AppendFormat(GPOS_WSZ_LIT("%d%ls"), id, CDXLTokens::GetDXLTokenStr(EdxltokenComma)->GetBuffer());
    }
  }

  return str;
}
// EOF
