//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIndexGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing metadata indexes
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIndexGPDB.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/IMDScalarOp.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::CMDIndexGPDB
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMDIndexGPDB::CMDIndexGPDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, bool is_clustered, bool is_partitioned,
                           bool amcanorder, EmdindexType index_type, IMDId *mdid_item_type,
                           ULongPtrArray *index_key_cols_array, ULongPtrArray *included_cols_array,
                           ULongPtrArray *returnable_cols_array, IMdIdArray *mdid_opfamilies_array,
                           IMdIdArray *child_index_oids, ULongPtrArray *sort_direction, ULongPtrArray *nulls_direction)

    : m_mp(mp),
      m_mdid(mdid),
      m_mdname(mdname),
      m_clustered(is_clustered),
      m_partitioned(is_partitioned),
      m_amcanorder(amcanorder),
      m_index_type(index_type),
      m_mdid_item_type(mdid_item_type),
      m_index_key_cols_array(index_key_cols_array),
      m_included_cols_array(included_cols_array),
      m_returnable_cols_array(returnable_cols_array),
      m_mdid_opfamilies_array(mdid_opfamilies_array),
      m_child_index_oids(child_index_oids),
      m_sort_direction(sort_direction),
      m_nulls_direction(nulls_direction) {
  GPOS_ASSERT(mdid->IsValid());
  GPOS_ASSERT(IMDIndex::EmdindSentinel > index_type);
  GPOS_ASSERT(nullptr != index_key_cols_array);
  GPOS_ASSERT(0 < index_key_cols_array->Size());
  GPOS_ASSERT(nullptr != included_cols_array);
  GPOS_ASSERT(nullptr != returnable_cols_array);
  GPOS_ASSERT_IMP(nullptr != mdid_item_type,
                  IMDIndex::EmdindBitmap == index_type || IMDIndex::EmdindBtree == index_type ||
                      IMDIndex::EmdindGist == index_type || IMDIndex::EmdindGin == index_type ||
                      IMDIndex::EmdindBrin == index_type || IMDIndex::EmdindHash == index_type);
  GPOS_ASSERT_IMP(IMDIndex::EmdindBitmap == index_type, nullptr != mdid_item_type && mdid_item_type->IsValid());
  GPOS_ASSERT(nullptr != mdid_opfamilies_array);
  GPOS_ASSERT(nullptr != sort_direction);
  GPOS_ASSERT(nullptr != nulls_direction);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::~CMDIndexGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIndexGPDB::~CMDIndexGPDB() {
  GPOS_DELETE(m_mdname);
  if (nullptr != m_dxl_str) {
    GPOS_DELETE(m_dxl_str);
  }
  m_mdid->Release();
  CRefCount::SafeRelease(m_mdid_item_type);
  m_index_key_cols_array->Release();
  m_included_cols_array->Release();
  m_returnable_cols_array->Release();
  m_mdid_opfamilies_array->Release();
  CRefCount::SafeRelease(m_child_index_oids);
  m_sort_direction->Release();
  m_nulls_direction->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::MDId
//
//	@doc:
//		Returns the metadata id of this index
//
//---------------------------------------------------------------------------
IMDId *CMDIndexGPDB::MDId() const {
  return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::Mdname
//
//	@doc:
//		Returns the name of this index
//
//---------------------------------------------------------------------------
CMDName CMDIndexGPDB::Mdname() const {
  return *m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::IsClustered
//
//	@doc:
//		Is the index clustered
//
//---------------------------------------------------------------------------
bool CMDIndexGPDB::IsClustered() const {
  return m_clustered;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::IsPartitioned
//
//	@doc:
//		Is the index partitioned
//
//---------------------------------------------------------------------------
bool CMDIndexGPDB::IsPartitioned() const {
  return m_partitioned;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::CanOrder
//
//	@doc:
//		Does Index Access Method support Ordering
//
//---------------------------------------------------------------------------
bool CMDIndexGPDB::CanOrder() const {
  return m_amcanorder;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::IndexType
//
//	@doc:
//		Index type
//
//---------------------------------------------------------------------------
IMDIndex::EmdindexType CMDIndexGPDB::IndexType() const {
  return m_index_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::Keys
//
//	@doc:
//		Returns the number of index keys
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::Keys() const {
  return m_index_key_cols_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::KeyAt
//
//	@doc:
//		Returns the n-th key column
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::KeyAt(uint32_t pos) const {
  return *((*m_index_key_cols_array)[pos]);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::GetKeyPos
//
//	@doc:
//		Return the position of the key column in the index
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::GetKeyPos(uint32_t column) const {
  const uint32_t size = Keys();

  for (uint32_t ul = 0; ul < size; ul++) {
    if (KeyAt(ul) == column) {
      return ul;
    }
  }

  return UINT32_MAX;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::IncludedCols
//
//	@doc:
//		Returns the number of included columns
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::IncludedCols() const {
  return m_included_cols_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::IncludedColAt
//
//	@doc:
//		Returns the n-th included column
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::IncludedColAt(uint32_t pos) const {
  return *((*m_included_cols_array)[pos]);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::KeySortDirectionAt
//
//	@doc:
//		Returns n-th column sort direction.
//		0 corresponds to ASC
//		1 corresponds to DESC
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::KeySortDirectionAt(uint32_t pos) const {
  return *((*m_sort_direction)[pos]);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::KeyNullsDirectionAt
//
//	@doc:
//		Returns n-th column Nulls direction
//		0 corresponds to NULLS LAST
//		1 corresponds to NULLS FIRST
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::KeyNullsDirectionAt(uint32_t pos) const {
  return *((*m_nulls_direction)[pos]);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::ReturnableCols
//
//	@doc:
//		Returns the number of returnable columns
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::ReturnableCols() const {
  return m_returnable_cols_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::ReturnableColAt
//
//	@doc:
//		Returns the n-th returnable column
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::ReturnableColAt(uint32_t pos) const {
  return *((*m_returnable_cols_array)[pos]);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::GetIncludedColPos
//
//	@doc:
//		Return the position of the included column in the index
//
//---------------------------------------------------------------------------
uint32_t CMDIndexGPDB::GetIncludedColPos(uint32_t column) const {
  const uint32_t size = IncludedCols();

  for (uint32_t ul = 0; ul < size; ul++) {
    if (IncludedColAt(ul) == column) {
      return ul;
    }
  }

  GPOS_ASSERT("Column not found in Index's included columns");

  return UINT32_MAX;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::DebugPrint
//
//	@doc:
//		Prints a MD index to the provided output
//
//---------------------------------------------------------------------------
void CMDIndexGPDB::DebugPrint(IOstream &os) const {
  os << "Index id: ";
  MDId()->OsPrint(os);
  os << std::endl;

  os << "Index name: " << (Mdname()).GetMDName()->GetBuffer() << std::endl;
  os << "Index type: " << GetDXLStr(m_index_type)->GetBuffer() << std::endl;

  os << "Index keys: ";
  for (uint32_t ul = 0; ul < Keys(); ul++) {
    uint32_t ulKey = KeyAt(ul);
    if (ul > 0) {
      os << ", ";
    }
    os << ulKey;
  }
  os << std::endl;

  os << "Included Columns: ";
  for (uint32_t ul = 0; ul < IncludedCols(); ul++) {
    uint32_t ulKey = IncludedColAt(ul);
    if (ul > 0) {
      os << ", ";
    }
    os << ulKey;
  }
  os << std::endl;
}

#endif  // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::MdidType
//
//	@doc:
//		Type of items returned by the index
//
//---------------------------------------------------------------------------
IMDId *CMDIndexGPDB::GetIndexRetItemTypeMdid() const {
  return m_mdid_item_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::IsCompatible
//
//	@doc:
//		Check if given scalar comparison can be used with the index key
// 		at the specified position
//
//---------------------------------------------------------------------------
bool CMDIndexGPDB::IsCompatible(const IMDScalarOp *md_scalar_op, uint32_t key_pos) const {
  GPOS_ASSERT(nullptr != md_scalar_op);

  // In cover indexes the non-key "payload" should not be considered
  // compatible with a predicate
  if (key_pos >= m_mdid_opfamilies_array->Size()) {
    return false;
  }

  // check if the index opfamily for the key at the given position is one of
  // the families the scalar comparison belongs to
  const IMDId *mdid_opfamily = (*m_mdid_opfamilies_array)[key_pos];

  const uint32_t opfamilies_count = md_scalar_op->OpfamiliesCount();

  for (uint32_t ul = 0; ul < opfamilies_count; ul++) {
    if (mdid_opfamily->Equals(md_scalar_op->OpfamilyMdidAt(ul))) {
      return true;
    }
  }

  return false;
}

IMdIdArray *CMDIndexGPDB::ChildIndexMdids() const {
  return m_child_index_oids;
}

// EOF
