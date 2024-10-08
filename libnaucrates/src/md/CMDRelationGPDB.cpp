//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDRelationGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing metadata cache relations
//---------------------------------------------------------------------------

#include "naucrates/md/CMDRelationGPDB.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::CMDRelationGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRelationGPDB::CMDRelationGPDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, bool fTemporary,
                                 Erelstoragetype rel_storage_type, CMDColumnArray *mdcol_array,
                                 ULongPtrArray *partition_cols_array, CharPtrArray *str_part_types_array,
                                 IMdIdArray *partition_oids, bool convert_hash_to_random, ULongPtr2dArray *keyset_array,
                                 CMDIndexInfoArray *md_index_info_array, IMdIdArray *mdid_check_constraint_array,
                                 CDXLNode *mdpart_constraint, IMDId *foreign_server, CDouble rows)
    : m_mp(mp),
      m_mdid(mdid),
      m_mdname(mdname),
      m_is_temp_table(fTemporary),
      m_rel_storage_type(rel_storage_type),
      m_md_col_array(mdcol_array),
      m_dropped_cols(0),
      m_convert_hash_to_random(convert_hash_to_random),
      m_partition_cols_array(partition_cols_array),
      m_str_part_types_array(str_part_types_array),
      m_partition_oids(partition_oids),
      m_keyset_array(keyset_array),
      m_mdindex_info_array(md_index_info_array),
      m_mdid_check_constraint_array(mdid_check_constraint_array),
      m_mdpart_constraint(mdpart_constraint),
      m_system_columns(0),
      m_foreign_server(foreign_server),
      m_colpos_nondrop_colpos_map(nullptr),
      m_attrno_nondrop_col_pos_map(nullptr),
      m_nondrop_col_pos_array(nullptr),
      m_rows(rows) {
  GPOS_ASSERT(mdid->IsValid());
  GPOS_ASSERT(nullptr != mdcol_array);
  GPOS_ASSERT(nullptr != md_index_info_array);
  GPOS_ASSERT(nullptr != mdid_check_constraint_array);

  m_colpos_nondrop_colpos_map = GPOS_NEW(m_mp) UlongToUlongMap(m_mp);
  m_attrno_nondrop_col_pos_map = GPOS_NEW(m_mp) IntToUlongMap(m_mp);
  m_nondrop_col_pos_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);
  m_col_width_array = GPOS_NEW(mp) CDoubleArray(mp);

  const uint32_t arity = mdcol_array->Size();
  uint32_t non_dropped_col_pos = 0;
  for (uint32_t ul = 0; ul < arity; ul++) {
    IMDColumn *mdcol = (*mdcol_array)[ul];
    bool is_system_col = mdcol->IsSystemColumn();
    if (is_system_col) {
      m_system_columns++;
    }

    (void)m_attrno_nondrop_col_pos_map->Insert(GPOS_NEW(m_mp) int32_t(mdcol->AttrNum()), GPOS_NEW(m_mp) uint32_t(ul));

    if (mdcol->IsDropped()) {
      m_dropped_cols++;
    } else {
      if (!is_system_col) {
        m_nondrop_col_pos_array->Append(GPOS_NEW(m_mp) uint32_t(ul));
      }
      (void)m_colpos_nondrop_colpos_map->Insert(GPOS_NEW(m_mp) uint32_t(ul),
                                                GPOS_NEW(m_mp) uint32_t(non_dropped_col_pos));
      non_dropped_col_pos++;
    }

    m_col_width_array->Append(GPOS_NEW(mp) CDouble(mdcol->Length()));
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::~CMDRelationGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDRelationGPDB::~CMDRelationGPDB() {
  GPOS_DELETE(m_mdname);
  if (nullptr != m_dxl_str) {
    GPOS_DELETE(m_dxl_str);
  }
  m_mdid->Release();
  m_md_col_array->Release();
  CRefCount::SafeRelease(m_partition_oids);
  CRefCount::SafeRelease(m_partition_cols_array);
  CRefCount::SafeRelease(m_str_part_types_array);
  CRefCount::SafeRelease(m_keyset_array);
  m_mdindex_info_array->Release();
  m_mdid_check_constraint_array->Release();
  m_col_width_array->Release();
  CRefCount::SafeRelease(m_foreign_server);
  CRefCount::SafeRelease(m_mdpart_constraint);
  CRefCount::SafeRelease(m_colpos_nondrop_colpos_map);
  CRefCount::SafeRelease(m_attrno_nondrop_col_pos_map);
  CRefCount::SafeRelease(m_nondrop_col_pos_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::MDId
//
//	@doc:
//		Returns the metadata id of this relation
//
//---------------------------------------------------------------------------
IMDId *CMDRelationGPDB::MDId() const {
  return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Mdname
//
//	@doc:
//		Returns the name of this relation
//
//---------------------------------------------------------------------------
CMDName CMDRelationGPDB::Mdname() const {
  return *m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::IsTemporary
//
//	@doc:
//		Is the relation temporary
//
//---------------------------------------------------------------------------
bool CMDRelationGPDB::IsTemporary() const {
  return m_is_temp_table;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::RetrieveRelStorageType
//
//	@doc:
//		Returns the storage type for this relation
//
//---------------------------------------------------------------------------
IMDRelation::Erelstoragetype CMDRelationGPDB::RetrieveRelStorageType() const {
  return m_rel_storage_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::ColumnCount
//
//	@doc:
//		Returns the number of columns of this relation
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::ColumnCount() const {
  GPOS_ASSERT(nullptr != m_md_col_array);

  return m_md_col_array->Size();
}

// Return the width of a column with regards to the position
double CMDRelationGPDB::ColWidth(uint32_t pos) const {
  return (*m_col_width_array)[pos]->Get();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::HasDroppedColumns
//
//	@doc:
//		Does relation have dropped columns
//
//---------------------------------------------------------------------------
bool CMDRelationGPDB::HasDroppedColumns() const {
  return 0 < m_dropped_cols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::NonDroppedColsCount
//
//	@doc:
//		Number of non-dropped columns
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::NonDroppedColsCount() const {
  return ColumnCount() - m_dropped_cols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::NonDroppedColAt
//
//	@doc:
//		Return the absolute position of the given attribute position excluding
//		dropped columns
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::NonDroppedColAt(uint32_t pos) const {
  GPOS_ASSERT(pos <= ColumnCount());

  if (!HasDroppedColumns()) {
    return pos;
  }

  uint32_t *colid = m_colpos_nondrop_colpos_map->Find(&pos);

  GPOS_ASSERT(nullptr != colid);
  return *colid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::GetPosFromAttno
//
//	@doc:
//		Return the position of a column in the metadata object given the
//      attribute number in the system catalog
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::GetPosFromAttno(int32_t attno) const {
  uint32_t *att_pos = m_attrno_nondrop_col_pos_map->Find(&attno);
  GPOS_ASSERT(nullptr != att_pos);

  return *att_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::NonDroppedColsArray
//
//	@doc:
//		Returns the original positions of all the non-dropped columns
//
//---------------------------------------------------------------------------
ULongPtrArray *CMDRelationGPDB::NonDroppedColsArray() const {
  return m_nondrop_col_pos_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::SystemColumnsCount
//
//	@doc:
//		Returns the number of system columns of this relation
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::SystemColumnsCount() const {
  return m_system_columns;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::KeySetCount
//
//	@doc:
//		Returns the number of key sets
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::KeySetCount() const {
  return (m_keyset_array == nullptr) ? 0 : m_keyset_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::KeySetAt
//
//	@doc:
//		Returns the key set at the specified position
//
//---------------------------------------------------------------------------
const ULongPtrArray *CMDRelationGPDB::KeySetAt(uint32_t pos) const {
  GPOS_ASSERT(nullptr != m_keyset_array);

  return (*m_keyset_array)[pos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::IsPartitioned
//
//	@doc:
//		Is the table partitioned
//
//---------------------------------------------------------------------------
bool CMDRelationGPDB::IsPartitioned() const {
  return (0 < PartColumnCount());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::PartColumnCount
//
//	@doc:
//		Returns the number of partition keys
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::PartColumnCount() const {
  return (m_partition_cols_array == nullptr) ? 0 : m_partition_cols_array->Size();
}

// Retrieve list of partition types
CharPtrArray *CMDRelationGPDB::GetPartitionTypes() const {
  return m_str_part_types_array;
}

// Returns the partition type of the given level
char CMDRelationGPDB::PartTypeAtLevel(uint32_t ulLevel) const {
  return *(*m_str_part_types_array)[ulLevel];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::PartColAt
//
//	@doc:
//		Returns the partition column at the specified position in the
//		partition key list
//
//---------------------------------------------------------------------------
const IMDColumn *CMDRelationGPDB::PartColAt(uint32_t pos) const {
  uint32_t partition_key_pos = (*(*m_partition_cols_array)[pos]);
  return GetMdCol(partition_key_pos);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::IndexCount
//
//	@doc:
//		Returns the number of indices of this relation
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::IndexCount() const {
  return m_mdindex_info_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::GetMdCol
//
//	@doc:
//		Returns the column at the specified position
//
//---------------------------------------------------------------------------
const IMDColumn *CMDRelationGPDB::GetMdCol(uint32_t pos) const {
  GPOS_ASSERT(pos < m_md_col_array->Size());

  return (*m_md_col_array)[pos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::ConvertHashToRandom
//
//	@doc:
//		Return true if a hash distributed table needs to be considered as random during planning
//---------------------------------------------------------------------------
bool CMDRelationGPDB::ConvertHashToRandom() const {
  return m_convert_hash_to_random;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::IndexMDidAt
//
//	@doc:
//		Returns the id of the index at the specified position of the index array
//
//---------------------------------------------------------------------------
IMDId *CMDRelationGPDB::IndexMDidAt(uint32_t pos) const {
  return (*m_mdindex_info_array)[pos]->MDId();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::CheckConstraintCount
//
//	@doc:
//		Returns the number of check constraints on this relation
//
//---------------------------------------------------------------------------
uint32_t CMDRelationGPDB::CheckConstraintCount() const {
  return m_mdid_check_constraint_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::CheckConstraintMDidAt
//
//	@doc:
//		Returns the id of the check constraint at the specified position of
//		the check constraint array
//
//---------------------------------------------------------------------------
IMDId *CMDRelationGPDB::CheckConstraintMDidAt(uint32_t pos) const {
  return (*m_mdid_check_constraint_array)[pos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::MDPartConstraint
//
//	@doc:
//		Return the part constraint
//
//---------------------------------------------------------------------------
CDXLNode *CMDRelationGPDB::MDPartConstraint() const {
  return m_mdpart_constraint;
}

IMDId *CMDRelationGPDB::ForeignServer() const {
  return m_foreign_server;
}

CDouble CMDRelationGPDB::Rows() const {
  return m_rows;
}

IMdIdArray *CMDRelationGPDB::ChildPartitionMdids() const {
  return m_partition_oids;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void CMDRelationGPDB::DebugPrint(IOstream &os) const {
  os << "Relation id: ";
  MDId()->OsPrint(os);
  os << std::endl;

  os << "Relation name: " << (Mdname()).GetMDName()->GetBuffer() << std::endl;

  os << "Storage type: " << IMDRelation::GetStorageTypeStr(m_rel_storage_type)->GetBuffer() << std::endl;

  os << "Relation columns: " << std::endl;
  const uint32_t num_of_columns = ColumnCount();
  for (uint32_t ul = 0; ul < num_of_columns; ul++) {
    const IMDColumn *mdcol = GetMdCol(ul);
    mdcol->DebugPrint(os);
  }
  os << std::endl;

  os << std::endl;

  os << "Partition keys: ";
  const uint32_t part_columns = PartColumnCount();
  for (uint32_t ul = 0; ul < part_columns; ul++) {
    if (0 < ul) {
      os << ", ";
    }

    const IMDColumn *mdcol_part_key = PartColAt(ul);
    os << (mdcol_part_key->Mdname()).GetMDName()->GetBuffer();
  }

  os << std::endl;

  os << "Index Info: ";
  const uint32_t indexes = m_mdindex_info_array->Size();
  for (uint32_t ul = 0; ul < indexes; ul++) {
    CMDIndexInfo *mdindex_info = (*m_mdindex_info_array)[ul];
    mdindex_info->DebugPrint(os);
  }

  os << "Check Constraint: ";
  CDXLUtils::DebugPrintMDIdArray(os, m_mdid_check_constraint_array);
}

#endif  // GPOS_DEBUG

// EOF
