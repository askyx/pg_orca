//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDRelationGPDB.h
//
//	@doc:
//		Class representing MD relations
//---------------------------------------------------------------------------

#ifndef GPMD_CMDRelationGPDB_H
#define GPMD_CMDRelationGPDB_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/md/CMDColumn.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDColumn.h"
#include "naucrates/md/IMDRelation.h"

namespace gpdxl {}

namespace gpmd {
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@class:
//		CMDRelationGPDB
//
//	@doc:
//		Class representing MD relations
//
//---------------------------------------------------------------------------
class CMDRelationGPDB : public IMDRelation {
 private:
  // memory pool
  CMemoryPool *m_mp;

  // DXL for object
  const CWStringDynamic *m_dxl_str = nullptr;

  // relation mdid
  IMDId *m_mdid;

  // table name
  CMDName *m_mdname;

  // is this a temporary relation
  bool m_is_temp_table;

  // storage type
  Erelstoragetype m_rel_storage_type;

  // columns
  CMDColumnArray *m_md_col_array;

  // number of dropped columns
  uint32_t m_dropped_cols;

  // do we need to consider a hash distributed table as random distributed
  bool m_convert_hash_to_random;

  // indices of partition columns
  ULongPtrArray *m_partition_cols_array;

  // partition types
  CharPtrArray *m_str_part_types_array;

  // Child partition oids
  IMdIdArray *m_partition_oids;

  // array of key sets
  ULongPtr2dArray *m_keyset_array;

  // array of index info
  CMDIndexInfoArray *m_mdindex_info_array;

  // array of check constraint mdids
  IMdIdArray *m_mdid_check_constraint_array;

  // partition constraint
  CDXLNode *m_mdpart_constraint;

  // number of system columns
  uint32_t m_system_columns;

  // oid of foreign server if this is a foreign relation
  IMDId *m_foreign_server;

  // mapping of column position to positions excluding dropped columns
  UlongToUlongMap *m_colpos_nondrop_colpos_map;

  // mapping of attribute number in the system catalog to the positions of
  // the non dropped column in the metadata object
  IntToUlongMap *m_attrno_nondrop_col_pos_map;

  // the original positions of all the non-dropped columns
  ULongPtrArray *m_nondrop_col_pos_array;

  // array of column widths including dropped columns
  CDoubleArray *m_col_width_array;

  // rows
  CDouble m_rows;

 public:
  CMDRelationGPDB(const CMDRelationGPDB &) = delete;

  // ctor
  CMDRelationGPDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, bool is_temp_table, Erelstoragetype rel_storage_type,
                  CMDColumnArray *mdcol_array, ULongPtrArray *partition_cols_array, CharPtrArray *str_part_types_array,
                  IMdIdArray *partition_oids, bool convert_hash_to_random, ULongPtr2dArray *keyset_array,
                  CMDIndexInfoArray *md_index_info_array, IMdIdArray *mdid_check_constraint_array,
                  CDXLNode *mdpart_constraint, IMDId *foreign_server, CDouble rows);

  // dtor
  ~CMDRelationGPDB() override;

  // accessors

  // the metadata id
  IMDId *MDId() const override;

  // relation name
  CMDName Mdname() const override;

  // is this a temp relation
  bool IsTemporary() const override;

  // storage type (heap, appendonly, ...)
  Erelstoragetype RetrieveRelStorageType() const override;

  // number of columns
  uint32_t ColumnCount() const override;

  // width of a column with regards to the position
  double ColWidth(uint32_t pos) const override;

  // does relation have dropped columns
  bool HasDroppedColumns() const override;

  // number of non-dropped columns
  uint32_t NonDroppedColsCount() const override;

  // return the absolute position of the given attribute position excluding dropped columns
  uint32_t NonDroppedColAt(uint32_t pos) const override;

  // return the position of a column in the metadata object given the attribute number in the system catalog
  uint32_t GetPosFromAttno(int32_t attno) const override;

  // return the original positions of all the non-dropped columns
  ULongPtrArray *NonDroppedColsArray() const override;

  // number of system columns
  uint32_t SystemColumnsCount() const override;

  // retrieve the column at the given position
  const IMDColumn *GetMdCol(uint32_t pos) const override;

  // number of key sets
  uint32_t KeySetCount() const override;

  // key set at given position
  const ULongPtrArray *KeySetAt(uint32_t pos) const override;

  // return true if a hash distributed table needs to be considered as random
  bool ConvertHashToRandom() const override;

  // is this a partitioned table
  bool IsPartitioned() const override;

  // number of partition keys
  uint32_t PartColumnCount() const override;

  // retrieve the partition key column at the given position
  const IMDColumn *PartColAt(uint32_t pos) const override;

  // retrieve list of partition types
  CharPtrArray *GetPartitionTypes() const override;

  // retrieve the partition type of the given level
  char PartTypeAtLevel(uint32_t ulLevel) const override;

  // number of indices
  uint32_t IndexCount() const override;

  // retrieve the id of the metadata cache index at the given position
  IMDId *IndexMDidAt(uint32_t pos) const override;

  // serialize metadata relation in DXL format given a serializer object

  // number of check constraints
  uint32_t CheckConstraintCount() const override;

  // retrieve the id of the check constraint cache at the given position
  IMDId *CheckConstraintMDidAt(uint32_t pos) const override;

  // part constraint
  CDXLNode *MDPartConstraint() const override;

  // child partition oids
  IMdIdArray *ChildPartitionMdids() const override;

  IMDId *ForeignServer() const override;

  CDouble Rows() const override;

#ifdef GPOS_DEBUG
  // debug print of the metadata relation
  void DebugPrint(IOstream &os) const override;
#endif
};
}  // namespace gpmd

#endif  // !GPMD_CMDRelationGPDB_H

// EOF
