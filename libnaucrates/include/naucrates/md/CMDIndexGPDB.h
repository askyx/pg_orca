//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIndexGPDB.h
//
//	@doc:
//		Implementation of indexes in the metadata cache
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIndexGPDB_H
#define GPMD_CMDIndexGPDB_H

#include "gpos/base.h"
#include "naucrates/md/IMDIndex.h"

namespace gpmd {
using namespace gpos;
using namespace gpdxl;

// fwd decl
class IMDPartConstraint;

//---------------------------------------------------------------------------
//	@class:
//		CMDIndexGPDB
//
//	@doc:
//		Class for indexes in the metadata cache
//
//---------------------------------------------------------------------------
class CMDIndexGPDB : public IMDIndex {
 private:
  // memory pool
  CMemoryPool *m_mp;

  // index mdid
  IMDId *m_mdid;

  // table name
  CMDName *m_mdname;

  // is the index clustered
  bool m_clustered;

  // is the index partitioned
  bool m_partitioned;

  // Can index AM order
  bool m_amcanorder;

  // index type
  EmdindexType m_index_type;

  // type of items returned by index
  IMDId *m_mdid_item_type;

  // index key columns
  ULongPtrArray *m_index_key_cols_array;

  // included columns
  ULongPtrArray *m_included_cols_array;

  // returnable columns
  ULongPtrArray *m_returnable_cols_array;

  // operator families for each index key
  IMdIdArray *m_mdid_opfamilies_array;

  // DXL for object
  const CWStringDynamic *m_dxl_str = nullptr;

  // Child index oids
  IMdIdArray *m_child_index_oids;

  // index key's sort direction
  ULongPtrArray *m_sort_direction;

  // index key's NULLs direction
  ULongPtrArray *m_nulls_direction;

 public:
  CMDIndexGPDB(const CMDIndexGPDB &) = delete;

  // ctor
  CMDIndexGPDB(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, bool is_clustered, bool is_partitioned, bool amcanorder,
               EmdindexType index_type, IMDId *mdid_item_type, ULongPtrArray *index_key_cols_array,
               ULongPtrArray *included_cols_array, ULongPtrArray *returnable_cols_array,
               IMdIdArray *mdid_opfamilies_array, IMdIdArray *child_index_oids, ULongPtrArray *sort_direction,
               ULongPtrArray *nulls_direction);

  // dtor
  ~CMDIndexGPDB() override;

  // index mdid
  IMDId *MDId() const override;

  // index name
  CMDName Mdname() const override;

  // is the index clustered
  bool IsClustered() const override;

  // is the index partitioned
  bool IsPartitioned() const override;

  // Does index AM support ordering
  bool CanOrder() const override;

  // index type
  EmdindexType IndexType() const override;

  // number of keys
  uint32_t Keys() const override;

  // return the n-th key column
  uint32_t KeyAt(uint32_t pos) const override;

  // return the position of the key column
  uint32_t GetKeyPos(uint32_t column) const override;

  // number of included columns
  uint32_t IncludedCols() const override;

  // return the n-th included column
  uint32_t IncludedColAt(uint32_t pos) const override;

  // number of returnable columns
  uint32_t ReturnableCols() const override;

  // return the n-th returnable column
  uint32_t ReturnableColAt(uint32_t pos) const override;

  // return the n-th column sort direction
  uint32_t KeySortDirectionAt(uint32_t pos) const override;

  // return the n-th column nulls direction
  uint32_t KeyNullsDirectionAt(uint32_t pos) const override;

  // return the position of the included column
  uint32_t GetIncludedColPos(uint32_t column) const override;

  // DXL string for index

  // serialize MD index in DXL format given a serializer object

  // type id of items returned by the index
  IMDId *GetIndexRetItemTypeMdid() const override;

  // check if given scalar comparison can be used with the index key
  // at the specified position
  bool IsCompatible(const IMDScalarOp *md_scalar_op, uint32_t key_pos) const override;

  // child index oids
  IMdIdArray *ChildIndexMdids() const override;

#ifdef GPOS_DEBUG
  // debug print of the MD index
  void DebugPrint(IOstream &os) const override;
#endif
};
}  // namespace gpmd

#endif  // !GPMD_CMDIndexGPDB_H

// EOF
