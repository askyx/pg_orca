//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDIndex.h
//
//	@doc:
//		Interface for indexes in the metadata cache
//---------------------------------------------------------------------------

#ifndef GPMD_IMDIndex_H
#define GPMD_IMDIndex_H

#include "gpos/base.h"
#include "naucrates/md/IMDCacheObject.h"

namespace gpmd {
using namespace gpos;

// fwd decl
class IMDPartConstraint;
class IMDScalarOp;

//---------------------------------------------------------------------------
//	@class:
//		IMDIndex
//
//	@doc:
//		Interface for indexes in the metadata cache
//
//---------------------------------------------------------------------------
class IMDIndex : public IMDCacheObject {
 public:
  // index type
  enum EmdindexType {
    EmdindBtree,   // btree
    EmdindHash,    // hash
    EmdindBitmap,  // bitmap
    EmdindGist,    // gist using btree or bitmap
    EmdindGin,     // gin using btree or bitmap
    EmdindBrin,    // brin
    EmdindSentinel
  };

  // object type
  Emdtype MDType() const override { return EmdtInd; }

  // is the index clustered
  virtual bool IsClustered() const = 0;

  // is the index partitioned
  virtual bool IsPartitioned() const = 0;

  // Does index AM support ordering
  virtual bool CanOrder() const = 0;

  // index type
  virtual EmdindexType IndexType() const = 0;

  // number of keys
  virtual uint32_t Keys() const = 0;

  // return the n-th key column
  virtual uint32_t KeyAt(uint32_t pos) const = 0;

  // return the position of the key column
  virtual uint32_t GetKeyPos(uint32_t pos) const = 0;

  // number of included columns
  virtual uint32_t IncludedCols() const = 0;

  // return the n-th included column
  virtual uint32_t IncludedColAt(uint32_t pos) const = 0;

  // number of returnable columns
  virtual uint32_t ReturnableCols() const = 0;

  // return the n-th returnable column
  virtual uint32_t ReturnableColAt(uint32_t pos) const = 0;

  // return the n-th column sort direction
  virtual uint32_t KeySortDirectionAt(uint32_t pos) const = 0;

  // return the n-th column nulls direction
  virtual uint32_t KeyNullsDirectionAt(uint32_t pos) const = 0;

  // return the position of the included column
  virtual uint32_t GetIncludedColPos(uint32_t column) const = 0;

  // type id of items returned by the index
  virtual IMDId *GetIndexRetItemTypeMdid() const = 0;

  // check if given scalar comparison can be used with the index key
  // at the specified position
  virtual bool IsCompatible(const IMDScalarOp *md_scalar_op, uint32_t key_pos) const = 0;

  // child index oids
  virtual IMdIdArray *ChildIndexMdids() const = 0;

  // index type as a string value
  static const CWStringConst *GetDXLStr(EmdindexType index_type);
};
}  // namespace gpmd

#endif  // !GPMD_IMDIndex_H

// EOF
