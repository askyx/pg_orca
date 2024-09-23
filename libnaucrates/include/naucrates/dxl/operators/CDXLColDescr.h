//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColDescr.h
//
//	@doc:
//		Class for representing column descriptors.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLColDescr_H
#define GPDXL_CDXLColDescr_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl {
using namespace gpmd;

// fwd decl

class CDXLColDescr;

using CDXLColDescrArray = CDynamicPtrArray<CDXLColDescr, CleanupRelease>;

//---------------------------------------------------------------------------
//	@class:
//		CDXLColDescr
//
//	@doc:
//		Class for representing column descriptors in DXL operators
//
//---------------------------------------------------------------------------
class CDXLColDescr : public CRefCount {
 private:
  // name
  CMDName *m_md_name;

  // column id: unique identifier of that instance of the column in the query
  uint32_t m_column_id;

  // attribute number in the database (corresponds to varattno in GPDB)
  int32_t m_attr_no;

  // mdid of column's type
  IMDId *m_column_mdid_type;

  int32_t m_type_modifier;

  // is column dropped from the table: needed for correct restoring of attribute numbers in the range table entries
  bool m_is_dropped;

  // width of the column, for instance  char(10) column has width 10
  uint32_t m_column_width;

 public:
  CDXLColDescr(const CDXLColDescr &) = delete;

  // ctor
  CDXLColDescr(CMDName *, uint32_t column_id, int32_t attr_no, IMDId *column_mdid_type, int32_t type_modifier,
               bool is_dropped, uint32_t width = UINT32_MAX);

  // dtor
  ~CDXLColDescr() override;

  // column name
  const CMDName *MdName() const;

  // column identifier
  uint32_t Id() const;

  // attribute number of the column in the base table
  int32_t AttrNum() const;

  // is the column dropped in the base table
  bool IsDropped() const;

  // column type
  IMDId *MdidType() const;

  int32_t TypeModifier() const;

  // column width
  uint32_t Width() const;
};

}  // namespace gpdxl

#endif  // !GPDXL_CDXLColDescr_H

// EOF
