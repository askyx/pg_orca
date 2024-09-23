//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CColRefTable.h
//
//	@doc:
//		Column reference implementation for base table columns
//---------------------------------------------------------------------------
#ifndef GPOS_CColRefTable_H
#define GPOS_CColRefTable_H

#include "gpopt/base/CColRef.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "gpopt/metadata/CName.h"
#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CList.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CColRefTable
//
//	@doc:
//		Column reference for base table columns
//
//---------------------------------------------------------------------------
class CColRefTable : public CColRef {
 private:
  // attno from catalog
  int32_t m_iAttno;

  // does column allow null values
  bool m_is_nullable;

  // id of the operator which is the source of this column reference
  // not owned
  uint32_t m_ulSourceOpId;

  // is the column a distribution key
  bool m_is_dist_col;

  // is the column a partition key
  bool m_is_part_col;

  // width of the column, for instance  char(10) column has width 10
  uint32_t m_width;

 public:
  CColRefTable(const CColRefTable &) = delete;

  // ctors
  CColRefTable(const CColumnDescriptor *pcd, uint32_t id, const CName *pname, uint32_t ulOpSource);

  CColRefTable(const IMDType *pmdtype, int32_t type_modifier, int32_t attno, bool is_nullable, uint32_t id,
               const CName *pname, uint32_t ulOpSource, bool is_dist_col, uint32_t ulWidth = UINT32_MAX);

  // dtor
  ~CColRefTable() override;

  // accessor of column reference type
  CColRef::Ecolreftype Ecrt() const override { return CColRef::EcrtTable; }

  // accessor of attribute number
  int32_t AttrNum() const { return m_iAttno; }

  // does column allow null values?
  bool IsNullable() const { return m_is_nullable; }

  // is column a system column?
  bool IsSystemCol() const override {
    // TODO-  04/13/2012, make this check system independent
    // using MDAccessor
    return 0 >= m_iAttno;
  }

  // is column a distribution column?
  bool IsDistCol() const override { return m_is_dist_col; }

  // is column a partition column?
  bool IsPartCol() const override { return m_is_part_col; }

  // width of the column
  uint32_t Width() const { return m_width; }

  // id of source operator
  uint32_t UlSourceOpId() const { return m_ulSourceOpId; }

  // conversion
  static CColRefTable *PcrConvert(CColRef *cr) {
    GPOS_ASSERT(cr->Ecrt() == CColRef::EcrtTable);
    return dynamic_cast<CColRefTable *>(cr);
  }

};  // class CColRefTable
}  // namespace gpopt

#endif  // !GPOS_CColRefTable_H

// EOF
