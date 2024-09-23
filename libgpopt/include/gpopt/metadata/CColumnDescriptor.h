//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnDescriptor.h
//
//	@doc:
//		Abstraction of columns in tables, functions, foreign tables etc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CColumnDescriptor_H
#define GPOPT_CColumnDescriptor_H

#include "gpopt/metadata/CName.h"
#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/IMDType.h"

namespace gpopt {
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CColumnDescriptor
//
//	@doc:
//		Metadata abstraction for columns that exist in the catalog;
//		Transient columns as computed during query execution do not have a
//		column descriptor;
//
//---------------------------------------------------------------------------
class CColumnDescriptor : public CRefCount {
 private:
  // type information
  const IMDType *m_pmdtype;

  // type modifier
  const int32_t m_type_modifier;

  // name of column -- owned
  CName m_name;

  // attribute number
  int32_t m_iAttno;

  // does column allow null values?
  bool m_is_nullable;

  // width of the column, for instance  char(10) column has width 10
  uint32_t m_width;

  // is the column a distribution col
  bool m_is_dist_col;

  // is the column a partition col
  bool m_is_part_col;

 public:
  // ctor
  CColumnDescriptor(CMemoryPool *mp, const IMDType *pmdtype, int32_t type_modifier, const CName &name, int32_t attno,
                    bool is_nullable, uint32_t ulWidth = UINT32_MAX);

  // dtor
  ~CColumnDescriptor() override;

  // return column name
  const CName &Name() const { return m_name; }

  // return metadata type
  const IMDType *RetrieveType() const { return m_pmdtype; }

  // type modifier
  int32_t TypeModifier() const { return m_type_modifier; }

  // return attribute number
  int32_t AttrNum() const { return m_iAttno; }

  // does column allow null values?
  bool IsNullable() const { return m_is_nullable; }

  // is this a system column
  virtual bool IsSystemColumn() const { return (0 > m_iAttno); }

  // width of the column
  virtual uint32_t Width() const { return m_width; }

  // is this a distribution column
  bool IsDistCol() const { return m_is_dist_col; }

  // is this a partition column
  bool IsPartCol() const { return m_is_part_col; }

  // set this column as a distribution column
  void SetAsDistCol() { m_is_dist_col = true; }

  // set this column as a partition column
  void SetAsPartCol() { m_is_part_col = true; }

  IOstream &OsPrint(IOstream &os) const;

  bool operator==(const CColumnDescriptor &other) const {
    if (this == &other) {
      // same object reference
      return true;
    }

    return Name().Equals(other.Name()) && RetrieveType()->MDId()->Equals(other.RetrieveType()->MDId()) &&
           TypeModifier() == other.TypeModifier() && AttrNum() == other.AttrNum() &&
           IsNullable() == other.IsNullable() && IsSystemColumn() == other.IsSystemColumn() &&
           Width() == other.Width() && IsDistCol() == other.IsDistCol();
  }

};  // class CColumnDescriptor
}  // namespace gpopt

#endif  // !GPOPT_CColumnDescriptor_H

// EOF
