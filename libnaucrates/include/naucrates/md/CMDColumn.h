//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDColumn.h
//
//	@doc:
//		Class for representing metadata about relation's columns.
//---------------------------------------------------------------------------

#ifndef GPMD_CDXLColumn_H
#define GPMD_CDXLColumn_H

#include "gpos/base.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDColumn.h"

// fwd decl
namespace gpdxl {
class CDXLNode;

}  // namespace gpdxl

namespace gpmd {
//---------------------------------------------------------------------------
//	@class:
//		CMDColumn
//
//	@doc:
//		Class for representing metadata about relation's columns.
//
//---------------------------------------------------------------------------
class CMDColumn : public IMDColumn {
 private:
  // attribute name
  CMDName *m_mdname;

  // attribute number
  int32_t m_attno;

  // column type
  IMDId *m_mdid_type;

  int32_t m_type_modifier;

  // is NULL an allowed value for the attribute
  bool m_is_nullable;

  // is column dropped
  bool m_is_dropped;

  // length of the column
  uint32_t m_length;

 public:
  CMDColumn(const CMDColumn &) = delete;

  // ctor
  CMDColumn(CMDName *mdname, int32_t attrnum, IMDId *mdid_type, int32_t type_modifier, bool is_nullable,
            bool is_dropped, uint32_t length = UINT32_MAX);

  // dtor
  ~CMDColumn() override;

  // accessors
  CMDName Mdname() const override;

  // column type
  IMDId *MdidType() const override;

  int32_t TypeModifier() const override;

  // attribute number
  int32_t AttrNum() const override;

  // is this a system column
  bool IsSystemColumn() const override { return (0 > m_attno); }

  // length of the column
  uint32_t Length() const override { return m_length; }

  // is the column nullable
  bool IsNullable() const override;

  // is the column dropped
  bool IsDropped() const override;

  // serialize metadata object in DXL format given a serializer object
  virtual

#ifdef GPOS_DEBUG
      // debug print of the column
      void
      DebugPrint(IOstream &os) const override;
#endif
};

// array of metadata column descriptor
using CMDColumnArray = CDynamicPtrArray<CMDColumn, CleanupRelease>;

}  // namespace gpmd

#endif  // !GPMD_CDXLColumn_H

// EOF
