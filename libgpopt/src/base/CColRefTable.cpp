//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CColRefTable.cpp
//
//	@doc:
//		Implementation of column reference class
//---------------------------------------------------------------------------

#include "gpopt/base/CColRefTable.h"

#include "gpos/base.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::CColRefTable
//
//	@doc:
//		Ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRefTable::CColRefTable(const CColumnDescriptor *pcoldesc, uint32_t id, const CName *pname, uint32_t ulOpSource)
    : CColRef(pcoldesc->RetrieveType(), pcoldesc->TypeModifier(), id, pname),
      m_iAttno(0),
      m_ulSourceOpId(ulOpSource),
      m_width(pcoldesc->Width()) {
  GPOS_ASSERT(nullptr != pname);

  m_iAttno = pcoldesc->AttrNum();
  m_is_nullable = pcoldesc->IsNullable();
  m_is_dist_col = pcoldesc->IsDistCol();
  m_is_part_col = pcoldesc->IsPartCol();
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::CColRefTable
//
//	@doc:
//		Ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRefTable::CColRefTable(const IMDType *pmdtype, int32_t type_modifier, int32_t attno, bool is_nullable, uint32_t id,
                           const CName *pname, uint32_t ulOpSource, bool is_dist_col, uint32_t ulWidth)
    : CColRef(pmdtype, type_modifier, id, pname),
      m_iAttno(attno),
      m_is_nullable(is_nullable),
      m_ulSourceOpId(ulOpSource),
      m_is_dist_col(is_dist_col),
      m_width(ulWidth) {
  GPOS_ASSERT(nullptr != pname);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::~CColRefTable
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CColRefTable::~CColRefTable() = default;

// EOF
