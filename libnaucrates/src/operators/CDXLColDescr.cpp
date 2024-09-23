//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColDescr.cpp
//
//	@doc:
//		Implementation of DXL column descriptors
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLColDescr.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::CDXLColDescr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLColDescr::CDXLColDescr(CMDName *md_name, uint32_t column_id, int32_t attr_no, IMDId *column_mdid_type,
                           int32_t type_modifier, bool is_dropped, uint32_t width)
    : m_md_name(md_name),
      m_column_id(column_id),
      m_attr_no(attr_no),
      m_column_mdid_type(column_mdid_type),
      m_type_modifier(type_modifier),
      m_is_dropped(is_dropped),
      m_column_width(width) {
  GPOS_ASSERT_IMP(m_is_dropped, 0 == m_md_name->GetMDName()->Length());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::~CDXLColDescr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLColDescr::~CDXLColDescr() {
  m_column_mdid_type->Release();
  GPOS_DELETE(m_md_name);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::MdName
//
//	@doc:
//		Returns the column name
//
//---------------------------------------------------------------------------
const CMDName *CDXLColDescr::MdName() const {
  return m_md_name;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::Id
//
//	@doc:
//		Returns the column Id
//
//---------------------------------------------------------------------------
uint32_t CDXLColDescr::Id() const {
  return m_column_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::AttrNum
//
//	@doc:
//		Returns the column attribute number in GPDB
//
//---------------------------------------------------------------------------
int32_t CDXLColDescr::AttrNum() const {
  return m_attr_no;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::MdidType
//
//	@doc:
//		Returns the type id for this column
//
//---------------------------------------------------------------------------
IMDId *CDXLColDescr::MdidType() const {
  return m_column_mdid_type;
}

int32_t CDXLColDescr::TypeModifier() const {
  return m_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::IsDropped
//
//	@doc:
//		Is the column dropped from the relation
//
//---------------------------------------------------------------------------
bool CDXLColDescr::IsDropped() const {
  return m_is_dropped;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::Width
//
//	@doc:
//		Returns the width of the column
//
//---------------------------------------------------------------------------
uint32_t CDXLColDescr::Width() const {
  return m_column_width;
}
