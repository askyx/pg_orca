//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTableDescr.cpp
//
//	@doc:
//		Implementation of DXL table descriptors
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLTableDescr.h"

#include "gpos/string/CWStringDynamic.h"

using namespace gpos;
using namespace gpdxl;

#define GPDXL_DEFAULT_USERID 0
#define GPDXL_INVALID_LOCKMODE -1

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::CDXLTableDescr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLTableDescr::CDXLTableDescr(CMemoryPool *mp, IMDId *mdid, CMDName *mdname, ULONG ulExecuteAsUser, int lockmode,
                               ULONG acl_mode, ULONG assigned_query_id_for_target_rel)
    : m_mdid(mdid),
      m_mdname(mdname),
      m_dxl_column_descr_array(nullptr),
      m_execute_as_user_id(ulExecuteAsUser),
      m_lockmode(lockmode),
      m_acl_mode(acl_mode),
      m_assigned_query_id_for_target_rel(assigned_query_id_for_target_rel) {
  GPOS_ASSERT(nullptr != m_mdname);
  m_dxl_column_descr_array = GPOS_NEW(mp) CDXLColDescrArray(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::~CDXLTableDescr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLTableDescr::~CDXLTableDescr() {
  m_mdid->Release();
  GPOS_DELETE(m_mdname);
  CRefCount::SafeRelease(m_dxl_column_descr_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::MDId
//
//	@doc:
//		Return the metadata id for the table
//
//---------------------------------------------------------------------------
IMDId *CDXLTableDescr::MDId() const {
  return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::MdName
//
//	@doc:
//		Return table name
//
//---------------------------------------------------------------------------
const CMDName *CDXLTableDescr::MdName() const {
  return m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::Arity
//
//	@doc:
//		Return number of columns in the table
//
//---------------------------------------------------------------------------
ULONG
CDXLTableDescr::Arity() const {
  return (m_dxl_column_descr_array == nullptr) ? 0 : m_dxl_column_descr_array->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::GetExecuteAsUserId
//
//	@doc:
//		Id of the user the table needs to be accessed with
//
//---------------------------------------------------------------------------
ULONG
CDXLTableDescr::GetExecuteAsUserId() const {
  return m_execute_as_user_id;
}

INT CDXLTableDescr::LockMode() const {
  return m_lockmode;
}

ULONG
CDXLTableDescr::GetAclMode() const {
  return m_acl_mode;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::SetColumnDescriptors
//
//	@doc:
//		Set the list of column descriptors
//
//---------------------------------------------------------------------------
void CDXLTableDescr::SetColumnDescriptors(CDXLColDescrArray *dxl_column_descr_array) {
  CRefCount::SafeRelease(m_dxl_column_descr_array);
  m_dxl_column_descr_array = dxl_column_descr_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::AddColumnDescr
//
//	@doc:
//		Add a column to the list of column descriptors
//
//---------------------------------------------------------------------------
void CDXLTableDescr::AddColumnDescr(CDXLColDescr *column_descr_dxl) {
  GPOS_ASSERT(nullptr != m_dxl_column_descr_array);
  GPOS_ASSERT(nullptr != column_descr_dxl);
  m_dxl_column_descr_array->Append(column_descr_dxl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::GetColumnDescrAt
//
//	@doc:
//		Get the column descriptor at the specified position from the col descr list
//
//---------------------------------------------------------------------------
const CDXLColDescr *CDXLTableDescr::GetColumnDescrAt(ULONG idx) const {
  GPOS_ASSERT(idx < Arity());

  return (*m_dxl_column_descr_array)[idx];
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::GetAssignedQueryIdForTargetRel
//
//	@doc:
//		Return id of query, to which TableDescr belongs to
//		(if this descriptor points to a result (target) entry,
//		else UNASSIGNED_QUERYID returned)
//
//---------------------------------------------------------------------------
ULONG
CDXLTableDescr::GetAssignedQueryIdForTargetRel() const {
  return m_assigned_query_id_for_target_rel;
}

// EOF
