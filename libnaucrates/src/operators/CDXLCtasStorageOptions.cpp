//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLCtasStorageOptions.cpp
//
//	@doc:
//		Implementation of DXL CTAS storage options
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLCtasStorageOptions.h"

#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::CDXLCtasStorageOptions
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::CDXLCtasStorageOptions(CMDName *mdname_tablespace, ECtasOnCommitAction ctas_on_commit_action,
                                               CDXLCtasOptionArray *ctas_storage_option_array)
    : m_mdname_tablespace(mdname_tablespace),
      m_ctas_on_commit_action(ctas_on_commit_action),
      m_ctas_storage_option_array(ctas_storage_option_array) {
  GPOS_ASSERT(EctascommitSentinel > ctas_on_commit_action);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::~CDXLCtasStorageOptions
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::~CDXLCtasStorageOptions() {
  GPOS_DELETE(m_mdname_tablespace);
  CRefCount::SafeRelease(m_ctas_storage_option_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::GetMdNameTableSpace
//
//	@doc:
//		Returns the tablespace name
//
//---------------------------------------------------------------------------
CMDName *CDXLCtasStorageOptions::GetMdNameTableSpace() const {
  return m_mdname_tablespace;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::GetOnCommitAction
//
//	@doc:
//		Returns the OnCommit ctas spec
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::ECtasOnCommitAction CDXLCtasStorageOptions::GetOnCommitAction() const {
  return m_ctas_on_commit_action;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::CDXLCtasOptionArray
//
//	@doc:
//		Returns array of storage options
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::CDXLCtasOptionArray *CDXLCtasStorageOptions::GetDXLCtasOptionArray() const {
  return m_ctas_storage_option_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::GetOnCommitActionStr
//
//	@doc:
//		String representation of OnCommit action spec
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLCtasStorageOptions::GetOnCommitActionStr(
    CDXLCtasStorageOptions::ECtasOnCommitAction ctas_on_commit_action) {
  switch (ctas_on_commit_action) {
    case EctascommitNOOP:
      return CDXLTokens::GetDXLTokenStr(EdxltokenOnCommitNOOP);

    case EctascommitPreserve:
      return CDXLTokens::GetDXLTokenStr(EdxltokenOnCommitPreserve);

    case EctascommitDelete:
      return CDXLTokens::GetDXLTokenStr(EdxltokenOnCommitDelete);

    case EctascommitDrop:
      return CDXLTokens::GetDXLTokenStr(EdxltokenOnCommitDrop);

    default:
      GPOS_ASSERT("Invalid on commit option");
      return nullptr;
  }
}

// EOF
