//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDRequest.cpp
//
//	@doc:
//		Implementation of the class for metadata requests
//---------------------------------------------------------------------------

#include "naucrates/md/CMDRequest.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::CMDRequest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRequest::CMDRequest(CMemoryPool *mp, IMdIdArray *mdid_array, SMDTypeRequestArray *mdtype_request_array)
    : m_mp(mp), m_mdid_array(mdid_array), m_mdtype_request_array(mdtype_request_array) {
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != mdid_array);
  GPOS_ASSERT(nullptr != mdtype_request_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::CMDRequest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRequest::CMDRequest(CMemoryPool *mp, SMDTypeRequest *md_type_request)
    : m_mp(mp), m_mdid_array(nullptr), m_mdtype_request_array(nullptr) {
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != md_type_request);

  m_mdid_array = GPOS_NEW(m_mp) IMdIdArray(m_mp);
  m_mdtype_request_array = GPOS_NEW(m_mp) SMDTypeRequestArray(m_mp);

  m_mdtype_request_array->Append(md_type_request);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::~CMDRequest
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDRequest::~CMDRequest() {
  m_mdid_array->Release();
  m_mdtype_request_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::GetMDName
//
//	@doc:
//		Serialize system id
//
//---------------------------------------------------------------------------
CWStringDynamic *CMDRequest::GetStrRepr(CSystemId sysid) {
  CWStringDynamic *str = GPOS_NEW(m_mp) CWStringDynamic(m_mp);
  str->AppendFormat(GPOS_WSZ_LIT("%d.%ls"), sysid.MdidType(), sysid.GetBuffer());
  return str;
}
