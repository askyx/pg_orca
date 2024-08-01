//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLIndexDescr.cpp
//
//	@doc:
//		Implementation of DXL index descriptors
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLIndexDescr.h"

#include "gpos/string/CWStringDynamic.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLIndexDescr::CDXLIndexDescr
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLIndexDescr::CDXLIndexDescr(IMDId *mdid, CMDName *mdname) : m_mdid(mdid), m_mdname(mdname) {
  GPOS_ASSERT(m_mdid->IsValid());
  GPOS_ASSERT(nullptr != m_mdname);
  GPOS_ASSERT(m_mdname->GetMDName()->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLIndexDescr::~CDXLIndexDescr
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLIndexDescr::~CDXLIndexDescr() {
  m_mdid->Release();
  GPOS_DELETE(m_mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLIndexDescr::MDId
//
//	@doc:
//		Return the metadata id for the index
//
//---------------------------------------------------------------------------
IMDId *CDXLIndexDescr::MDId() const {
  return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLIndexDescr::MdName
//
//	@doc:
//		Return index name
//
//---------------------------------------------------------------------------
const CMDName *CDXLIndexDescr::MdName() const {
  return m_mdname;
}
