//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdRelStats.cpp
//
//	@doc:
//		Implementation of mdids for relation statistics
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIdRelStats.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::CMDIdRelStats
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdRelStats::CMDIdRelStats(CMDIdGPDB *rel_mdid)
    : m_rel_mdid(rel_mdid), m_str(m_mdid_array, GPOS_ARRAY_SIZE(m_mdid_array)) {}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::~CMDIdRelStats
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdRelStats::~CMDIdRelStats() {
  m_rel_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void CMDIdRelStats::Serialize() const {
  if (m_str.Length() > 0) {
    return;
  }

  m_str.Reset();
  // serialize mdid as SystemType.Oid.Major.Minor
  m_str.AppendFormat(GPOS_WSZ_LIT("%d.%d.%d.%d"), MdidType(), m_rel_mdid->Oid(), m_rel_mdid->VersionMajor(),
                     m_rel_mdid->VersionMinor());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::GetBuffer
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const wchar_t *CMDIdRelStats::GetBuffer() const {
  Serialize();
  return m_str.GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::GetRelMdId
//
//	@doc:
//		Returns the base relation id
//
//---------------------------------------------------------------------------
IMDId *CMDIdRelStats::GetRelMdId() const {
  return m_rel_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::Equals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
bool CMDIdRelStats::Equals(const IMDId *mdid) const {
  if (nullptr == mdid || EmdidRelStats != mdid->MdidType()) {
    return false;
  }

  const CMDIdRelStats *rel_stats_mdid = CMDIdRelStats::CastMdid(mdid);

  return m_rel_mdid->Equals(rel_stats_mdid->GetRelMdId());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &CMDIdRelStats::OsPrint(IOstream &os) const {
  os << "(" << GetBuffer() << ")";
  return os;
}

// EOF
