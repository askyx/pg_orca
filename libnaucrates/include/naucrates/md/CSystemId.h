//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSystemId.h
//
//	@doc:
//		Class for representing the system id of a metadata provider
//---------------------------------------------------------------------------

#ifndef GPMD_CSystemId_H
#define GPMD_CSystemId_H

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

#define GPDXL_SYSID_LENGTH 10

namespace gpmd {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CSystemId
//
//	@doc:
//		Class for representing the system id of a metadata provider
//
//---------------------------------------------------------------------------
class CSystemId {
 private:
  // system id type
  IMDId::EMDIdType m_mdid_type;

  // system id
  wchar_t m_sysid_char[GPDXL_SYSID_LENGTH + 1];

 public:
  // ctor
  CSystemId(IMDId::EMDIdType mdid_type, const wchar_t *sysid_char, uint32_t length = GPDXL_SYSID_LENGTH);

  // copy ctor
  CSystemId(const CSystemId &);

  // type of system id
  IMDId::EMDIdType MdidType() const { return m_mdid_type; }

  // system id string
  const wchar_t *GetBuffer() const { return m_sysid_char; }

  // equality
  bool Equals(const CSystemId &sysid) const;

  // hash function
  uint32_t HashValue() const;
};

// dynamic arrays over md system id elements
using CSystemIdArray = CDynamicPtrArray<CSystemId, CleanupDelete>;
}  // namespace gpmd

#endif  // !GPMD_CSystemId_H

// EOF
