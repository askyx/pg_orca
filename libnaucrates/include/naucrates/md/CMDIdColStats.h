//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdColStats.h
//
//	@doc:
//		Class for representing mdids for column statistics
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIdColStats_H
#define GPMD_CMDIdColStats_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/string/CWStringConst.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CSystemId.h"

namespace gpmd {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMDIdColStats
//
//	@doc:
//		Class for representing ids of column stats objects
//
//---------------------------------------------------------------------------
class CMDIdColStats : public IMDId {
 private:
  // mdid of base relation
  CMDIdGPDB *m_rel_mdid;

  // position of the attribute in the base relation
  uint32_t m_attr_pos;

  // buffer for the serialized mdid
  wchar_t m_mdid_buffer[GPDXL_MDID_LENGTH];

  // string representation of the mdid
  mutable CWStringStatic m_str;

  // serialize mdid
  void Serialize() const;

 public:
  CMDIdColStats(const CMDIdColStats &) = delete;

  // ctor
  CMDIdColStats(CMDIdGPDB *rel_mdid, uint32_t attno);

  // dtor
  ~CMDIdColStats() override;

  EMDIdType MdidType() const override { return EmdidColStats; }

  // string representation of mdid
  const wchar_t *GetBuffer() const override;

  // source system id
  CSystemId Sysid() const override { return m_rel_mdid->Sysid(); }

  // accessors
  IMDId *GetRelMdId() const;
  uint32_t Position() const;

  // equality check
  bool Equals(const IMDId *mdid) const override;

  // computes the hash value for the metadata id
  uint32_t HashValue() const override {
    return gpos::CombineHashes(m_rel_mdid->HashValue(), gpos::HashValue(&m_attr_pos));
  }

  // is the mdid valid
  bool IsValid() const override { return IMDId::IsValid(m_rel_mdid); }

  // debug print of the metadata id
  IOstream &OsPrint(IOstream &os) const override;

  // const converter
  static const CMDIdColStats *CastMdid(const IMDId *mdid) {
    GPOS_ASSERT(nullptr != mdid && EmdidColStats == mdid->MdidType());

    return dynamic_cast<const CMDIdColStats *>(mdid);
  }

  // non-const converter
  static CMDIdColStats *CastMdid(IMDId *mdid) {
    GPOS_ASSERT(nullptr != mdid && EmdidColStats == mdid->MdidType());

    return dynamic_cast<CMDIdColStats *>(mdid);
  }

  // make a copy in the given memory pool
  IMDId *Copy(CMemoryPool *mp) const override {
    CMDIdGPDB *mdid_rel = CMDIdGPDB::CastMdid(m_rel_mdid->Copy(mp));
    return GPOS_NEW(mp) CMDIdColStats(mdid_rel, m_attr_pos);
  }
};

}  // namespace gpmd

#endif  // !GPMD_CMDIdColStats_H

// EOF
