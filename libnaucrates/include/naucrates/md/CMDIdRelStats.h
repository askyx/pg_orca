//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdRelStats.h
//
//	@doc:
//		Class for representing mdids for relation statistics
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIdRelStats_H
#define GPMD_CMDIdRelStats_H

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
//		CMDIdRelStats
//
//	@doc:
//		Class for representing ids of relation stats objects
//
//---------------------------------------------------------------------------
class CMDIdRelStats : public IMDId {
 private:
  // mdid of base relation
  CMDIdGPDB *m_rel_mdid;

  // buffer for the serialzied mdid
  wchar_t m_mdid_array[GPDXL_MDID_LENGTH];

  // string representation of the mdid
  mutable CWStringStatic m_str;

  // serialize mdid
  void Serialize() const;

 public:
  CMDIdRelStats(const CMDIdRelStats &) = delete;

  // ctor
  explicit CMDIdRelStats(CMDIdGPDB *rel_mdid);

  // dtor
  ~CMDIdRelStats() override;

  EMDIdType MdidType() const override { return EmdidRelStats; }

  // string representation of mdid
  const wchar_t *GetBuffer() const override;

  // source system id
  CSystemId Sysid() const override { return m_rel_mdid->Sysid(); }

  // accessors
  IMDId *GetRelMdId() const;

  // equality check
  bool Equals(const IMDId *mdid) const override;

  // computes the hash value for the metadata id
  uint32_t HashValue() const override { return m_rel_mdid->HashValue(); }

  // is the mdid valid
  bool IsValid() const override { return IMDId::IsValid(m_rel_mdid); }

  // debug print of the metadata id
  IOstream &OsPrint(IOstream &os) const override;

  // const converter
  static const CMDIdRelStats *CastMdid(const IMDId *mdid) {
    GPOS_ASSERT(nullptr != mdid && EmdidRelStats == mdid->MdidType());

    return dynamic_cast<const CMDIdRelStats *>(mdid);
  }

  // non-const converter
  static CMDIdRelStats *CastMdid(IMDId *mdid) {
    GPOS_ASSERT(nullptr != mdid && EmdidRelStats == mdid->MdidType());

    return dynamic_cast<CMDIdRelStats *>(mdid);
  }

  // make a copy in the given memory pool
  IMDId *Copy(CMemoryPool *mp) const override {
    CMDIdGPDB *mdid_rel = CMDIdGPDB::CastMdid(m_rel_mdid->Copy(mp));
    return GPOS_NEW(mp) CMDIdRelStats(mdid_rel);
  }
};

}  // namespace gpmd

#endif  // !GPMD_CMDIdRelStats_H

// EOF
