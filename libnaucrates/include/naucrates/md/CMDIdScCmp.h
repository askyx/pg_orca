//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDIdScCmp.h
//
//	@doc:
//		Class for representing mdids of scalar comparison operators
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIdScCmpFunc_H
#define GPMD_CMDIdScCmpFunc_H

#include "gpos/base.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDType.h"

namespace gpmd {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMDIdScCmp
//
//	@doc:
//		Class for representing ids of scalar comparison operators
//
//---------------------------------------------------------------------------
class CMDIdScCmp : public IMDId {
 private:
  // mdid of source type
  CMDIdGPDB *m_mdid_left;

  // mdid of destinatin type
  CMDIdGPDB *m_mdid_right;

  // comparison type
  IMDType::ECmpType m_comparision_type;

  // buffer for the serialized mdid
  wchar_t m_mdid_array[GPDXL_MDID_LENGTH];

  // string representation of the mdid
  mutable CWStringStatic m_str;

  // serialize mdid
  void Serialize() const;

 public:
  CMDIdScCmp(const CMDIdScCmp &) = delete;

  // ctor
  CMDIdScCmp(CMDIdGPDB *left_mdid, CMDIdGPDB *right_mdid, IMDType::ECmpType cmp_type);

  // dtor
  ~CMDIdScCmp() override;

  EMDIdType MdidType() const override { return EmdidScCmp; }

  // string representation of mdid
  const wchar_t *GetBuffer() const override;

  // source system id
  CSystemId Sysid() const override { return m_mdid_left->Sysid(); }

  // left type id
  IMDId *GetLeftMdid() const;

  // right type id
  IMDId *GetRightMdid() const;

  IMDType::ECmpType ParseCmpType() const { return m_comparision_type; }

  // equality check
  bool Equals(const IMDId *mdid) const override;

  // computes the hash value for the metadata id
  uint32_t HashValue() const override;

  // is the mdid valid
  bool IsValid() const override {
    return IMDId::IsValid(m_mdid_left) && IMDId::IsValid(m_mdid_right) && IMDType::EcmptOther != m_comparision_type;
  }

  // debug print of the metadata id
  IOstream &OsPrint(IOstream &os) const override;

  // const converter
  static const CMDIdScCmp *CastMdid(const IMDId *mdid) {
    GPOS_ASSERT(nullptr != mdid && EmdidScCmp == mdid->MdidType());

    return dynamic_cast<const CMDIdScCmp *>(mdid);
  }

  // non-const converter
  static CMDIdScCmp *CastMdid(IMDId *mdid) {
    GPOS_ASSERT(nullptr != mdid && EmdidScCmp == mdid->MdidType());

    return dynamic_cast<CMDIdScCmp *>(mdid);
  }

  // make a copy in the given memory pool
  IMDId *Copy(CMemoryPool *mp) const override {
    CMDIdGPDB *mdid_left = CMDIdGPDB::CastMdid(m_mdid_left->Copy(mp));
    CMDIdGPDB *mdid_right = CMDIdGPDB::CastMdid(m_mdid_right->Copy(mp));

    return GPOS_NEW(mp) CMDIdScCmp(mdid_left, mdid_right, m_comparision_type);
  }
};
}  // namespace gpmd

#endif  // !GPMD_CMDIdScCmpFunc_H

// EOF
