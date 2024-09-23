//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumBoolGPDB.h
//
//	@doc:
//		GPDB-specific bool representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumBoolGPDB_H
#define GPNAUCRATES_CDatumBoolGPDB_H

#include "gpos/base.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"

namespace gpnaucrates {
//---------------------------------------------------------------------------
//	@class:
//		CDatumBoolGPDB
//
//	@doc:
//		GPDB-specific bool representation
//
//---------------------------------------------------------------------------
class CDatumBoolGPDB : public IDatumBool {
 private:
  // type information
  IMDId *m_mdid;

  // boolean value
  bool m_value;

  // is null
  bool m_is_null;

 public:
  CDatumBoolGPDB(const CDatumBoolGPDB &) = delete;

  // ctors
  CDatumBoolGPDB(CSystemId sysid, bool value, bool is_null = false);
  CDatumBoolGPDB(IMDId *mdid, bool value, bool is_null = false);

  // dtor
  ~CDatumBoolGPDB() override;

  // accessor of metadata type mdid
  IMDId *MDId() const override;

  // accessor of boolean value
  bool GetValue() const override;

  // accessor of size
  uint32_t Size() const override;

  // accessor of is null
  bool IsNull() const override;

  // return string representation
  const CWStringConst *GetStrRepr(CMemoryPool *mp) const override;

  // hash function
  uint32_t HashValue() const override;

  // match function for datums
  bool Matches(const IDatum *) const override;

  // copy datum
  IDatum *MakeCopy(CMemoryPool *mp) const override;

  // print function
  IOstream &OsPrint(IOstream &os) const override;

};  // class CDatumBoolGPDB
}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_CDatumBoolGPDB_H

// EOF
