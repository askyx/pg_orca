//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDatumInt8GPDB.h
//
//	@doc:
//		GPDB-specific Int8 representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumInt8GPDB_H
#define GPNAUCRATES_CDatumInt8GPDB_H

#include "gpos/base.h"
#include "naucrates/base/IDatumInt8.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"

namespace gpnaucrates {
//---------------------------------------------------------------------------
//	@class:
//		CDatumInt8GPDB
//
//	@doc:
//		GPDB-specific Int8 representation
//
//---------------------------------------------------------------------------
class CDatumInt8GPDB : public IDatumInt8 {
 private:
  // type information
  IMDId *m_mdid;

  // integer value
  int64_t m_val;

  // is null
  bool m_is_null;

 public:
  CDatumInt8GPDB(const CDatumInt8GPDB &) = delete;

  // ctors
  CDatumInt8GPDB(CSystemId sysid, int64_t val, bool is_null = false);
  CDatumInt8GPDB(IMDId *mdid, int64_t val, bool is_null = false);

  // dtor
  ~CDatumInt8GPDB() override;

  // accessor of metadata type id
  IMDId *MDId() const override;

  // accessor of size
  uint32_t Size() const override;

  // accessor of integer value
  int64_t Value() const override;

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

};  // class CDatumInt8GPDB

}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_CDatumInt8GPDB_H

// EOF
