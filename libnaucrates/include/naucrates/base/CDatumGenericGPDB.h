//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumGenericGPDB.h
//
//	@doc:
//		GPDB-specific generic datum representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumGenericGPDB_H
#define GPNAUCRATES_CDatumGenericGPDB_H

#include "gpos/base.h"
#include "naucrates/base/IDatumGeneric.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"

#define GPDB_DATUM_HDRSZ 4

namespace gpnaucrates {
//---------------------------------------------------------------------------
//	@class:
//		CDatumGenericGPDB
//
//	@doc:
//		GPDB-specific generic datum representation
//
//---------------------------------------------------------------------------
class CDatumGenericGPDB : public IDatumGeneric {
 private:
  // memory pool
  CMemoryPool *m_mp;

  // size in bytes
  uint32_t m_size;

  // a pointer to datum value
  uint8_t *m_bytearray_value;

  // is null
  bool m_is_null;

  // type information
  IMDId *m_mdid;

  int32_t m_type_modifier;

  // cached type information (can be set from const methods)
  mutable const IMDType *m_cached_type;

  // long int value used for statistic computation
  int64_t m_stats_comp_val_int;

  // double value used for statistic computation
  CDouble m_stats_comp_val_double;

 public:
  CDatumGenericGPDB(const CDatumGenericGPDB &) = delete;

  // ctor
  CDatumGenericGPDB(CMemoryPool *mp, IMDId *mdid, int32_t type_modifier, const void *src, uint32_t size, bool is_null,
                    int64_t stats_comp_val_int, CDouble stats_comp_val_double);

  // dtor
  ~CDatumGenericGPDB() override;

  // accessor of metadata type id
  IMDId *MDId() const override;

  int32_t TypeModifier() const override;

  // accessor of size
  uint32_t Size() const override;

  // accessor of is null
  bool IsNull() const override;

  // return string representation
  const CWStringConst *GetStrRepr(CMemoryPool *mp) const override;

  // hash function
  uint32_t HashValue() const override;

  // match function for datums
  bool Matches(const IDatum *datum) const override;

  // copy datum
  IDatum *MakeCopy(CMemoryPool *mp) const override;

  // print function
  IOstream &OsPrint(IOstream &os) const override;

  // accessor to bytearray, creates a copy
  virtual uint8_t *MakeCopyOfValue(CMemoryPool *mp, uint32_t *pulLength) const;

  // statistics related APIs

  // can datum be mapped to a double
  bool IsDatumMappableToDouble() const override;

  // map to double for stats computation
  CDouble GetDoubleMapping() const override {
    GPOS_ASSERT(IsDatumMappableToDouble());

    return m_stats_comp_val_double;
  }

  // can datum be mapped to int64_t
  bool IsDatumMappableToLINT() const override;

  // map to int64_t for statistics computation
  int64_t GetLINTMapping() const override {
    GPOS_ASSERT(IsDatumMappableToLINT());

    return m_stats_comp_val_int;
  }

  // byte array representation of datum
  const uint8_t *GetByteArrayValue() const override;

  // stats equality
  bool StatsAreEqual(const IDatum *datum) const override;

  // does the datum need to be padded before statistical derivation
  bool NeedsPadding() const override;

  // return the padded datum
  IDatum *MakePaddedDatum(CMemoryPool *mp, uint32_t col_len) const override;

  // does datum support like predicate
  bool SupportsLikePredicate() const override { return true; }

  // return the default scale factor of like predicate
  CDouble GetLikePredicateScaleFactor() const override;

  // default selectivity of the trailing wildcards
  virtual CDouble GetTrailingWildcardSelectivity(const uint8_t *pba, uint32_t ulPos) const;

  // selectivities needed for LIKE predicate statistics evaluation
  static const CDouble DefaultFixedCharSelectivity;
  static const CDouble DefaultCharRangeSelectivity;
  static const CDouble DefaultAnyCharSelectivity;
  static const CDouble DefaultCdbRanchorSelectivity;
  static const CDouble DefaultCdbRolloffSelectivity;

};  // class CDatumGenericGPDB
}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_CDatumGenericGPDB_H

// EOF
