//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IDatumOid.h
//
//	@doc:
//		Base abstract class for oid representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumOid_H
#define GPNAUCRATES_IDatumOid_H

#include "gpos/base.h"
#include "naucrates/base/IDatum.h"

namespace gpnaucrates {
//---------------------------------------------------------------------------
//	@class:
//		IDatumOid
//
//	@doc:
//		Base abstract class for oid representation
//
//---------------------------------------------------------------------------
class IDatumOid : public IDatum {
 private:
 public:
  IDatumOid(const IDatumOid &) = delete;

  // ctor
  IDatumOid() = default;

  // dtor
  ~IDatumOid() override = default;

  // accessor for datum type
  IMDType::ETypeInfo GetDatumType() override { return IMDType::EtiOid; }

  // accessor of oid value
  virtual OID OidValue() const = 0;

  // can datum be mapped to a double
  bool IsDatumMappableToDouble() const override { return true; }

  // map to double for stats computation
  CDouble GetDoubleMapping() const override { return CDouble(GetLINTMapping()); }

  // can datum be mapped to int64_t
  bool IsDatumMappableToLINT() const override { return true; }

  // map to int64_t for statistics computation
  int64_t GetLINTMapping() const override { return int64_t(OidValue()); }

  // byte array representation of datum
  const uint8_t *GetByteArrayValue() const override {
    GPOS_ASSERT(!"Invalid invocation of MakeCopyOfValue");
    return nullptr;
  }

  // does the datum need to be padded before statistical derivation
  bool NeedsPadding() const override { return false; }

  // return the padded datum
  IDatum *MakePaddedDatum(CMemoryPool *,  // mp,
                          uint32_t        // col_len
  ) const override {
    GPOS_ASSERT(!"Invalid invocation of MakePaddedDatum");
    return nullptr;
  }

  // does datum support like predicate
  bool SupportsLikePredicate() const override { return false; }

  // return the default scale factor of like predicate
  CDouble GetLikePredicateScaleFactor() const override {
    GPOS_ASSERT(!"Invalid invocation of DLikeSelectivity");
    return false;
  }
};  // class IDatumOid
}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_IDatumOid_H

// EOF
