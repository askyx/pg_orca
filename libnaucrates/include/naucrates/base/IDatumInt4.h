//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IDatumInt4.h
//
//	@doc:
//		Base abstract class for int4 representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumInt4_H
#define GPNAUCRATES_IDatumInt4_H

#include "gpos/base.h"
#include "naucrates/base/IDatum.h"

namespace gpnaucrates {
//---------------------------------------------------------------------------
//	@class:
//		IDatumInt4
//
//	@doc:
//		Base abstract class for int4 representation
//
//---------------------------------------------------------------------------
class IDatumInt4 : public IDatum {
 private:
 public:
  IDatumInt4(const IDatumInt4 &) = delete;

  // ctor
  IDatumInt4() = default;

  // dtor
  ~IDatumInt4() override = default;

  // accessor for datum type
  IMDType::ETypeInfo GetDatumType() override { return IMDType::EtiInt4; }

  // accessor of integer value
  virtual int32_t Value() const = 0;

  // can datum be mapped to a double
  bool IsDatumMappableToDouble() const override { return true; }

  // map to double for stats computation
  CDouble GetDoubleMapping() const override { return CDouble(Value()); }

  // can datum be mapped to int64_t
  bool IsDatumMappableToLINT() const override { return true; }

  // map to int64_t for statistics computation
  int64_t GetLINTMapping() const override { return int64_t(Value()); }

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

};  // class IDatumInt4

}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_IDatumInt4_H

// EOF
