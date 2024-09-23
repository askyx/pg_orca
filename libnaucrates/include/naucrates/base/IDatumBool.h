//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IDatumBool.h
//
//	@doc:
//		Base abstract class for bool representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumBool_H
#define GPNAUCRATES_IDatumBool_H

#include "gpos/base.h"
#include "naucrates/base/IDatum.h"

namespace gpnaucrates {
//---------------------------------------------------------------------------
//	@class:
//		IDatumBool
//
//	@doc:
//		Base abstract class for bool representation
//
//---------------------------------------------------------------------------
class IDatumBool : public IDatum {
 private:
 public:
  IDatumBool(const IDatumBool &) = delete;

  // ctor
  IDatumBool() = default;

  // dtor
  ~IDatumBool() override = default;

  // accessor for datum type
  IMDType::ETypeInfo GetDatumType() override { return IMDType::EtiBool; }

  // accessor of boolean value
  virtual bool GetValue() const = 0;

  // can datum be mapped to a double
  bool IsDatumMappableToDouble() const override { return true; }

  // map to double for stats computation
  CDouble GetDoubleMapping() const override {
    if (GetValue()) {
      return CDouble(1.0);
    }

    return CDouble(0.0);
  }

  // can datum be mapped to int64_t
  bool IsDatumMappableToLINT() const override { return true; }

  // map to int64_t for statistics computation
  int64_t GetLINTMapping() const override {
    if (GetValue()) {
      return int64_t(1);
    }
    return int64_t(0);
  }

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
};  // class IDatumBool

}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_IDatumBool_H

// EOF
