//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IDatum.h
//
//	@doc:
//		Base class for datum representation inside optimizer
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatum_H
#define GPNAUCRATES_IDatum_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CHashMap.h"
#include "gpos/string/CWStringConst.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

namespace gpnaucrates {
using namespace gpos;
using namespace gpmd;

class IDatum;

// hash map mapping uint32_t -> Datum
using UlongToIDatumMap = CHashMap<uint32_t, IDatum, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                  CleanupDelete<uint32_t>, CleanupRelease<IDatum>>;

//---------------------------------------------------------------------------
//	@class:
//		IDatum
//
//	@doc:
//		Base abstract class for datum representation inside optimizer
//
//---------------------------------------------------------------------------
class IDatum : public CRefCount {
 private:
 public:
  IDatum(const IDatum &) = delete;

  // ctor
  IDatum() = default;

  // dtor
  ~IDatum() override = default;

  // accessor for datum type
  virtual IMDType::ETypeInfo GetDatumType() = 0;

  // accessor of metadata id
  virtual IMDId *MDId() const = 0;

  virtual int32_t TypeModifier() const { return default_type_modifier; }

  // accessor of size
  virtual uint32_t Size() const = 0;

  // is datum null?
  virtual bool IsNull() const = 0;

  // return string representation
  virtual const CWStringConst *GetStrRepr(CMemoryPool *mp) const = 0;

  // hash function
  virtual uint32_t HashValue() const = 0;

  // Match function on datums
  virtual bool Matches(const IDatum *) const = 0;

  // create a copy of the datum
  virtual IDatum *MakeCopy(CMemoryPool *mp) const = 0;

  // stats greater than
  virtual bool StatsAreGreaterThan(const IDatum *datum) const {
    bool stats_are_comparable = datum->StatsAreComparable(this);
    GPOS_ASSERT(stats_are_comparable && "Invalid invocation of StatsAreGreaterThan");
    return stats_are_comparable && datum->StatsAreLessThan(this);
  }

  // does the datum need to be padded before statistical derivation
  virtual bool NeedsPadding() const = 0;

  // return the padded datum
  virtual IDatum *MakePaddedDatum(CMemoryPool *mp, uint32_t col_len) const = 0;

  // does datum support like predicate
  virtual bool SupportsLikePredicate() const = 0;

  // return the default scale factor of like predicate
  virtual CDouble GetLikePredicateScaleFactor() const = 0;

  // byte array for char/varchar columns
  virtual const uint8_t *GetByteArrayValue() const = 0;

  // is datum mappable to a base type for statistics purposes
  virtual bool StatsMappable() { return this->StatsAreComparable(this); }

  // can datum be mapped to a double
  virtual bool IsDatumMappableToDouble() const = 0;

  // map to double for statistics computation
  virtual CDouble GetDoubleMapping() const = 0;

  // can datum be mapped to int64_t
  virtual bool IsDatumMappableToLINT() const = 0;

  // map to int64_t for statistics computation
  virtual int64_t GetLINTMapping() const = 0;

  // equality based on mapping to int64_t or CDouble
  virtual bool StatsAreEqual(const IDatum *datum) const;

  // stats less than
  virtual bool StatsAreLessThan(const IDatum *datum) const;

  // distance function
  virtual CDouble GetStatsDistanceFrom(const IDatum *datum) const;

  // return double representation of mapping value
  CDouble GetValAsDouble() const;

  // check if the given pair of datums are stats comparable
  virtual bool StatsAreComparable(const IDatum *datum) const;

  virtual gpos::IOstream &OsPrint(gpos::IOstream &os) const = 0;

  bool operator==(const IDatum &other) const {
    if (this == &other) {
      // same object reference
      return true;
    }

    return Matches(&other);
  }
};  // class IDatum

// array of idatums
using IDatumArray = CDynamicPtrArray<IDatum, CleanupRelease>;
}  // namespace gpnaucrates

#endif  // !GPNAUCRATES_IDatum_H

// EOF
