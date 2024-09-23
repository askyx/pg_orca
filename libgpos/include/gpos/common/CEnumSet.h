//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CEnumSet.h
//
//	@doc:
//		Implementation of set of enums as bitset
//---------------------------------------------------------------------------
#ifndef GPOS_CEnumSet_H
#define GPOS_CEnumSet_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CEnumSet
//
//	@doc:
//		Template derived from CBitSet
//
//---------------------------------------------------------------------------
template <class T, uint32_t sentinel_index>
class CEnumSet : public CBitSet {
 private:
 public:
  CEnumSet(const CEnumSet<T, sentinel_index> &) = delete;

  // ctor
  explicit CEnumSet(CMemoryPool *mp) : CBitSet(mp, sentinel_index) {}

  explicit CEnumSet(CMemoryPool *mp, const CEnumSet<T, sentinel_index> &enum_set) : CBitSet(mp, enum_set) {}

  // dtor
  ~CEnumSet() override = default;

  // determine if bit is set
  bool Get(T t) const {
    GPOS_ASSERT(t >= 0);

    uint32_t bit_index = static_cast<uint32_t>(t);
    GPOS_ASSERT(bit_index < sentinel_index && "Out of range of enum");

    return CBitSet::Get(bit_index);
  }

  // set given bit; return previous value
  bool ExchangeSet(T t) {
    GPOS_ASSERT(t >= 0);

    uint32_t bit_index = static_cast<uint32_t>(t);
    GPOS_ASSERT(bit_index < sentinel_index && "Out of range of enum");

    return CBitSet::ExchangeSet(bit_index);
  }

  // clear given bit; return previous value
  bool ExchangeClear(T t) {
    GPOS_ASSERT(t >= 0);

    uint32_t bit_index = static_cast<uint32_t>(t);
    GPOS_ASSERT(bit_index < sentinel_index && "Out of range of enum");

    return CBitSet::ExchangeClear(bit_index);
  }

};  // class CEnumSet
}  // namespace gpos

#endif  // !GPOS_CEnumSet_H

// EOF
