//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CBitSetIter.h
//
//	@doc:
//		Implementation of iterator for bitset
//---------------------------------------------------------------------------
#ifndef GPOS_CBitSetIter_H
#define GPOS_CBitSetIter_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CBitSetIter
//
//	@doc:
//		Iterator for bitset's; defined as friend, ie can access bitset's
//		internal links
//
//---------------------------------------------------------------------------
class CBitSetIter {
 private:
  // bitset
  const CBitSet &m_bs;

  // current cursor position (in current link)
  uint32_t m_cursor;

  // current cursor link
  CBitSet::CBitSetLink *m_bsl;

  // is iterator active or exhausted
  bool m_active;

 public:
  CBitSetIter(const CBitSetIter &) = delete;

  // ctor
  explicit CBitSetIter(const CBitSet &bs);
  // dtor
  ~CBitSetIter() = default;

  // short hand for cast
  operator bool() const { return m_active; }

  // move to next bit
  bool Advance();

  // current bit
  uint32_t Bit() const;

};  // class CBitSetIter
}  // namespace gpos

#endif  // !GPOS_CBitSetIter_H

// EOF
