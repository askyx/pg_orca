//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CBitVector.h
//
//	@doc:
//		Implementation of static bit vector;
//---------------------------------------------------------------------------
#ifndef GPOS_CBitVector_H
#define GPOS_CBitVector_H

#include "gpos/base.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CBitVector
//
//	@doc:
//		Bit vector based on uint64_t elements
//
//---------------------------------------------------------------------------
class CBitVector {
 private:
  // size in bits
  uint32_t m_nbits;

  // size of vector in units, not bits
  uint32_t m_len;

  // vector
  uint64_t *m_vec;

  // clear vector
  void Clear();

 public:
  CBitVector(const CBitVector &) = delete;

  // ctor
  CBitVector(CMemoryPool *mp, uint32_t cBits);

  // dtor
  ~CBitVector();

  // copy ctor with target mem pool
  CBitVector(CMemoryPool *mp, const CBitVector &);

  // determine if bit is set
  bool Get(uint32_t ulBit) const;

  // set given bit; return previous value
  bool ExchangeSet(uint32_t ulBit);

  // clear given bit; return previous value
  bool ExchangeClear(uint32_t ulBit);

  // union vectors
  void Or(const CBitVector *);

  // intersect vectors
  void And(const CBitVector *);

  // is subset
  bool ContainsAll(const CBitVector *) const;

  // is dijoint
  bool IsDisjoint(const CBitVector *) const;

  // equality
  bool Equals(const CBitVector *) const;

  // is empty?
  bool IsEmpty() const;

  // find next bit from given position
  bool GetNextSetBit(uint32_t, uint32_t &) const;

  // number of bits set
  uint32_t CountSetBits() const;

  // hash value
  uint32_t HashValue() const;

};  // class CBitVector

}  // namespace gpos

#endif  // !GPOS_CBitVector_H

// EOF
