//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CBitVector.cpp
//
//	@doc:
//		Implementation of simple, static bit vector class
//---------------------------------------------------------------------------

#include "gpos/common/CBitVector.h"

#include "gpos/base.h"
#include "gpos/common/CAutoRg.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/utils.h"

using namespace gpos;

#define BYTES_PER_UNIT GPOS_SIZEOF(uint64_t)
#define BITS_PER_UNIT (8 * BYTES_PER_UNIT)

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Clear
//
//	@doc:
//		wipe all units
//
//---------------------------------------------------------------------------
void CBitVector::Clear() {
  GPOS_ASSERT(nullptr != m_vec);
  clib::Memset(m_vec, 0, m_len * BYTES_PER_UNIT);
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::CBitVector
//
//	@doc:
//		ctor -- allocates actual vector, clears it
//
//---------------------------------------------------------------------------
CBitVector::CBitVector(CMemoryPool *mp, uint32_t nbits) : m_nbits(nbits), m_len(0), m_vec(nullptr) {
  // determine units needed to represent the number
  m_len = m_nbits / BITS_PER_UNIT;
  if (m_len * BITS_PER_UNIT < m_nbits) {
    m_len++;
  }

  GPOS_ASSERT(m_len * BITS_PER_UNIT >= m_nbits && "Bit vector sized incorrectly");

  // allocate and clear
  m_vec = GPOS_NEW_ARRAY(mp, uint64_t, m_len);

  CAutoRg<uint64_t> argull;
  argull = m_vec;

  Clear();

  // unhook from protector
  argull.RgtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::~CBitVector
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CBitVector::~CBitVector() {
  GPOS_DELETE_ARRAY(m_vec);
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::CBitVector
//
//	@doc:
//		copy ctor;
//
//---------------------------------------------------------------------------
CBitVector::CBitVector(CMemoryPool *mp, const CBitVector &bv) : m_nbits(bv.m_nbits), m_len(bv.m_len), m_vec(nullptr) {
  // deep copy
  m_vec = GPOS_NEW_ARRAY(mp, uint64_t, m_len);

  // Using auto range for cleanliness only;
  // NOTE: 03/25/2008; strictly speaking not necessary since there is
  //		no operation that could fail and it's the only allocation in the
  //		ctor;
  CAutoRg<uint64_t> argull;
  argull = m_vec;

  clib::Memcpy(m_vec, bv.m_vec, BYTES_PER_UNIT * m_len);

  // unhook from protector
  argull.RgtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Get
//
//	@doc:
//		Check if given bit is set
//
//---------------------------------------------------------------------------
bool CBitVector::Get(uint32_t pos) const {
  GPOS_ASSERT(pos < m_nbits && "Bit index out of bounds.");

  uint32_t idx = pos / BITS_PER_UNIT;
  uint64_t mask = ((uint64_t)1) << (pos % BITS_PER_UNIT);

  return m_vec[idx] & mask;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::ExchangeSet
//
//	@doc:
//		Set given bit; return previous value
//
//---------------------------------------------------------------------------
bool CBitVector::ExchangeSet(uint32_t pos) {
  GPOS_ASSERT(pos < m_nbits && "Bit index out of bounds.");

  // CONSIDER: 03/25/2008; make testing for the bit part of this routine and
  // avoid function call
  bool fSet = Get(pos);

  uint32_t idx = pos / BITS_PER_UNIT;
  uint64_t mask = ((uint64_t)1) << (pos % BITS_PER_UNIT);

  // OR the target unit with the mask
  m_vec[idx] |= mask;

  return fSet;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::ExchangeClear
//
//	@doc:
//		Clear given bit; return previous value
//
//---------------------------------------------------------------------------
bool CBitVector::ExchangeClear(uint32_t ulBit) {
  GPOS_ASSERT(ulBit < m_nbits && "Bit index out of bounds.");

  // CONSIDER: 03/25/2008; make testing for the bit part of this routine and
  // avoid function call
  bool fSet = Get(ulBit);

  uint32_t idx = ulBit / BITS_PER_UNIT;
  uint64_t mask = ((uint64_t)1) << (ulBit % BITS_PER_UNIT);

  // AND the target unit with the inverted mask
  m_vec[idx] &= ~mask;

  return fSet;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Union
//
//	@doc:
//		Union with given other vector
//
//---------------------------------------------------------------------------
void CBitVector::Or(const CBitVector *vec) {
  GPOS_ASSERT(m_nbits == vec->m_nbits && m_len == vec->m_len && "vectors must be of same size");

  // OR all components
  for (uint32_t i = 0; i < m_len; i++) {
    m_vec[i] |= vec->m_vec[i];
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Intersection
//
//	@doc:
//		Intersect with given other vector
//
//---------------------------------------------------------------------------
void CBitVector::And(const CBitVector *vec) {
  GPOS_ASSERT(m_nbits == vec->m_nbits && m_len == vec->m_len && "vectors must be of same size");

  // AND all components
  for (uint32_t i = 0; i < m_len; i++) {
    m_vec[i] &= vec->m_vec[i];
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FSubset
//
//	@doc:
//		Determine if given vector is subset
//
//---------------------------------------------------------------------------
bool CBitVector::ContainsAll(const CBitVector *vec) const {
  GPOS_ASSERT(m_nbits == vec->m_nbits && m_len == vec->m_len && "vectors must be of same size");

  // OR all components
  for (uint32_t i = 0; i < m_len; i++) {
    uint64_t ull = m_vec[i] & vec->m_vec[i];
    if (ull != vec->m_vec[i]) {
      return false;
    }
  }

  return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FDisjoint
//
//	@doc:
//		Determine if given vector is disjoint
//
//---------------------------------------------------------------------------
bool CBitVector::IsDisjoint(const CBitVector *vec) const {
  GPOS_ASSERT(m_nbits == vec->m_nbits && m_len == vec->m_len && "vectors must be of same size");

  for (uint32_t i = 0; i < m_len; i++) {
    if (0 != (m_vec[i] & vec->m_vec[i])) {
      return false;
    }
  }

  return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Equals
//
//	@doc:
//		Determine if equal
//
//---------------------------------------------------------------------------
bool CBitVector::Equals(const CBitVector *vec) const {
  GPOS_ASSERT(m_nbits == vec->m_nbits && m_len == vec->m_len && "vectors must be of same size");

  // compare all components
  if (0 == clib::Memcmp(m_vec, vec->m_vec, m_len * BYTES_PER_UNIT)) {
    GPOS_ASSERT(this->ContainsAll(vec) && vec->ContainsAll(this));
    return true;
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::IsEmpty
//
//	@doc:
//		Determine if vector is empty
//
//---------------------------------------------------------------------------
bool CBitVector::IsEmpty() const {
  for (uint32_t i = 0; i < m_len; i++) {
    if (0 != m_vec[i]) {
      return false;
    }
  }

  return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::GetNextBit
//
//	@doc:
//		Determine the next bit set greater or equal than the provided position
//
//---------------------------------------------------------------------------
bool CBitVector::GetNextSetBit(uint32_t start_pos, uint32_t &next_pos) const {
  uint32_t offset = start_pos % BITS_PER_UNIT;
  for (uint32_t idx = start_pos / BITS_PER_UNIT; idx < m_len; idx++) {
    uint64_t ull = m_vec[idx] >> offset;

    uint32_t bit = offset;
    while (0 != ull && 0 == (ull & (uint64_t)1)) {
      ull >>= 1;
      bit++;
    }

    // if any bits left we found the next set position
    if (0 != ull) {
      next_pos = bit + (idx * BITS_PER_UNIT);
      return true;
    }

    // the initial offset applies only to the first chunk
    offset = 0;
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::CElements
//
//	@doc:
//		Count bits in vector
//
//---------------------------------------------------------------------------
uint32_t CBitVector::CountSetBits() const {
  uint32_t nbits = 0;
  for (uint32_t i = 0; i < m_len; i++) {
    uint64_t ull = m_vec[i];
    uint32_t j = 0;

    for (j = 0; ull != 0; j++) {
      ull &= (ull - 1);
    }

    nbits += j;
  }

  return nbits;
}

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::HashValue
//
//	@doc:
//		Compute hash value for bit vector
//
//---------------------------------------------------------------------------
uint32_t CBitVector::HashValue() const {
  return gpos::HashByteArray((uint8_t *)&m_vec[0], GPOS_SIZEOF(m_vec[0]) * m_len);
}

// EOF
