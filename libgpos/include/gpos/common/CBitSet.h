//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CBitSet.h
//
//	@doc:
//		Implementation of bitset as linked list of bitvectors
//---------------------------------------------------------------------------
#ifndef GPOS_CBitSet_H
#define GPOS_CBitSet_H

#include "gpos/base.h"
#include "gpos/common/CBitVector.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CList.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CBitSet
//
//	@doc:
//		Linked list of CBitSetLink's
//
//---------------------------------------------------------------------------
class CBitSet : public CRefCount {
  // bitset iter needs to access internals
  friend class CBitSetIter;

 protected:
  //---------------------------------------------------------------------------
  //	@class:
  //		CBitSetLink
  //
  //	@doc:
  //		bit vector + offset + link
  //
  //---------------------------------------------------------------------------
  class CBitSetLink {
   private:
    // offset
    uint32_t m_offset;

    // bitvector
    CBitVector *m_vec;

   public:
    CBitSetLink(const CBitSetLink &) = delete;

    // ctor
    explicit CBitSetLink(CMemoryPool *, uint32_t offset, uint32_t vector_size);

    explicit CBitSetLink(CMemoryPool *, const CBitSetLink &);

    // dtor
    ~CBitSetLink();

    // accessor
    uint32_t GetOffset() const { return m_offset; }

    // accessor
    CBitVector *GetVec() const { return m_vec; }

    // list link
    SLink m_link;

  };  // class CBitSetLink

  // list of bit set links
  CList<CBitSetLink> m_bsllist;

  // pool to allocate links from
  CMemoryPool *m_mp;

  // size of individual bitvectors
  uint32_t m_vector_size;

  // number of elements
  uint32_t m_size;

  // private copy ctor
  CBitSet(const CBitSet &);

  // find link with offset less or equal to given value
  CBitSetLink *FindLinkByOffset(uint32_t, CBitSetLink * = nullptr) const;

  // reset set
  void Clear();

  // compute target offset
  uint32_t ComputeOffset(uint32_t) const;

  // re-compute size of set
  void RecomputeSize();

 public:
  // ctor
  CBitSet(CMemoryPool *mp, uint32_t vector_size = 256);
  CBitSet(CMemoryPool *mp, const CBitSet &);

  // dtor
  ~CBitSet() override;

  // determine if bit is set
  bool Get(uint32_t pos) const;

  // set given bit; return previous value
  bool ExchangeSet(uint32_t pos);

  // clear given bit; return previous value
  bool ExchangeClear(uint32_t pos);

  // union sets
  void Union(const CBitSet *);

  // intersect sets
  void Intersection(const CBitSet *);

  // difference of sets
  void Difference(const CBitSet *);

  // is subset
  bool ContainsAll(const CBitSet *) const;

  // equality
  bool Equals(const CBitSet *) const;

  // disjoint
  bool IsDisjoint(const CBitSet *) const;

  // hash value for set
  uint32_t HashValue() const;

  // number of elements
  uint32_t Size() const { return m_size; }

  // print function
  virtual IOstream &OsPrint(IOstream &os) const;

};  // class CBitSet

// shorthand for printing
inline IOstream &operator<<(IOstream &os, CBitSet &bs) {
  return bs.OsPrint(os);
}
}  // namespace gpos

#endif  // !GPOS_CBitSet_H

// EOF
