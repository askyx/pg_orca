//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates
//
//	Hash set iterator

#ifndef GPOS_CHashSetIter_H
#define GPOS_CHashSetIter_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashSet.h"
#include "gpos/common/CStackObject.h"

namespace gpos {
// Hash set iterator
template <class T, uint32_t (*HashFn)(const T *), bool (*EqFn)(const T *, const T *), void (*CleanupFn)(T *)>
class CHashSetIter : public CStackObject {
  // short hand for hashset type
  using TSet = CHashSet<T, HashFn, EqFn, CleanupFn>;

 private:
  // set to iterate
  const TSet *m_set;

  // current hashchain
  uint32_t m_chain_idx;

  // current element
  uint32_t m_elem_idx;

  // is initialized?
  bool m_is_initialized;

 public:
  CHashSetIter(const CHashSetIter<T, HashFn, EqFn, CleanupFn> &) = delete;

  // ctor
  CHashSetIter(TSet *set) : m_set(set), m_chain_idx(0), m_elem_idx(0) { GPOS_ASSERT(nullptr != set); }

  // dtor
  virtual ~CHashSetIter() = default;

  // advance iterator to next element
  bool Advance() {
    if (m_elem_idx < m_set->m_elements->Size()) {
      m_elem_idx++;
      return true;
    }

    return false;
  }

  // current element
  const T *Get() const {
    const typename TSet::CHashSetElem *elem = nullptr;
    T *t = (*(m_set->m_elements))[m_elem_idx - 1];
    elem = m_set->Lookup(t);
    if (nullptr != elem) {
      return elem->Value();
    }
    return nullptr;
  }

};  // class CHashSetIter

}  // namespace gpos

#endif  // !GPOS_CHashSetIter_H

// EOF
