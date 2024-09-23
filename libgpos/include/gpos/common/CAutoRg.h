//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoRg.h
//
//	@doc:
//		Basic auto range implementation; do not anticipate ownership based
//		on assignment to other auto ranges etc. Require explicit return/assignment
//		to re-init the object;
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoRg_H
#define GPOS_CAutoRg_H

#include "gpos/base.h"
#include "gpos/common/CStackObject.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CAutoRg
//
//	@doc:
//		Wrapper around arrays analogous to CAutoP
//
//---------------------------------------------------------------------------
template <class T>
class CAutoRg : public CStackObject {
 private:
  // actual element to point to
  T *m_object_array;

 public:
  CAutoRg(const CAutoRg &) = delete;

  // ctor
  explicit CAutoRg() : m_object_array(nullptr) {}

  // ctor
  explicit CAutoRg(T *object_array) : m_object_array(object_array) {}

  // dtor
  virtual ~CAutoRg();

  // simple assignment
  inline CAutoRg<T> const &operator=(T *object_array) {
    m_object_array = object_array;
    return *this;
  }

  // indexed access
  inline T &operator[](uint32_t ulPos) { return m_object_array[ulPos]; }

  // return basic pointer
  T *Rgt() { return m_object_array; }

  // unhook pointer from auto object
  inline T *RgtReset() {
    T *object_array = m_object_array;
    m_object_array = nullptr;
    return object_array;
  }

};  // class CAutoRg

//---------------------------------------------------------------------------
//	@function:
//		CAutoRg::~CAutoRg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
template <class T>
CAutoRg<T>::~CAutoRg() {
  GPOS_DELETE_ARRAY(m_object_array);
}
}  // namespace gpos

#endif  // !GPOS_CAutoRg_H

// EOF
