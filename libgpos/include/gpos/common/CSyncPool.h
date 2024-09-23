//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncPool.h
//
//	@doc:
//		Template-based synchronized object pool class with minimum synchronization
//		overhead; it provides thread-safe object retrieval and release through
//		atomic primitives (lock-free);
//
//		Object pool is dynamically created during construction and released at
//		destruction; users retrieve objects without incurring the construction
//		cost (memory allocation, constructor invocation)
//
//		In order for the objects to be used in lock-free lists, the class uses
//		the clock algorithm to recycle objects.
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncPool_H
#define GPOS_CSyncPool_H

#include "gpos/common/CAutoP.h"
#include "gpos/task/ITask.h"
#include "gpos/types.h"
#include "gpos/utils.h"

#define BYTES_PER_ULONG (GPOS_SIZEOF(uint32_t))
#define BITS_PER_ULONG (BYTES_PER_ULONG * 8)

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CSyncPool<class T>
//
//	@doc:
//		Object pool class (not thread-safe, despite the name)
//
//---------------------------------------------------------------------------
template <class T>
class CSyncPool {
 private:
  // memory pool
  CMemoryPool *m_mp;

  // array of preallocated objects
  T *m_objects;

  // bitmap indicating object reservation
  uint32_t *m_objs_reserved;

  // bitmap indicating object recycle
  uint32_t *m_objs_recycled;

  // number of allocated objects
  uint32_t m_numobjs;

  // number of elements (uint32_t) in bitmap
  uint32_t m_bitmap_size;

  // offset of last lookup - clock index
  uintptr_t m_last_lookup_idx;

  // offset of id inside the object
  uint32_t m_id_offset;

  // atomically set bit if it is unset
  bool SetBit(uint32_t *dest, uint32_t bit_val) {
    GPOS_ASSERT(nullptr != dest);

    uint32_t old_val = *dest;

    // keep trying while the bit is unset
    while (0 == (bit_val & old_val)) {
      uint32_t new_val = bit_val | old_val;

      // attempt to set the bit
      if (*dest == old_val) {
        *dest = new_val;
        return true;
      }

      old_val = *dest;
    }

    return false;
  }

  // atomically unset bit if it is set
  bool UnsetBit(uint32_t *dest, uint32_t bit_val) {
    GPOS_ASSERT(nullptr != dest);

    uint32_t old_val = *dest;

    // keep trying while the bit is set
    while (bit_val == (bit_val & old_val)) {
      uint32_t new_val = bit_val ^ old_val;

      // attempt to set the bit
      if (*dest == old_val) {
        *dest = new_val;
        return true;
      }

      old_val = *dest;
    }

    return false;
  }

 public:
  CSyncPool(const CSyncPool &) = delete;

  // ctor
  CSyncPool(CMemoryPool *mp, uint32_t size)
      : m_mp(mp),
        m_objects(nullptr),
        m_objs_reserved(nullptr),
        m_objs_recycled(nullptr),
        m_numobjs(size),
        m_bitmap_size(size / BITS_PER_ULONG + 1),
        m_last_lookup_idx(0),
        m_id_offset(UINT32_MAX) {}

  // dtor
  ~CSyncPool() {
    if (UINT32_MAX != m_id_offset) {
      GPOS_ASSERT(nullptr != m_objects);
      GPOS_ASSERT(nullptr != m_objs_reserved);
      GPOS_ASSERT(nullptr != m_objs_recycled);

#ifdef GPOS_DEBUG
      if (!ITask::Self()->HasPendingExceptions()) {
        for (uint32_t i = 0; i < m_numobjs; i++) {
          uint32_t elem_offset = i / BITS_PER_ULONG;
          uint32_t bit_offset = i % BITS_PER_ULONG;
          uint32_t bit_val = 1 << bit_offset;

          bool reserved = (bit_val == (m_objs_reserved[elem_offset] & bit_val));
          bool recycled = (bit_val == (m_objs_recycled[elem_offset] & bit_val));

          GPOS_ASSERT((!reserved || recycled) && "Object is still in use");
        }
      }
#endif  // GPOS_DEBUG

      GPOS_DELETE_ARRAY(m_objects);
      GPOS_DELETE_ARRAY(m_objs_reserved);
      GPOS_DELETE_ARRAY(m_objs_recycled);
    }
  }

  // init function to facilitate arrays
  void Init(uint32_t id_offset) {
    GPOS_ASSERT(ALIGNED_32(id_offset));

    m_objects = GPOS_NEW_ARRAY(m_mp, T, m_numobjs);
    m_objs_reserved = GPOS_NEW_ARRAY(m_mp, uint32_t, m_bitmap_size);
    m_objs_recycled = GPOS_NEW_ARRAY(m_mp, uint32_t, m_bitmap_size);

    m_id_offset = id_offset;

    // initialize object ids
    for (uint32_t i = 0; i < m_numobjs; i++) {
      uint32_t *id = (uint32_t *)(((uint8_t *)&m_objects[i]) + m_id_offset);
      *id = i;
    }

    // initialize bitmaps
    for (uint32_t i = 0; i < m_bitmap_size; i++) {
      m_objs_reserved[i] = 0;
      m_objs_recycled[i] = 0;
    }
  }

  // find unreserved object and reserve it
  T *PtRetrieve() {
    GPOS_ASSERT(UINT32_MAX != m_id_offset && "Id offset not initialized.");

    // iterate over all objects twice (two full clock rotations);
    // objects marked as recycled cannot be reserved on the first round;
    for (uint32_t i = 0; i < 2 * m_numobjs; i++) {
      // move clock index
      uintptr_t index = (m_last_lookup_idx++) % m_numobjs;

      uint32_t elem_offset = (uint32_t)index / BITS_PER_ULONG;
      uint32_t bit_offset = (uint32_t)index % BITS_PER_ULONG;
      uint32_t bit_val = 1 << bit_offset;

      // attempt to reserve object
      if (SetBit(&m_objs_reserved[elem_offset], bit_val)) {
        // set id in corresponding object
        T *elem = &m_objects[index];

#ifdef GPOS_DEBUG
        uint32_t *id = (uint32_t *)(((uint8_t *)elem) + m_id_offset);
        GPOS_ASSERT(index == *id);
#endif  // GPOS_DEBUG

        return elem;
      }

      // object is reserved, check if it has been marked for recycling
      if (bit_val == (bit_val & m_objs_recycled[elem_offset])) {
        // attempt to unset the recycle bit
        if (UnsetBit(&m_objs_recycled[elem_offset], bit_val)) {
#ifdef GPOS_DEBUG
          bool recycled =
#endif  // GPOS_DEBUG

              // unset the reserve bit - must succeed
              UnsetBit(&m_objs_reserved[elem_offset], bit_val);

          GPOS_ASSERT(recycled && "Object was reserved before being recycled");
        }
      }
    }

    // no object is currently available, create a new one
    T *elem = GPOS_NEW(m_mp) T();
    *(uint32_t *)(((uint8_t *)elem) + m_id_offset) = UINT32_MAX;

    return elem;
  }

  // recycle reserved object
  void Recycle(T *elem) {
    GPOS_ASSERT(UINT32_MAX != m_id_offset && "Id offset not initialized.");

    uint32_t offset = *(uint32_t *)(((uint8_t *)elem) + m_id_offset);
    if (UINT32_MAX == offset) {
      // object does not belong to the array, delete it
      GPOS_DELETE(elem);
      return;
    }

    GPOS_ASSERT(offset < m_numobjs);

    uint32_t elem_offset = offset / BITS_PER_ULONG;
    uint32_t bit_offset = offset % BITS_PER_ULONG;
    uint32_t bit_val = 1 << bit_offset;

#ifdef GPOS_DEBUG
    uint32_t reserved = m_objs_reserved[elem_offset];
    GPOS_ASSERT((bit_val == (bit_val & reserved)) && "Object is not reserved");

    bool mark_recycled =
#endif  // GPOS_DEBUG
        SetBit(&m_objs_recycled[elem_offset], bit_val);

    GPOS_ASSERT(mark_recycled && "Object has already been marked for recycling");
  }

};  // class CSyncPool
}  // namespace gpos

#endif  // !GPOS_CSyncPool_H

// EOF
