//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCacheEntry.h
//
//	@doc:
//		Definition of cache class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHEENTRY_H_
#define GPOS_CCACHEENTRY_H_

#include "gpos/common/CList.h"
#include "gpos/memory/CMemoryPool.h"

// setting the cache quota to 0 means unlimited
#define UNLIMITED_CACHE_QUOTA 0

// no. of hashtable buckets
#define CACHE_HT_NUM_OF_BUCKETS 1000

using namespace gpos;

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CCacheEntry
//
//	@doc:
//		Definition of cache entry;
//
//		Each cache entry is a container that holds information about one
//		cached object.
//
//---------------------------------------------------------------------------
template <class T, class K>
class CCacheEntry {
 private:
  // allocated memory pool to the cached object
  CMemoryPool *m_mp;

  // value that needs to be cached
  T m_val;

  // true if this entry is marked for deletion
  bool m_deleted;

  // gclock counter; an entry is eligible for eviction if this
  // counter drops to 0 and the entry is not pinned
  uint32_t m_g_clock_counter;

 public:
  // ctor
  CCacheEntry(CMemoryPool *mp, K key, T val, uint32_t g_clock_counter)
      : m_mp(mp), m_val(val), m_deleted(false), m_g_clock_counter(g_clock_counter), m_key(key) {
    // CCache entry has the ownership now. So ideally any time ref count can't go lesser than 1.
    // In destructor, we decrease it from 1 to 0.
    IncRefCount();
  }

  // dtor
  virtual ~CCacheEntry() {
    // Decrease ref count of m_stats_comp_val_int to get destroyed by itself if ref count is 0
    DecRefCount();
  }

  // gets the key of cached object
  K Key() const { return m_key; }

  // gets the value of cached object
  T Val() const { return m_val; }

  // gets the memory pool of cached object
  CMemoryPool *Pmp() const { return m_mp; }

  // marks entry as deleted
  void MarkForDeletion() { m_deleted = true; }

  // returns true if entry is marked as deleted
  bool IsMarkedForDeletion() const { return m_deleted; }

  // get value's ref-count
  uint32_t RefCount() const { return (uint32_t)m_val->RefCount(); }

  // increments value's ref-count
  void IncRefCount() { m_val->AddRef(); }

  // decrements value's ref-count
  void DecRefCount() { m_val->Release(); }

  // sets the gclock counter for an entry; useful for updating counter upon access
  void SetGClockCounter(uint32_t g_clock_counter) { m_g_clock_counter = g_clock_counter; }

  // decrements the gclock counter for an entry during eviction process
  void DecrementGClockCounter() { m_g_clock_counter--; }

  // returns the current value of the gclock counter
  uint32_t GetGClockCounter() { return m_g_clock_counter; }

  // the following data members are public because they
  // need to be used by GPOS_OFFSET macro for list construction

  // a pointer to entry's key
  K m_key;

  // link used to maintain entries in a hashtable
  SLink m_link_hash;

  // invalid key
  static const K m_invalid_key;

};  // CCacheEntry

}  // namespace gpos

#endif  // GPOS_CCACHEENTRY_H_

// EOF
