//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolTracker.h
//
//	@doc:
//		Memory pool that allocates from malloc() and adds on
//		statistics and debugging
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolTracker_H
#define GPOS_CMemoryPoolTracker_H

#include "gpos/assert.h"
#include "gpos/common/CList.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolStatistics.h"
#include "gpos/types.h"
#include "gpos/utils.h"

namespace gpos {
// memory pool with statistics and debugging support
class CMemoryPoolTracker : public CMemoryPool {
 private:
  // Defines memory block header layout for all allocations;
  // does not include the pointer to the pool;
  struct SAllocHeader {
    // pointer to pool
    CMemoryPoolTracker *m_mp;

    // total allocation size (including headers)
    uint32_t m_alloc_size;

    // user requested size
    uint32_t m_user_size;

    // sequence number
    uint64_t m_serial;

    // file name
    const char *m_filename;

    // line in file
    uint32_t m_line;

#ifdef GPOS_DEBUG
    // allocation stack
    CStackDescriptor m_stack_desc;
#endif  // GPOS_DEBUG

    // link for allocation list
    SLink m_link;
  };

  // statistics
  CMemoryPoolStatistics m_memory_pool_statistics;

  // allocation sequence number
  uint32_t m_alloc_sequence{0};

  // list of allocated (live) objects
  CList<SAllocHeader> m_allocations_list;

  // record a successful allocation
  void RecordAllocation(SAllocHeader *header);

  // record a successful free
  void RecordFree(SAllocHeader *header);

 protected:
  // dtor
  ~CMemoryPoolTracker() override;

 public:
  CMemoryPoolTracker(CMemoryPoolTracker &) = delete;

  // ctor
  CMemoryPoolTracker();

  // prepare the memory pool to be deleted
  void TearDown() override;

  // allocate memory
  void *NewImpl(const uint32_t bytes, const char *file, const uint32_t line, CMemoryPool::EAllocationType eat) override;

  // free memory allocation
  static void DeleteImpl(void *ptr, EAllocationType eat);

  // get user requested size of allocation
  static uint32_t UserSizeOfAlloc(const void *ptr);

  // return total allocated size
  uint64_t TotalAllocatedSize() const override { return m_memory_pool_statistics.TotalAllocatedSize(); }

#ifdef GPOS_DEBUG

  // check if the memory pool keeps track of live objects
  bool SupportsLiveObjectWalk() const override { return true; }

  // walk the live objects
  void WalkLiveObjects(gpos::IMemoryVisitor *visitor) override;

#endif  // GPOS_DEBUG
};
}  // namespace gpos

#endif  // !GPOS_CMemoryPoolTracker_H

// EOF
