//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolStatistics.h
//
//	@doc:
//		Statistics for memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolStatistics_H
#define GPOS_CMemoryPoolStatistics_H

#include "gpos/types.h"

namespace gpos {
// Statistics for a memory pool
class CMemoryPoolStatistics {
 private:
  uint64_t m_num_successful_allocations{0};

  uint64_t m_num_failed_allocations{0};

  uint64_t m_num_free{0};

  uint64_t m_num_live_obj{0};

  uint64_t m_live_obj_user_size{0};

  uint64_t m_live_obj_total_size{0};

 public:
  CMemoryPoolStatistics(CMemoryPoolStatistics &) = delete;

  // ctor
  CMemoryPoolStatistics() = default;

  // dtor
  virtual ~CMemoryPoolStatistics() = default;

  // get the total number of successful allocation calls
  uint64_t GetNumSuccessfulAllocations() const { return m_num_successful_allocations; }

  // get the total number of failed allocation calls
  uint64_t GetNumFailedAllocations() const { return m_num_failed_allocations; }

  // get the total number of free calls
  uint64_t GetNumFree() const { return m_num_free; }

  // get the number of live objects
  uint64_t GetNumLiveObj() const { return m_num_live_obj; }

  // get the user data size of live objects
  uint64_t LiveObjUserSize() const { return m_live_obj_user_size; }

  // get the total data size (user + header padding) of live objects;
  // not accounting for memory used by the underlying allocator for its header;
  uint64_t LiveObjTotalSize() const { return m_live_obj_total_size; }

  // record a successful allocation
  void RecordAllocation(uint32_t user_data_size, uint32_t total_data_size) {
    ++m_num_successful_allocations;
    ++m_num_live_obj;
    m_live_obj_user_size += user_data_size;
    m_live_obj_total_size += total_data_size;
  }

  // record a successful free call (of a valid, non-NULL pointer)
  void RecordFree(uint32_t user_data_size, uint32_t total_data_size) {
    ++m_num_free;
    --m_num_live_obj;
    m_live_obj_user_size -= user_data_size;
    m_live_obj_total_size -= total_data_size;
  }

  // record a failed allocation attempt
  void RecordFailedAllocation() { ++m_num_failed_allocations; }

  // return total allocated size
  virtual uint64_t TotalAllocatedSize() const { return m_live_obj_total_size; }

};  // class CMemoryPoolStatistics
}  // namespace gpos

#endif  // ! CMemoryPoolStatistics

// EOF
