//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryVisitorPrint.h
//
//	@doc:
//		Memory object visitor that prints debug information for all allocated
//		objects inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryVisitorPrint_H
#define GPOS_CMemoryVisitorPrint_H

#include "gpos/assert.h"
#include "gpos/memory/IMemoryVisitor.h"
#include "gpos/types.h"
#include "gpos/utils.h"

namespace gpos {
// specialization of memory object visitor that prints out
// the debugging information to a stream
class CMemoryVisitorPrint : public IMemoryVisitor {
 private:
  // call counter for the visit function
  uint64_t m_visits;

  // stream used for writing debug information
  IOstream &m_os;

 public:
  CMemoryVisitorPrint(CMemoryVisitorPrint &) = delete;

  // ctor
  CMemoryVisitorPrint(IOstream &os);

  // dtor
  ~CMemoryVisitorPrint() override;

  // output information about a memory allocation
  void Visit(void *user_addr, size_t user_size, void *total_addr, size_t total_size, const char *alloc_filename,
             const uint32_t alloc_line, uint64_t alloc_seq_number, CStackDescriptor *stack_desc) override;

  // visit counter accessor
  uint64_t GetNumVisits() const { return m_visits; }
};
}  // namespace gpos

#endif  // GPOS_CMemoryVisitorPrint_H

// EOF
