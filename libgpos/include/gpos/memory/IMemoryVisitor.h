//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMemoryVisitor.h
//
//	@doc:
//      Interface for applying a common operation to all allocated objects
//		inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_IMemoryVisitor_H
#define GPOS_IMemoryVisitor_H

#include "gpos/assert.h"
#include "gpos/types.h"

namespace gpos {
// prototypes
class CStackDescriptor;

// wrapper for common operation on allocated memory;
// called by memory pools when a walk of the memory is requested;
class IMemoryVisitor {
 private:
 public:
  IMemoryVisitor(IMemoryVisitor &) = delete;

  // ctor
  IMemoryVisitor() = default;

  // dtor
  virtual ~IMemoryVisitor() = default;

  // executed operation during a walk of objects;
  // file name may be NULL (when debugging is not enabled);
  // line number will be zero in that case;
  // sequence number is a constant in case allocation sequencing is not supported;
  virtual void Visit(void *user_addr, size_t user_size, void *total_addr, size_t total_size, const char *alloc_filename,
                     const uint32_t alloc_line, uint64_t alloc_seq_number, CStackDescriptor *desc) = 0;
};
}  // namespace gpos

#endif  // GPOS_IMemoryVisitor_H

// EOF
