//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CIdGenerator.h
//
//	@doc:
//		Class providing methods for a uint32_t counter
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CIdGenerator_H
#define GPDXL_CIdGenerator_H

#define GPDXL_INVALID_ID UINT32_MAX

#include "gpos/base.h"

namespace gpdxl {
using namespace gpos;

class CIdGenerator {
 private:
  uint32_t id;

 public:
  explicit CIdGenerator(uint32_t);
  uint32_t next_id();
  uint32_t current_id() const;
};
}  // namespace gpdxl
#endif  // GPDXL_CIdGenerator_H

// EOF
