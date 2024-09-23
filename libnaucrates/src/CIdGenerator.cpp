//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CIdGenerator.cpp
//
//	@doc:
//		Implementing the uint32_t Counter
//---------------------------------------------------------------------------

#include "naucrates/dxl/CIdGenerator.h"

#include "gpos/base.h"

using namespace gpdxl;
using namespace gpos;

CIdGenerator::CIdGenerator(uint32_t start_id) : id(start_id) {}

//---------------------------------------------------------------------------
//	@function:
//		CIdGenerator::next_id
//
//	@doc:
//		Returns the next unique id
//
//---------------------------------------------------------------------------
uint32_t CIdGenerator::next_id() {
  return id++;
}

//---------------------------------------------------------------------------
//	@function:
//		CIdGenerator::current_id
//
//	@doc:
//		Returns the current unique id used
//
//---------------------------------------------------------------------------
uint32_t CIdGenerator::current_id() const {
  return id;
}

// EOF
