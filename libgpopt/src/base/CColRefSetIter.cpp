//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColRefSetIter.cpp
//
//	@doc:
//		Implementation of bitset iterator
//---------------------------------------------------------------------------

#include "gpopt/base/CColRefSetIter.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpos/base.h"
#include "gpos/common/CAutoRef.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CColRefSetIter::CColRefSetIter
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColRefSetIter::CColRefSetIter(const CColRefSet &bs) : CBitSetIter(bs) {
  // get column factory from optimizer context object
  m_pcf = COptCtxt::PoctxtFromTLS()->Pcf();
  GPOS_ASSERT(nullptr != m_pcf);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSetIter::Pcr
//
//	@doc:
//		Return colref of current position of cursor
//
//---------------------------------------------------------------------------
CColRef *CColRefSetIter::Pcr() const {
  uint32_t id = CBitSetIter::Bit();

  // resolve id through column factory
  return m_pcf->LookupColRef(id);
}

// EOF
