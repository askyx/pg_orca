//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2010 Greenplum, Inc.
//
//	@filename:
//		COstreamBasic.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------

#include "gpos/io/COstreamBasic.h"

#include "gpos/base.h"
#include "gpos/io/ioutils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::COstreamBasic
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstreamBasic::COstreamBasic(WOSTREAM *pos) : COstream(), m_ostream(pos) {
  GPOS_ASSERT(nullptr != m_ostream && "Output stream cannot be NULL");
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::operator<<
//
//	@doc:
//		wchar_t write thru;
//
//---------------------------------------------------------------------------
IOstream &COstreamBasic::operator<<(const wchar_t *wsz) {
  m_ostream = &(*m_ostream << wsz);
  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::operator<<
//
//	@doc:
//		wchar_t write thru;
//
//---------------------------------------------------------------------------
IOstream &COstreamBasic::operator<<(const wchar_t wc) {
  m_ostream = &(*m_ostream << wc);
  return *this;
}

// EOF
