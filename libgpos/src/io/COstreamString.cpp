//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2010 Greenplum Inc.
//
//	@filename:
//		COstreamString.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------

#include "gpos/io/COstreamString.h"

#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::COstreamString
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstreamString::COstreamString(CWString *pws) : COstream(), m_string(pws) {
  GPOS_ASSERT(m_string && "Backing string cannot be NULL");
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		wchar_t array write thru;
//
//---------------------------------------------------------------------------
IOstream &COstreamString::operator<<(const wchar_t *wc_array) {
  m_string->AppendWideCharArray(wc_array);

  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		char array write thru;
//
//---------------------------------------------------------------------------
IOstream &COstreamString::operator<<(const char *c) {
  m_string->AppendCharArray(c);

  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		wchar_t write thru;
//
//---------------------------------------------------------------------------
IOstream &COstreamString::operator<<(const wchar_t wc) {
  wchar_t wc_array[2];
  wc_array[0] = wc;
  wc_array[1] = L'\0';
  m_string->AppendWideCharArray(wc_array);

  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		char write thru;
//
//---------------------------------------------------------------------------
IOstream &COstreamString::operator<<(const char c) {
  char char_array[2];
  char_array[0] = c;
  char_array[1] = '\0';
  m_string->AppendCharArray(char_array);

  return *this;
}

// EOF
