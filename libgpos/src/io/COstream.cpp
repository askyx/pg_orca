//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2010 Greenplum, Inc.
//
//	@filename:
//		COstream.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------

#include "gpos/io/COstream.h"

#include "gpos/common/clibwrapper.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		COstream::COstream
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstream::COstream()
    : m_static_string_buffer(m_string_format_buffer, GPOS_OSTREAM_CONVBUF_SIZE)

{}

IOstream &COstream::AppendFormat(const wchar_t *format, ...) {
  VA_LIST vl;

  VA_START(vl, format);

  m_static_string_buffer.Reset();
  m_static_string_buffer.AppendFormatVA(format, vl);

  VA_END(vl);

  (*this) << m_static_string_buffer.GetBuffer();
  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write of char with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(const char *input_char) {
  return AppendFormat(GPOS_WSZ_LIT("%s"), input_char);
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a single wchar_t with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(const wchar_t input_char) {
  return AppendFormat(GPOS_WSZ_LIT("%lc"), input_char);
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a single char with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(const char input_char) {
  return AppendFormat(GPOS_WSZ_LIT("%c"), input_char);
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a uint32_t with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(uint32_t input_ulong) {
  switch (GetStreamManipulator()) {
    case EsmDec:
      return AppendFormat(GPOS_WSZ_LIT("%u"), input_ulong);

    case EsmHex:
      return AppendFormat(GPOS_WSZ_LIT("%x"), input_ulong);

    default:
      GPOS_ASSERT(!"Unexpected stream mode");
  }

  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a uint64_t with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(uint64_t input_ullong) {
  switch (GetStreamManipulator()) {
    case EsmDec:
      return AppendFormat(GPOS_WSZ_LIT("%llu"), input_ullong);

    case EsmHex:
      return AppendFormat(GPOS_WSZ_LIT("%llx"), input_ullong);

    default:
      GPOS_ASSERT(!"Unexpected stream mode");
  }

  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write an int32_t with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(int32_t input_int) {
  switch (GetStreamManipulator()) {
    case EsmDec:
      return AppendFormat(GPOS_WSZ_LIT("%d"), input_int);

    case EsmHex:
      return AppendFormat(GPOS_WSZ_LIT("%x"), input_int);

    default:
      GPOS_ASSERT(!"Unexpected stream mode");
  }

  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a int64_t with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(int64_t input_long_int) {
  switch (GetStreamManipulator()) {
    case EsmDec:
      return AppendFormat(GPOS_WSZ_LIT("%lld"), input_long_int);

    case EsmHex:
      return AppendFormat(GPOS_WSZ_LIT("%llx"), input_long_int);

    default:
      GPOS_ASSERT(!"Unexpected stream mode");
  }

  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a double-precision floating point number
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(const double input_double) {
  if (m_fullPrecision) {
    return AppendFormat(GPOS_WSZ_LIT("%.17f"), input_double);
  }
  return AppendFormat(GPOS_WSZ_LIT("%f"), input_double);
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a pointer with conversion
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(const void *input_pointer) {
  return AppendFormat(GPOS_WSZ_LIT("%p"), input_pointer);
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		Change the stream modifier
//
//---------------------------------------------------------------------------
IOstream &COstream::operator<<(EStreamManipulator stream_manipulator) {
  m_stream_manipulator = stream_manipulator;
  return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		Return the stream modifier
//
//---------------------------------------------------------------------------
IOstream::EStreamManipulator COstream::GetStreamManipulator() const {
  return m_stream_manipulator;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		To support << std::endl
//
//---------------------------------------------------------------------------

IOstream &COstream::operator<<(WOSTREAM &(*func_ptr)(WOSTREAM &)__attribute__((unused))) {
// This extra safety check is not portable accross different C++
// standard-library implementations that may implement std::endl as a template.
// It is enabled only for GNU libstdc++, where it is known to work.
#if defined(GPOS_DEBUG) && defined(__GLIBCXX__)
  using TManip = WOSTREAM &(*)(WOSTREAM &);
  TManip tmf = func_ptr;
  GPOS_ASSERT(tmf == static_cast<TManip>(std::endl) && "Only std::endl allowed");
#endif
  (*this) << '\n';
  return *this;
}

// EOF
