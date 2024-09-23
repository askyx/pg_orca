//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CStringStatic.h
//
//	@doc:
//		ASCII-character String class with buffer
//---------------------------------------------------------------------------
#ifndef GPOS_CStringStatic_H
#define GPOS_CStringStatic_H

#include "gpos/attributes.h"
#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"

// use this character to substitute non-ASCII wide characters
#define GPOS_WCHAR_UNPRINTABLE '.'

// end-of-string character
#define CHAR_EOS '\0'

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CStringStatic
//
//	@doc:
//		ASCII-character string interface with buffer pre-allocation.
//		Internally, the class uses a null-terminated char buffer to store the string
//		characters.	The buffer is assigned at construction time; its capacity cannot be
//		modified, thus restricting the maximum size of the stored string. Attempting to
//		store a larger string than the available buffer capacity results in truncation.
//		CStringStatic owner is responsible for allocating the buffer and releasing it
//		after the object is destroyed.
//
//---------------------------------------------------------------------------
class CStringStatic {
 private:
  // null-terminated wide character buffer
  char *m_buffer;

  // size of the string in number of char units,
  // not counting the terminating '\0'
  uint32_t m_length;

  // buffer capacity
  uint32_t m_capacity;

#ifdef GPOS_DEBUG
  // checks whether a string is properly null-terminated
  bool IsValid() const;
#endif  // GPOS_DEBUG

 public:
  CStringStatic(const CStringStatic &) = delete;

  // ctor
  CStringStatic(char buffer[], uint32_t capacity);

  // ctor with string initialization
  CStringStatic(char buffer[], uint32_t capacity, const char init_str[]);

  // dtor - owner is responsible for releasing the buffer
  ~CStringStatic() = default;

  // returns the wide character buffer storing the string
  const char *Buffer() const { return m_buffer; }

  // returns the string length
  uint32_t Length() const { return m_length; }

  // checks whether the string contains any characters
  bool IsEmpty() const { return (0 == m_length); }

  // checks whether the string is byte-wise equal to a given string literal
  bool Equals(const char *buf) const;

  // appends a string
  void Append(const CStringStatic *str);

  // appends the contents of a buffer to the current string
  void AppendBuffer(const char *buf);

  // appends a formatted string
  void AppendFormat(const char *format, ...) GPOS_ATTRIBUTE_PRINTF(2, 3);

  // appends a formatted string based on passed va list
  void AppendFormatVA(const char *format, VA_LIST va_args) GPOS_ATTRIBUTE_PRINTF(2, 0);

  // appends wide character string
  void AppendConvert(const wchar_t *wc_str);

  // resets string
  void Reset();
};
}  // namespace gpos

#endif  // !GPOS_CStringStatic_H

// EOF
