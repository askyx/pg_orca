//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CWStringBase.cpp
//
//	@doc:
//		Implementation of the base abstract wide character string class
//---------------------------------------------------------------------------

#include "gpos/string/CWStringBase.h"

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/string/CWStringConst.h"

using namespace gpos;

const wchar_t CWStringBase::m_empty_wcstr = GPOS_WSZ_LIT('\0');

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Copy
//
//	@doc:
//		Creates a deep copy of the string
//
//---------------------------------------------------------------------------
CWStringConst *CWStringBase::Copy(CMemoryPool *mp) const {
  return GPOS_NEW(mp) CWStringConst(mp, GetBuffer());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::IsValid
//
//	@doc:
//		Checks if the string is properly NULL-terminated
//
//---------------------------------------------------------------------------
bool CWStringBase::IsValid() const {
  return (Length() == GPOS_WSZ_LENGTH(GetBuffer()));
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::operator ==
//
//	@doc:
//		Equality operator on strings
//
//---------------------------------------------------------------------------
bool CWStringBase::operator==(const CWStringBase &str) const {
  return Equals(&str);
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Length()
//
//	@doc:
//		Returns the length of the string in number of wide characters,
//		not counting the terminating '\0'
//
//---------------------------------------------------------------------------
uint32_t CWStringBase::Length() const {
  return m_length;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Equals
//
//	@doc:
//		Checks whether the string is byte-wise equal to another string
//
//---------------------------------------------------------------------------
bool CWStringBase::Equals(const CWStringBase *str) const {
  GPOS_ASSERT(nullptr != str);
  return Length() == str->Length() && 0 == clib::Wcsncmp(GetBuffer(), str->GetBuffer(), Length());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Equals
//
//	@doc:
//		Checks whether the string is equal to a string literal
//
//---------------------------------------------------------------------------
bool CWStringBase::Equals(const wchar_t *str) const {
  GPOS_ASSERT(nullptr != str);
  return Length() == GPOS_WSZ_LENGTH(str) && 0 == clib::Wcsncmp(GetBuffer(), str, Length());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::IsEmpty
//
//	@doc:
//		Checks whether the string is empty
//
//---------------------------------------------------------------------------
bool CWStringBase::IsEmpty() const {
  return (0 == Length());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Find
//
//	@doc:
//		Returns the index of the first occurrence of a character, -1 if not found
//
//---------------------------------------------------------------------------
int32_t CWStringBase::Find(wchar_t wc) const {
  const wchar_t *w_str = GetBuffer();
  const uint32_t length = Length();

  for (uint32_t i = 0; i < length; i++) {
    if (wc == w_str[i]) {
      return i;
    }
  }

  return -1;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::HasEscapedCharAt
//
//	@doc:
//		Checks if a character is escaped
//
//---------------------------------------------------------------------------
bool CWStringBase::HasEscapedCharAt(uint32_t offset) const {
  GPOS_ASSERT(!IsEmpty());
  GPOS_ASSERT(Length() > offset);

  const wchar_t *w_str_buffer = GetBuffer();

  for (uint32_t i = offset; i > 0; i--) {
    // check for escape character
    if (GPOS_WSZ_LIT('\\') != w_str_buffer[i - 1]) {
      if (0 == ((offset - i) & uint32_t(1))) {
        return false;
      } else {
        return true;
      }
    }
  }

  // reached beginning of string
  if (0 == (offset & uint32_t(1))) {
    return false;
  } else {
    return true;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::CountOccurrencesOf
//
//	@doc:
//		Count how many times the character appears in string
//
//---------------------------------------------------------------------------
uint32_t CWStringBase::CountOccurrencesOf(const wchar_t wc) const {
  uint32_t occurrences = 0;
  uint32_t length = Length();
  const wchar_t *buf = GetBuffer();

  for (uint32_t i = 0; i < length; i++) {
    if (wc == buf[i] && !HasEscapedCharAt(i)) {
      occurrences++;
    }
  }
  return occurrences;
}
// EOF
