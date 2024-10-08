//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CWStringBase.h
//
//	@doc:
//		Abstract wide character string class
//---------------------------------------------------------------------------
#ifndef GPOS_CWStringBase_H
#define GPOS_CWStringBase_H

#include "gpos/common/clibwrapper.h"
#include "gpos/types.h"

// #define GPOS_WSZ_LENGTH(x) gpos::clib::Wcslen(x)
#define GPOS_WSZ_LENGTH(x) wcslen(x)
#define GPOS_WSZ_STR_LENGTH(x) GPOS_WSZ_LIT(x), GPOS_WSZ_LENGTH(GPOS_WSZ_LIT(x))
#define GPOS_SZ_LENGTH(x) gpos::clib::Strlen(x)

#define WCHAR_EOS GPOS_WSZ_LIT('\0')

namespace gpos {
class CWStringConst;
class CMemoryPool;

//---------------------------------------------------------------------------
//	@class:
//		CWStringBase
//
//	@doc:
//		Abstract wide character string class.
//		Currently, the class has two derived classes: CWStringConst and CWString.
//		CWString is used to represent constant strings that once initialized are never
//		modified. This class is not responsible for any memory management, rather
//		its users are in charge for allocating and releasing the necessary memory.
//		In contrast, CWString can be used to store strings that are modified after
//		they are created. CWString is in charge of dynamically allocating and deallocating
//		memory for storing the characters of the string.
//
//---------------------------------------------------------------------------
class CWStringBase {
 private:
 protected:
  // represents end-of-wide-string character
  static const wchar_t m_empty_wcstr;

  // size of the string in number of wchar_t units (not counting the terminating '\0')
  uint32_t m_length;

  // whether string owns its memory and should take care of deallocating it at destruction time
  bool m_owns_memory;

 public:
  CWStringBase(const CWStringBase &) = delete;

  // ctor
  CWStringBase(uint32_t length, bool owns_memory) : m_length(length), m_owns_memory(owns_memory) {}

  // dtor
  virtual ~CWStringBase() = default;

  // deep copy of the string
  virtual CWStringConst *Copy(CMemoryPool *mp) const;

  // accessors
  virtual uint32_t Length() const;

  // checks whether the string is byte-wise equal to another string
  virtual bool Equals(const CWStringBase *str) const;

  // checks whether the string is equal to a string literal
  virtual bool Equals(const wchar_t *str) const;

  // checks whether the string contains any characters
  virtual bool IsEmpty() const;

  // checks whether a string is properly null-terminated
  bool IsValid() const;

  // equality operator
  bool operator==(const CWStringBase &str) const;

  // returns the wide character buffer storing the string
  virtual const wchar_t *GetBuffer() const = 0;

  // returns the index of the first occurrence of a character, -1 if not found
  int32_t Find(wchar_t wc) const;

  // checks if a character is escaped
  bool HasEscapedCharAt(uint32_t offset) const;

  // count how many times the character appears in string
  uint32_t CountOccurrencesOf(const wchar_t wc) const;

  static int32_t Compare(const void *left, const void *right) {
    CWStringBase *leftstr = *(CWStringBase **)left;
    CWStringBase *rightstr = *(CWStringBase **)right;
    return wcscmp(leftstr->GetBuffer(), rightstr->GetBuffer());
  }
};

}  // namespace gpos

#endif  // !GPOS_CWStringBase_H

// EOF
