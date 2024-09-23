//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CWStringConst.h
//
//	@doc:
//		Constant string class
//---------------------------------------------------------------------------
#ifndef GPOS_CWStringConst_H
#define GPOS_CWStringConst_H

#include "gpos/string/CWStringBase.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CWStringConst
//
//	@doc:
//		Constant string class.
//		The class represents constant strings, which cannot be modified after creation.
//		The class can either own its own memory, or be supplied with an external
//		memory buffer holding the string characters.
//		For a general string class that can be modified, see CWString.
//
//---------------------------------------------------------------------------
class CWStringConst : public CWStringBase {
 private:
  // null terminated wide character buffer
  const wchar_t *m_w_str_buffer;

 public:
  using CWStringBase::Equals;
  // ctors
  CWStringConst(const wchar_t *w_str_buffer);
  CWStringConst(CMemoryPool *mp, const wchar_t *w_str_buffer);
  CWStringConst(CMemoryPool *mp, const char *str_buffer);

  // shallow copy ctor
  CWStringConst(const CWStringConst &);

  // dtor
  ~CWStringConst() override;

  // returns the wide character buffer storing the string
  const wchar_t *GetBuffer() const override;

  // equality
  static bool Equals(const CWStringConst *string1, const CWStringConst *string2);

  // hash function
  static uint32_t HashValue(const CWStringConst *string);

  // checks whether the string is byte-wise equal to another string
  bool Equals(const CWStringBase *str) const override;
};
}  // namespace gpos

#endif  // #ifndef GPOS_CWStringConst_H

// EOF
