//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum Inc.
//
//	@filename:
//		COstreamString.h
//
//	@doc:
//		Output string stream class;
//---------------------------------------------------------------------------
#ifndef GPOS_COstreamString_H
#define GPOS_COstreamString_H

#include "gpos/io/COstream.h"
#include "gpos/string/CWString.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		COstreamString
//
//	@doc:
//		Implements an output stream writing to a string
//
//---------------------------------------------------------------------------
class COstreamString : public COstream {
 private:
  // underlying string
  CWString *m_string;

 public:
  COstreamString(const COstreamString &) = delete;

  // please see comments in COstream.h for an explanation
  using COstream::operator<<;

  // ctor
  explicit COstreamString(CWString *);

  ~COstreamString() override = default;

  // implement << operator on wide char array
  IOstream &operator<<(const wchar_t *wc_array) override;

  // implement << operator on char array
  IOstream &operator<<(const char *c_array) override;

  // implement << operator on wide char
  IOstream &operator<<(const wchar_t wc) override;

  // implement << operator on char
  IOstream &operator<<(const char c) override;
};

}  // namespace gpos

#endif  // !GPOS_COstreamString_H

// EOF
