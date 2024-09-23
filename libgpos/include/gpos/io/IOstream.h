//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IOstream.h
//
//	@doc:
//		Output stream interface;
//---------------------------------------------------------------------------
#ifndef GPOS_IOstream_H
#define GPOS_IOstream_H

#include "gpos/types.h"

namespace gpos {
// wide char ostream
using WOSTREAM = std::basic_ostream<wchar_t, std::char_traits<wchar_t>>;

//---------------------------------------------------------------------------
//	@class:
//		IOstream
//
//	@doc:
//		Defines all available operator interfaces; avoids having to overload
//		system stream classes or their operators/member functions
//
//---------------------------------------------------------------------------
class IOstream {
 protected:
  // ctor
  IOstream() = default;

  bool m_fullPrecision{false};

 public:
  enum EStreamManipulator {
    EsmDec,
    EsmHex
    // no sentinel to enforce strict switch-ing
  };

  // virtual dtor
  virtual ~IOstream() = default;

  // operator interface
  virtual IOstream &operator<<(const char *) = 0;
  virtual IOstream &operator<<(const wchar_t) = 0;
  virtual IOstream &operator<<(const char) = 0;
  virtual IOstream &operator<<(uint32_t) = 0;
  virtual IOstream &operator<<(uint64_t) = 0;
  virtual IOstream &operator<<(int32_t) = 0;
  virtual IOstream &operator<<(int64_t) = 0;
  virtual IOstream &operator<<(double) = 0;
  virtual IOstream &operator<<(const void *) = 0;
  virtual IOstream &operator<<(WOSTREAM &(*)(WOSTREAM &)) = 0;
  virtual IOstream &operator<<(EStreamManipulator) = 0;

  // needs to be implemented by subclass
  virtual IOstream &operator<<(const wchar_t *) = 0;

  void SetFullPrecision(bool fullPrecision) { m_fullPrecision = fullPrecision; }
};

}  // namespace gpos

#endif  // !GPOS_IOstream_H

// EOF
