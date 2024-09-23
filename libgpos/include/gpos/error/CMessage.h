//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMessage.h
//
//	@doc:
//		Error message container; each instance corresponds to a message as
//		loaded from an external configuration file;
//		Both warnings and errors;
//---------------------------------------------------------------------------
#ifndef GPOS_CMessage_H
#define GPOS_CMessage_H

#include "gpos/assert.h"
#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/types.h"

#define GPOS_WSZ_WSZLEN(x) (L##x), (gpos::clib::Wcslen(L##x))

#define GPOS_ERRMSG_FORMAT(...) gpos::CMessage::FormatMessage(__VA_ARGS__)

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CMessage
//
//	@doc:
//		Corresponds to individual message as defined in config file
//
//---------------------------------------------------------------------------
class CMessage {
 private:
  // severity
  uint32_t m_severity;

  // format string
  const wchar_t *m_fmt;

  // length of format string
  uint32_t m_fmt_len;

  // number of parameters
  uint32_t m_num_params;

  // comment string
  const wchar_t *m_comment;

  // length of commen string
  uint32_t m_comment_len;

 public:
  // exception carries error number/identification
  CException m_exception;

  // TODO: 6/29/2010: incorporate string class
  // as soon as available
  //
  // ctor
  CMessage(CException exc, uint32_t severity, const wchar_t *fmt, uint32_t fmt_len, uint32_t num_params,
           const wchar_t *comment, uint32_t comment_len);

  // copy ctor
  CMessage(const CMessage &);

  // format contents into given buffer
  void Format(CWStringStatic *buf, VA_LIST) const;

  // severity accessor
  uint32_t GetSeverity() const { return m_severity; }

  // link object
  SLink m_link;

  // access a message by index
  static CMessage *GetMessage(uint32_t index);

  // format an error message
  static void FormatMessage(CWStringStatic *str, uint32_t major, uint32_t minor, ...);

#ifdef GPOS_DEBUG
  // debug print function
  IOstream &OsPrint(IOstream &) const;
#endif  // GPOS_DEBUG

};  // class CMessage
}  // namespace gpos

#endif  // !GPOS_CMessage_H

// EOF
