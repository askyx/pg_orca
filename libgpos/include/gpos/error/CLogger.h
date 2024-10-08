//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLogger.h
//
//	@doc:
//		Partial implementation of interface class for logging
//---------------------------------------------------------------------------
#ifndef GPOS_CLogger_H
#define GPOS_CLogger_H

#include "gpos/error/ILogger.h"
#include "gpos/string/CWStringStatic.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CLogger
//
//	@doc:
//		Partial implementation of interface for abstracting logging primitives.
//
//---------------------------------------------------------------------------
class CLogger : public ILogger {
  friend class ILogger;
  friend class CErrorHandlerStandard;

 private:
  // buffer used to construct the log entry
  wchar_t m_entry[GPOS_LOG_ENTRY_BUFFER_SIZE];

  // buffer used to construct the log entry
  wchar_t m_msg[GPOS_LOG_MESSAGE_BUFFER_SIZE];

  // buffer used to retrieve system error messages
  char m_retrieved_msg[GPOS_LOG_MESSAGE_BUFFER_SIZE];

  // entry buffer wrapper
  CWStringStatic m_entry_wrapper;

  // message buffer wrapper
  CWStringStatic m_msg_wrapper;

  // error logging information level
  ErrorInfoLevel m_info_level;

  // log message
  void Log(const wchar_t *msg, uint32_t severity, const char *filename, uint32_t line);

  // format log message
  void Format(const wchar_t *msg, uint32_t severity, const char *filename, uint32_t line);

  // add date to message
  void AppendDate();

  // report logging failure
  void ReportFailure();

 protected:
  // accessor for system error buffer
  char *Msg() { return m_retrieved_msg; }

 public:
  CLogger(const CLogger &) = delete;

  // ctor
  explicit CLogger(ErrorInfoLevel info_level = ILogger::EeilMsgHeaderStack);

  // dtor
  ~CLogger() override;

  // error level accessor
  ErrorInfoLevel InfoLevel() const override { return m_info_level; }

  // set error info level
  void SetErrorInfoLevel(ErrorInfoLevel info_level) override { m_info_level = info_level; }

};  // class CLogger
}  // namespace gpos

#endif  // !GPOS_CLogger_H

// EOF
