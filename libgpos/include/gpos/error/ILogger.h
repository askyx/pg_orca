//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ILogger.h
//
//	@doc:
//		Interface class for logging
//---------------------------------------------------------------------------
#ifndef GPOS_ILogger_H
#define GPOS_ILogger_H

#include "gpos/types.h"

#ifndef GPOS_DEBUG
#define GPOS_LOG_MESSAGE_BUFFER_SIZE (1024 * 128)
#else
// have a larger buffer size for debug builds (e.g. printing out MEMO)
#define GPOS_LOG_MESSAGE_BUFFER_SIZE (8192 * 128)
#endif
#define GPOS_LOG_TRACE_BUFFER_SIZE (1024 * 8)
#define GPOS_LOG_ENTRY_BUFFER_SIZE (GPOS_LOG_MESSAGE_BUFFER_SIZE + 256)
#define GPOS_LOG_WRITE_RETRIES (10)

#define GPOS_WARNING(...) ILogger::Warning(__FILE__, __LINE__, __VA_ARGS__)

#define GPOS_TRACE(msg) ILogger::Trace(__FILE__, __LINE__, false /*is_err*/, msg)

#define GPOS_TRACE_ERR(msg) ILogger::Trace(__FILE__, __LINE__, true /*is_err*/, msg)

#define GPOS_TRACE_FORMAT(format, ...) \
  ILogger::TraceFormat(__FILE__, __LINE__, false /*is_err*/, GPOS_WSZ_LIT(format), __VA_ARGS__)

#define GPOS_TRACE_FORMAT_ERR(format, ...) \
  ILogger::TraceFormat(__FILE__, __LINE__, true /*is_err*/, GPOS_WSZ_LIT(format), __VA_ARGS__)

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		ILogger
//
//	@doc:
//		Interface for abstracting logging primitives.
//
//---------------------------------------------------------------------------

class ILogger {
  friend class CErrorHandlerStandard;

 public:
  // enum indicating error logging information
  enum ErrorInfoLevel {
    EeilMsg,            // log error message only
    EeilMsgHeader,      // log error header and message
    EeilMsgHeaderStack  // log error header, message and stack trace
  };

 private:
  // log message to current task's logger;
  // use stdout/stderr wrapping loggers outside worker framework;
  static void LogTask(const wchar_t *msg, uint32_t severity, bool is_err, const char *filename, uint32_t line);

 protected:
  // write log message
  virtual void Write(const wchar_t *log_entry, uint32_t severity) = 0;

 public:
  ILogger(const ILogger &) = delete;

  // ctor
  ILogger();

  // dtor
  virtual ~ILogger();

  // error info level accessor
  virtual ErrorInfoLevel InfoLevel() const = 0;

  // set error info level
  virtual void SetErrorInfoLevel(ErrorInfoLevel info_level) = 0;

  // retrieve warning message from repository and log it to error log
  static void Warning(const char *filename, uint32_t line, uint32_t major, uint32_t minor, ...);

  // log trace message to current task's output or error log
  static void Trace(const char *filename, uint32_t line, bool is_err, const wchar_t *msg);

  // format and log trace message to current task's output or error log
  static void TraceFormat(const char *filename, uint32_t line, bool is_err, const wchar_t *format, ...);

};  // class ILogger
}  // namespace gpos

#endif  // !GPOS_ILogger_H

// EOF
