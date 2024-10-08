//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ILogger.cpp
//
//	@doc:
//		Worker abstraction, e.g. thread
//---------------------------------------------------------------------------

#include "gpos/error/ILogger.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/error/CLoggerSyslog.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/string/CWStringConst.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		ILogger::ILogger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
ILogger::ILogger() = default;

//---------------------------------------------------------------------------
//	@function:
//		ILogger::~ILogger
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
ILogger::~ILogger() = default;

//---------------------------------------------------------------------------
//	@function:
//		ILogger::Warning
//
//	@doc:
//		Retrieve warning message from repository and log it to error log
//
//---------------------------------------------------------------------------
void ILogger::Warning(const char *filename, uint32_t line, uint32_t major, uint32_t minor...) {
  GPOS_CHECK_ABORT;

  // get warning
  CException exc(major, minor, filename, line);

  ITask *task = ITask::Self();

  // get current task's locale
  ELocale locale = ElocEnUS_Utf8;
  if (nullptr != task) {
    locale = task->Locale();
  }

  // retrieve warning message from repository
  CMessage *msg = CMessageRepository::GetMessageRepository()->LookupMessage(exc, locale);

  GPOS_ASSERT(CException::ExsevWarning == msg->GetSeverity());

  wchar_t buffer[GPOS_LOG_MESSAGE_BUFFER_SIZE];
  CWStringStatic str(buffer, GPOS_ARRAY_SIZE(buffer));

  // format warning message
  {
    VA_LIST va_args;

    VA_START(va_args, minor);

    msg->Format(&str, va_args);

    VA_END(va_args);
  }

  LogTask(str.GetBuffer(), CException::ExsevWarning, true /*is_err*/, filename, line);
}

//---------------------------------------------------------------------------
//	@function:
//		ILogger::Trace
//
//	@doc:
//		Format and log debugging message to current task's output or error log
//
//---------------------------------------------------------------------------
void ILogger::Trace(const char *filename, uint32_t line, bool is_err, const wchar_t *msg) {
  GPOS_CHECK_ABORT;

  LogTask(msg, CException::ExsevTrace, is_err, filename, line);
}

//---------------------------------------------------------------------------
//	@function:
//		ILogger::TraceFormat
//
//	@doc:
//		Format and log debugging message to current task's output or error log
//
//---------------------------------------------------------------------------
void ILogger::TraceFormat(const char *filename, uint32_t line, bool is_err, const wchar_t *format, ...) {
  GPOS_CHECK_ABORT;

  wchar_t buffer[GPOS_LOG_TRACE_BUFFER_SIZE];
  CWStringStatic str(buffer, GPOS_ARRAY_SIZE(buffer));

  VA_LIST va_args;

  // get arguments
  VA_START(va_args, format);

  str.AppendFormatVA(format, va_args);

  // reset arguments
  VA_END(va_args);

  LogTask(str.GetBuffer(), CException::ExsevTrace, is_err, filename, line);
}

//---------------------------------------------------------------------------
//	@function:
//		ILogger::LogTask
//
//	@doc:
//		Log message to current task's logger;
// 		Use stdout/stderr-wrapping loggers outside worker framework;
//
//---------------------------------------------------------------------------
void ILogger::LogTask(const wchar_t *msg, uint32_t severity, bool is_err, const char *filename, uint32_t line) {
  CLogger *log = nullptr;

  if (is_err) {
    log = &CLoggerStream::m_stderr_stream_logger;
  } else {
    log = &CLoggerStream::m_stdout_stream_logger;
  }

  ITask *task = ITask::Self();
  if (nullptr != task) {
    if (is_err) {
      log = dynamic_cast<CLogger *>(task->GetErrorLogger());
    } else {
      log = dynamic_cast<CLogger *>(task->GetOutputLogger());
    }
  }

  GPOS_ASSERT(nullptr != log);

  log->Log(msg, severity, filename, line);
}

// EOF
