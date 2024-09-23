//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerSyslog.h
//
//	@doc:
//		Implementation of logging interface over syslog
//---------------------------------------------------------------------------
#ifndef GPOS_CLoggerSyslog_H
#define GPOS_CLoggerSyslog_H

#include "gpos/error/CLogger.h"

#define GPOS_SYSLOG_ALERT(szMsg) CLoggerSyslog::Alert(GPOS_WSZ_LIT(szMsg))

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CLoggerSyslog
//
//	@doc:
//		Syslog logging.
//
//---------------------------------------------------------------------------

class CLoggerSyslog : public CLogger {
 private:
  // executable name
  const char *m_proc_name;

  // initialization flags
  uint32_t m_init_mask;

  // message priotity
  uint32_t m_message_priority;

  // write string to syslog
  void Write(const wchar_t *log_entry, uint32_t severity) override;

  static CLoggerSyslog m_alert_logger;

 public:
  CLoggerSyslog(const CLoggerSyslog &) = delete;

  // ctor
  CLoggerSyslog(const char *proc_name, uint32_t init_mask, uint32_t message_priority);

  // dtor
  ~CLoggerSyslog() override;

  // write alert message to syslog - use ASCII characters only
  static void Alert(const wchar_t *msg);

};  // class CLoggerSyslog
}  // namespace gpos

#endif  // !GPOS_CLoggerSyslog_H

// EOF
