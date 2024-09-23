//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//	       	syslibwrapper.h
//
//	@doc:
//	       	Wrapper for functions in system library
//
//---------------------------------------------------------------------------

#ifndef GPOS_syslibwrapper_H
#define GPOS_syslibwrapper_H

#include "gpos/common/clibtypes.h"
#include "gpos/types.h"

namespace gpos {
namespace syslib {
// get the date and time
void GetTimeOfDay(TIMEVAL *tv, TIMEZONE *tz);

// get system and user time
void GetRusage(RUSAGE *usage);

// open a connection to the system logger for a program
void OpenLog(const char *ident, int32_t option, int32_t facility);

// generate a log message
void SysLog(int32_t priority, const char *format);

// close the descriptor being used to write to the system logger
void CloseLog();

}  // namespace syslib
}  // namespace gpos

#endif
// EOF
