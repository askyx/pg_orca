//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoTraceFlag.h
//
//	@doc:
//		Auto wrapper to set/reset a traceflag for a scope
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoTraceFlag_H
#define GPOS_CAutoTraceFlag_H

#include "gpos/base.h"
#include "gpos/common/CStackObject.h"
#include "gpos/task/ITask.h"
#include "gpos/task/traceflags.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CAutoTraceFlag
//
//	@doc:
//		Auto wrapper;
//
//---------------------------------------------------------------------------
class CAutoTraceFlag : public CStackObject {
 private:
  // traceflag id
  uint32_t m_trace;

  // original value
  bool m_orig;

 public:
  CAutoTraceFlag(const CAutoTraceFlag &) = delete;

  // ctor
  CAutoTraceFlag(uint32_t trace, bool orig);

  // dtor
  virtual ~CAutoTraceFlag();

};  // class CAutoTraceFlag

}  // namespace gpos

#endif  // !GPOS_CAutoTraceFlag_H

// EOF
