//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CWorker.h
//
//	@doc:
//		Abstraction of schedule-able unit, e.g. a pthread etc.
//---------------------------------------------------------------------------
#ifndef GPOS_CWorker_H
#define GPOS_CWorker_H

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/common/CTimerUser.h"
#include "gpos/task/CTask.h"
#include "gpos/task/IWorker.h"

namespace gpos {
class CTask;

//---------------------------------------------------------------------------
//	@class:
//		CWorker
//
//	@doc:
//		Worker abstraction keeps track of resource held by worker; management
//		of control flow such as abort signal etc.
//
//---------------------------------------------------------------------------

class CWorker : public IWorker {
  friend class CAutoTaskProxy;

 private:
  // current task
  CTask *m_task;

  // available stack
  uint32_t m_stack_size;

  // start address of current thread's stack
  const uintptr_t m_stack_start;

  // execute single task
  void Execute(CTask *task);

  // check for abort request
  void CheckForAbort(const char *file, uint32_t line_num) override;

 public:
  CWorker(const CWorker &) = delete;

  // ctor
  CWorker(uint32_t stack_size, uintptr_t stack_start);

  // dtor
  ~CWorker() override;

  // stack start accessor
  inline uintptr_t GetStackStart() const override { return m_stack_start; }

  // stack check
  bool CheckStackSize(uint32_t request = 0) const override;

  // accessor
  inline CTask *GetTask() override { return m_task; }

  // slink for hashtable
  SLink m_link;

  // lookup worker in worker pool manager
  static CWorker *Self() { return dynamic_cast<CWorker *>(IWorker::Self()); }

  // host system callback function to report abort requests
  static bool (*abort_requested_by_system)(void);

};  // class CWorker
}  // namespace gpos

#endif  // !GPOS_CWorker_H

// EOF
