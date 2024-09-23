//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CTask.h
//
//	@doc:
//		Interface class for task abstraction
//---------------------------------------------------------------------------
#ifndef GPOS_CTask_H
#define GPOS_CTask_H

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/error/CException.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/task/CTaskContext.h"
#include "gpos/task/CTaskId.h"
#include "gpos/task/CTaskLocalStorage.h"
#include "gpos/task/ITask.h"

namespace gpos {
//---------------------------------------------------------------------------
//	@class:
//		CTask
//
//	@doc:
//		Interface to abstract task (work unit);
//		provides asynchronous task execution and error handling;
//
//---------------------------------------------------------------------------
class CTask : public ITask {
  friend class CAutoTaskProxy;
  friend class CAutoTaskProxyTest;
  friend class CTaskSchedulerFifo;
  friend class CWorker;
  friend class CWorkerPoolManager;
  friend class CAutoSuspendAbort;

 private:
  // task memory pool -- exclusively used by this task
  CMemoryPool *m_mp;

  // task context
  CTaskContext *m_task_ctxt;

  // error context
  IErrorContext *m_err_ctxt;

  // error handler stack
  CErrorHandler *m_err_handle;

  // function to execute
  void *(*m_func)(void *);

  // function argument
  void *m_arg;

  // function result
  void *m_res;

  // TLS
  CTaskLocalStorage m_tls;

  // task status
  ETaskStatus m_status;

  // cancellation flag
  bool *m_cancel;

  // local cancellation flag; used when no flag is externally passed
  bool m_cancel_local;

  // counter of requests to suspend cancellation
  uint32_t m_abort_suspend_count;

  // flag denoting task completion report
  bool m_reported;

  // task identifier
  CTaskId m_tid;

  // ctor
  CTask(CMemoryPool *mp, CTaskContext *task_ctxt, IErrorContext *err_ctxt, bool *cancel);

  // binding a task structure to a function and its arguments
  void Bind(void *(*func)(void *), void *arg);

  // execution, called by the owning worker
  void Execute();

  // check if task has been scheduled
  bool IsScheduled() const;

  // check if task finished executing
  bool IsFinished() const;

  // check if task is currently executing
  bool IsRunning() const { return EtsRunning == m_status; }

  // reported flag accessor
  bool IsReported() const { return m_reported; }

  // set reported flag
  void SetReported() {
    GPOS_ASSERT(!m_reported && "Task already reported as completed");

    m_reported = true;
  }

 public:
  CTask(const CTask &) = delete;

  // dtor
  ~CTask() override;

  // accessor for memory pool, e.g. used for allocating task parameters in
  CMemoryPool *Pmp() const override { return m_mp; }

  // TLS accessor
  CTaskLocalStorage &GetTls() override { return m_tls; }

  // task id accessor
  CTaskId &GetTid() { return m_tid; }

  // task context accessor
  CTaskContext *GetTaskCtxt() const override { return m_task_ctxt; }

  // basic output streams
  ILogger *GetOutputLogger() const override { return this->m_task_ctxt->GetOutputLogger(); }

  ILogger *GetErrorLogger() const override { return this->m_task_ctxt->GetErrorLogger(); }

  bool SetTrace(uint32_t trace, bool val) override { return this->m_task_ctxt->SetTrace(trace, val); }

  bool IsTraceSet(uint32_t trace) override { return this->m_task_ctxt->IsTraceSet(trace); }

  // locale
  ELocale Locale() const override { return m_task_ctxt->Locale(); }

  // check if task is canceled
  bool IsCanceled() const { return *m_cancel; }

  // reset cancel flag
  void ResetCancel() { *m_cancel = false; }

  // set cancel flag
  void Cancel() { *m_cancel = true; }

  // check if a request to suspend abort was received
  bool IsAbortSuspended() const { return (0 < m_abort_suspend_count); }

  // increment counter for requests to suspend abort
  void SuspendAbort() { m_abort_suspend_count++; }

  // decrement counter for requests to suspend abort
  void ResumeAbort();

  // task status accessor
  ETaskStatus GetStatus() const { return m_status; }

  // set task status
  void SetStatus(ETaskStatus status);

  // task result accessor
  void *GetRes() const { return m_res; }

  // error context
  IErrorContext *GetErrCtxt() const override { return m_err_ctxt; }

  // error context
  CErrorContext *ConvertErrCtxt() { return dynamic_cast<CErrorContext *>(m_err_ctxt); }

  // pending exceptions
  bool HasPendingExceptions() const override { return m_err_ctxt->IsPending(); }

#ifdef GPOS_DEBUG
  // check if task has expected status
  bool CheckStatus(bool completed);
#endif  // GPOS_DEBUG

  // slink for auto task proxy
  SLink m_proxy_link;

  // slink for task scheduler
  SLink m_task_scheduler_link;

  // slink for worker pool manager
  SLink m_worker_pool_manager_link;

  static CTask *Self() { return dynamic_cast<CTask *>(ITask::Self()); }

};  // class CTask

}  // namespace gpos

#endif  // !GPOS_CTask_H

// EOF
