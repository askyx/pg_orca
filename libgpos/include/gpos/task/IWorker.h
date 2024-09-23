//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		IWorker.h
//
//	@doc:
//		Interface class to worker; broken into interface and implementation
//		to avoid cyclic dependencies between worker, list, etc.
//		The Worker abstraction contains only components needed to schedule,
//		execute, and abort tasks; no task specific configuration such as
//		output streams is contained in Worker;
//---------------------------------------------------------------------------
#ifndef GPOS_IWorker_H
#define GPOS_IWorker_H

#include "gpos/common/CStackObject.h"
#include "gpos/types.h"

#define GPOS_CHECK_ABORT (IWorker::CheckAbort(__FILE__, __LINE__))

#define GPOS_CHECK_STACK_SIZE                  \
  do {                                         \
    if (NULL != IWorker::Self()) {             \
      (void)IWorker::Self()->CheckStackSize(); \
    }                                          \
  } while (0)

#define GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC (uint32_t(1500))

namespace gpos {
// prototypes
class ITask;
class CWorkerId;

//---------------------------------------------------------------------------
//	@class:
//		IWorker
//
//	@doc:
//		Interface to abstract scheduling primitive such as threads;
//
//---------------------------------------------------------------------------
class IWorker : public CStackObject {
 private:
  // check for abort request
  virtual void CheckForAbort(const char *, uint32_t) = 0;

 public:
  IWorker(const IWorker &) = delete;

  // dummy ctor
  IWorker() = default;

  // dummy dtor
  virtual ~IWorker() = default;

  // accessors
  virtual uintptr_t GetStackStart() const = 0;
  virtual ITask *GetTask() = 0;

  // stack check
  virtual bool CheckStackSize(uint32_t request = 0) const = 0;

  // lookup worker in worker pool manager
  static IWorker *Self();

  // check for aborts
  static void CheckAbort(const char *file, uint32_t line_num);

};  // class IWorker
}  // namespace gpos

#endif  // !GPOS_IWorker_H

// EOF
