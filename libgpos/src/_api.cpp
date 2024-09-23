//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 VMware, Inc. or its affiliates.
//
//	@filename:
//		_api.cpp
//
//	@doc:
//		Implementation of GPOS wrapper interface for GPDB.
//---------------------------------------------------------------------------

#include "gpos/_api.h"

#include "gpopt/exception.h"
#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCacheFactory.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CWorkerPoolManager.h"
#include "naucrates/exception.h"

using namespace gpos;

// refer gpopt/exception.cpp for explanation of errors
const uint32_t expected_opt_fallback[] = {
    gpopt::ExmiInvalidPlanAlternative,             // chosen plan id is outside range of possible plans
    gpopt::ExmiUnsupportedOp,                      // unsupported operator
    gpopt::ExmiUnsupportedPred,                    // unsupported predicate
    gpopt::ExmiUnsupportedCompositePartKey,        // composite partitioning keys
    gpopt::ExmiUnsupportedNonDeterministicUpdate,  // non deterministic update
    gpopt::ExmiNoPlanFound,
    gpopt::ExmiUnsupportedOp,
    gpopt::ExmiUnexpectedOp,
    gpopt::ExmiUnsatisfiedRequiredProperties,
    gpopt::ExmiEvalUnsupportedScalarExpr,
    gpopt::ExmiCTEProducerConsumerMisAligned,
    gpopt::ExmiNoStats};

// array of DXL minor exception types that trigger expected fallback to the planner
// refer naucrates/exception.cpp for explanation of errors
const uint32_t expected_dxl_fallback[] = {
    gpdxl::ExmiMDObjUnsupported,             // unsupported metadata object
    gpdxl::ExmiQuery2DXLUnsupportedFeature,  // unsupported feature during algebrization
    gpdxl::ExmiDXL2PlStmtConversion,         // unsupported feature during planned statement translation
    gpdxl::ExmiDXL2ExprAttributeNotFound,
    gpdxl::ExmiDXLMissingAttribute,
    gpdxl::ExmiDXLUnrecognizedOperator,
    gpdxl::ExmiDXLUnrecognizedCompOperator,
    gpdxl::ExmiDXLIncorrectNumberOfChildren,
    gpdxl::ExmiMDCacheEntryNotFound,
    gpdxl::ExmiQuery2DXLError};

bool FoundException(gpos::CException &exc, const uint32_t *exceptions, uint32_t size) {
  GPOS_ASSERT(nullptr != exceptions);

  uint32_t minor = exc.Minor();
  bool found = false;
  for (uint32_t ul = 0; !found && ul < size; ul++) {
    found = (exceptions[ul] == minor);
  }

  return found;
}

bool IsLoggableFailure(gpos::CException &exc) {
  uint32_t major = exc.Major();

  bool is_opt_failure_expected =
      gpopt::ExmaGPOPT == major && FoundException(exc, expected_opt_fallback, GPOS_ARRAY_SIZE(expected_opt_fallback));

  bool is_dxl_failure_expected = (gpdxl::ExmaDXL == major || gpdxl::ExmaMD == major) &&
                                 FoundException(exc, expected_dxl_fallback, GPOS_ARRAY_SIZE(expected_dxl_fallback));

  return (!is_opt_failure_expected && !is_dxl_failure_expected);
}

//---------------------------------------------------------------------------
//	@function:
//		gpos_init
//
//	@doc:
//		Initialize GPOS memory pool, worker pool and message repository
//
//---------------------------------------------------------------------------
void gpos_init(struct gpos_init_params *params) {
  CWorker::abort_requested_by_system = params->abort_requested;

  CMemoryPoolManager::Init();
  CWorkerPoolManager::Init();
  CMessageRepository::Init();
  CCacheFactory::Init();
}

//---------------------------------------------------------------------------
//	@function:
//		gpos_exec
//
//	@doc:
//		Execute function as a GPOS task using current thread;
//		return 0 for successful completion, 1 for error;
//
//---------------------------------------------------------------------------
int gpos_exec(gpos_exec_params *params) {
  // check if passed parameters are valid
  if (nullptr == params || nullptr == params->func) {
    return 1;
  }

  try {
    CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

    // check if worker pool is initialized
    if (nullptr == pwpm) {
      return 1;
    }

    if (pwpm->Self()) {
      // Raise an exception, if a worker already exists in the Worker Pool
      // Manager. Since, only one worker is supported, if a new worker is
      // registered then the address of old worker will be lost and would
      // lead to an undefined behaviour.

      GPOS_RAISE(CException::ExmaInvalid, CException::ExmiORCAInvalidState);
    }

    // if no stack start address is passed, use address in current stack frame
    void *pvStackStart = params->stack_start;
    if (nullptr == pvStackStart) {
      pvStackStart = &pwpm;
    }

    // put worker to stack - main thread has id '0'
    CWorker wrkr(GPOS_WORKER_STACK_SIZE, (uintptr_t)pvStackStart);

    // scope for memory pool
    {
      // setup task memory
      CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
      CMemoryPool *mp = amp.Pmp();

      // scope for ATP
      {
        // task handler for this process
        CAutoTaskProxy atp(mp, pwpm, true /*fPropagateError*/);

        CTask *ptsk = atp.Create(params->func, params->arg, params->abort_requested);

        ptsk->GetTaskCtxt()->config = ((gpdxl::OptConfig *)params->config);

        // init TLS
        ptsk->GetTls().Reset(mp);

        CAutoP<CWStringStatic> apwstr;
        CAutoP<COstreamString> aposs;
        CAutoP<CLoggerStream> aplogger;

        // use passed buffer for logging
        if (nullptr != params->error_buffer) {
          GPOS_ASSERT(0 < params->error_buffer_size);

          apwstr = GPOS_NEW(mp)
              CWStringStatic((wchar_t *)params->error_buffer, params->error_buffer_size / GPOS_SIZEOF(wchar_t));
          aposs = GPOS_NEW(mp) COstreamString(apwstr.Value());
          aplogger = GPOS_NEW(mp) CLoggerStream(*aposs.Value());

          CTaskContext *ptskctxt = ptsk->GetTaskCtxt();
          ptskctxt->SetLogOut(aplogger.Value());
          ptskctxt->SetLogErr(aplogger.Value());
        }

        // execute function
        atp.Execute(ptsk);

        // export task result
        params->result = ptsk->GetRes();

        // check for errors during execution
        if (CTask::EtsError == ptsk->GetStatus()) {
          return 1;
        }
      }
    }
  } catch (CException ex) {
    throw ex;
  } catch (...) {
    // unexpected failure
    GPOS_RAISE(CException::ExmaUnhandled, CException::ExmiUnhandled);
  }

  return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		gpos_terminate
//
//	@doc:
//		Shutdown GPOS memory pool, worker pool and message repository
//
//---------------------------------------------------------------------------
void gpos_terminate() {
#ifdef GPOS_DEBUG
  CMessageRepository::Shutdown();
  CWorkerPoolManager::Shutdown();
  CCacheFactory::Shutdown();
  CMemoryPoolManager::Shutdown();
#endif  // GPOS_DEBUG
}

// EOF
