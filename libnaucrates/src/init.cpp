//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		init.cpp
//
//	@doc:
//		Implementation of initialization and termination functions for
//		libgpdxl.
//---------------------------------------------------------------------------

#include "naucrates/init.h"

#include "gpos/memory/CMemoryPoolManager.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/exception.h"

using namespace gpos;
using namespace gpdxl;

static CMemoryPool *pmpXerces = nullptr;

static CMemoryPool *pmpDXL = nullptr;

// safe-guard to prevent initializing DXL support more than once
static uintptr_t m_ulpInitDXL = 0;

// safe-guard to prevent shutting DXL support down more than once
static uintptr_t m_ulpShutdownDXL = 0;

//---------------------------------------------------------------------------
//      @function:
//              InitDXL
//
//      @doc:
//				Initialize DXL support; must be called before any library
//				function is called
//
//
//---------------------------------------------------------------------------
void InitDXL() {
  if (0 < m_ulpInitDXL) {
    // DXL support is already initialized by a previous call
    return;
  }

  GPOS_ASSERT(nullptr != pmpXerces);
  GPOS_ASSERT(nullptr != pmpDXL);

  // initialize DXL tokens
  CDXLTokens::Init(pmpDXL);

  m_ulpInitDXL++;
}

//---------------------------------------------------------------------------
//      @function:
//              ShutdownDXL
//
//      @doc:
//				Shutdown DXL support; called only at library termination
//
//---------------------------------------------------------------------------
void ShutdownDXL() {
  if (0 < m_ulpShutdownDXL) {
    // DXL support is already shut-down by a previous call
    return;
  }

  GPOS_ASSERT(nullptr != pmpXerces);

  m_ulpShutdownDXL++;

  CDXLTokens::Terminate();
}

//---------------------------------------------------------------------------
//      @function:
//              gpdxl_init
//
//      @doc:
//              Initialize Xerces parser utils
//
//---------------------------------------------------------------------------
void gpdxl_init() {
  // create memory pool for Xerces global allocations
  pmpXerces = CMemoryPoolManager::CreateMemoryPool();

  // create memory pool for DXL global allocations
  pmpDXL = CMemoryPoolManager::CreateMemoryPool();

  // add standard exception messages
  EresExceptionInit(pmpDXL);
}

//---------------------------------------------------------------------------
//      @function:
//              gpdxl_terminate
//
//      @doc:
//              Terminate Xerces parser utils and destroy memory pool
//
//---------------------------------------------------------------------------
void gpdxl_terminate() {
#ifdef GPOS_DEBUG
  ShutdownDXL();

  if (nullptr != pmpDXL) {
    CMemoryPoolManager::Destroy(pmpDXL);
    pmpDXL = nullptr;
  }

  if (nullptr != pmpXerces) {
    CMemoryPoolManager::Destroy(pmpXerces);
    pmpXerces = nullptr;
  }
#endif  // GPOS_DEBUG
}

// EOF
