//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CMDProviderMemory.cpp
//
//	@doc:
//		Implementation of a memory-based metadata provider, which loads all
//		objects in memory and provides a function for looking them up by id.
//---------------------------------------------------------------------------

#include "naucrates/md/CMDProviderMemory.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/task/CWorker.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/md/CDXLExtStatsInfo.h"
#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::CMDProviderMemory
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDProviderMemory::CMDProviderMemory(CMemoryPool *mp, IMDCacheObjectArray *mdcache_obj_array) : m_mdmap(nullptr) {}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::~CMDProviderMemory
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDProviderMemory::~CMDProviderMemory() {
  CRefCount::SafeRelease(m_mdmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::MDId
//
//	@doc:
//		Returns the mdid for the requested system and type info.
//		The caller takes ownership over the object.
//
//---------------------------------------------------------------------------
IMDId *CMDProviderMemory::MDId(CMemoryPool *mp, CSystemId sysid, IMDType::ETypeInfo type_info) const {
  return GetGPDBTypeMdid(mp, sysid, type_info);
}
