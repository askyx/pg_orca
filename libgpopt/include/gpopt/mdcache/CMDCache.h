//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDCache.h
//
//	@doc:
//		Metadata cache.
//---------------------------------------------------------------------------

#ifndef GPOPT_CMDCache_H
#define GPOPT_CMDCache_H

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDKey.h"
#include "gpos/base.h"
#include "gpos/memory/CCache.h"
#include "gpos/memory/CCacheFactory.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CMDCache
//
//	@doc:
//		A wrapper for a generic cache to hide the details of metadata cache
//		creation and encapsulate a singleton cache object
//
//---------------------------------------------------------------------------
class CMDCache {
 private:
  // pointer to the underlying cache
  static CMDAccessor::MDCache *m_pcache;

  // the maximum size of the cache
  static uint64_t m_ullCacheQuota;

  // private ctor
  CMDCache() = default;

  // private dtor
  ~CMDCache() = default;

 public:
  CMDCache(const CMDCache &) = delete;

  // initialize underlying cache
  static void Init();

  // has cache been initialized?
  static bool FInitialized() { return (nullptr != m_pcache); }

  // destroy global instance
  static void Shutdown();

  // set the maximum size of the cache
  static void SetCacheQuota(uint64_t ullCacheQuota);

  // get the maximum size of the cache
  static uint64_t ULLGetCacheQuota();

  // get the number of times we evicted entries from this cache
  static uint64_t ULLGetCacheEvictionCounter();

  // reset global instance
  static void Reset();

  // global accessor
  static CMDAccessor::MDCache *Pcache() { return m_pcache; }

};  // class CMDCache

}  // namespace gpopt

#endif  // !GPOPT_CMDCache_H

// EOF
