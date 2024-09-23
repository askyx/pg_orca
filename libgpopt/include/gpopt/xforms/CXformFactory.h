//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformFactory.h
//
//	@doc:
//		Management of global xform set
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformFactory_H
#define GPOPT_CXformFactory_H

#include "gpopt/xforms/CXform.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformFactory
//
//	@doc:
//		Factory class to manage xforms
//
//---------------------------------------------------------------------------
class CXformFactory {
 private:
  // definition of hash map to maintain mappings
  using XformNameToXformMap =
      CHashMap<char, CXform, gpos::HashValue<char>, CXform::FEqualIds, CleanupDeleteArray<char>, CleanupNULL<CXform>>;

  // memory pool
  CMemoryPool *m_mp;

  // range of all xforms
  CXform *m_rgpxf[CXform::ExfSentinel];

  // name -> xform map
  XformNameToXformMap *m_phmszxform;

  // bitset of exploration xforms
  CXformSet *m_pxfsExploration;

  // bitset of implementation xforms
  CXformSet *m_pxfsImplementation;

  // ensure that xforms are inserted in order
  uint32_t m_lastAddedOrSkippedXformId;

  // global instance
  static CXformFactory *m_pxff;

  // private ctor
  explicit CXformFactory(CMemoryPool *mp);

  // actual adding of xform
  void Add(CXform *pxform);

  // skip unused xforms that have been removed, preserving
  // xform ids of the remaining ones
  void SkipUnused(uint32_t numXformsToSkip) { m_lastAddedOrSkippedXformId += numXformsToSkip; }

 public:
  CXformFactory(const CXformFactory &) = delete;

  // dtor
  ~CXformFactory();

  // create all xforms
  void Instantiate();

  // accessor by xform id
  CXform *Pxf(CXform::EXformId exfid) const;

  // accessor by xform name
  CXform *Pxf(const char *szXformName) const;

  // accessor of exploration xforms
  CXformSet *PxfsExploration() const { return m_pxfsExploration; }

  // accessor of implementation xforms
  CXformSet *PxfsImplementation() const { return m_pxfsImplementation; }

  // is this xform id still used?
  bool IsXformIdUsed(CXform::EXformId exfid);

  // global accessor
  static CXformFactory *Pxff() { return m_pxff; }

  // initialize global factory instance
  static void Init();

  // destroy global factory instance
  static void Shutdown();

};  // class CXformFactory

}  // namespace gpopt

#endif  // !GPOPT_CXformFactory_H

// EOF
