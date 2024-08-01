//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		IMDCacheObject.h
//
//	@doc:
//		Base interface for metadata cache objects
//---------------------------------------------------------------------------

#ifndef GPMD_IMDCacheObject_H
#define GPMD_IMDCacheObject_H

#include "gpos/base.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDInterface.h"

// fwd decl
namespace gpos {
class CWStringDynamic;
}
namespace gpdxl {}

namespace gpmd {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDCacheObject
//
//	@doc:
//		Base interface for metadata cache objects
//
//---------------------------------------------------------------------------
class IMDCacheObject : public IMDInterface {
 public:
  // type of md object
  enum Emdtype {
    EmdtRel,
    EmdtInd,
    EmdtFunc,
    EmdtAgg,
    EmdtOp,
    EmdtType,
    EmdtCheckConstraint,
    EmdtRelStats,
    EmdtColStats,
    EmdtCastFunc,
    EmdtScCmp,
    EmdtExtStats,
    EmdtExtStatsInfo,
    EmdtSentinel
  };

  // md id of cache object
  virtual IMDId *MDId() const = 0;

  // cache object name
  virtual CMDName Mdname() const = 0;

  // object type
  virtual Emdtype MDType() const = 0;

#ifdef GPOS_DEBUG
  virtual void DebugPrint(IOstream &os) const = 0;
#endif
};

using IMDCacheObjectArray = CDynamicPtrArray<IMDCacheObject, CleanupRelease>;

}  // namespace gpmd

#endif  // !GPMD_IMDCacheObject_H

// EOF
