//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CMDProviderMemory.h
//
//	@doc:
//		Memory-based provider of metadata objects.
//---------------------------------------------------------------------------

#ifndef GPMD_CMDProviderMemory_H
#define GPMD_CMDProviderMemory_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"
#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"

namespace gpmd {
using namespace gpos;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		CMDProviderMemory
//
//	@doc:
//		Memory-based provider of metadata objects.
//
//---------------------------------------------------------------------------
class CMDProviderMemory : public IMDProvider {
 protected:
  // hash map of serialized MD objects indexed by their MD id
  using MDIdToSerializedMDIdMap =
      CHashMap<IMDId, CWStringDynamic, IMDId::MDIdHash, IMDId::MDIdCompare, CleanupRelease, CleanupDelete>;

  // metadata objects indexed by their metadata id
  MDIdToSerializedMDIdMap *m_mdmap;

  // private copy ctor
  CMDProviderMemory(const CMDProviderMemory &);

 public:
  // ctor
  CMDProviderMemory(CMemoryPool *mp, IMDCacheObjectArray *mdcache_obj_array);

  // dtor
  ~CMDProviderMemory() override;

  // returns the requested metadata object
  IMDCacheObject *GetMDObj(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid,
                           IMDCacheObject::Emdtype mdtype) const override {
    return nullptr;
  }

  // return the mdid for the specified system id and type
  IMDId *MDId(CMemoryPool *mp, CSystemId sysid, IMDType::ETypeInfo type_info) const override;
};
}  // namespace gpmd

#endif  // !GPMD_CMDProviderMemory_H

// EOF
