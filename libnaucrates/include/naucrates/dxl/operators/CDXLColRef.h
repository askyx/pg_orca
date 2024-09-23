//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColRef.h
//
//	@doc:
//		Class for representing column references.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLColRef_H
#define GPDXL_CDXLColRef_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl {
using namespace gpmd;
using namespace gpos;

// fwd decl

class CDXLColRef;

// arrays of column references
using CDXLColRefArray = CDynamicPtrArray<CDXLColRef, CleanupRelease>;

//---------------------------------------------------------------------------
//	@class:
//		CDXLColRef
//
//	@doc:
//		Class for representing references to columns in DXL trees
//
//---------------------------------------------------------------------------
class CDXLColRef : public CRefCount {
 private:
  // name
  CMDName *m_mdname;

  // id
  const uint32_t m_id;

  // column type
  IMDId *m_mdid_type;

  // column type modifier
  int32_t m_iTypeModifer;

 public:
  CDXLColRef(const CDXLColRef &) = delete;

  // ctor/dtor
  CDXLColRef(CMDName *mdname, uint32_t id, IMDId *mdid_type, int32_t type_modifier);

  ~CDXLColRef() override;

  // accessors
  const CMDName *MdName() const;

  IMDId *MdidType() const;

  int32_t TypeModifier() const;

  uint32_t Id() const;
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLColRef_H

// EOF
