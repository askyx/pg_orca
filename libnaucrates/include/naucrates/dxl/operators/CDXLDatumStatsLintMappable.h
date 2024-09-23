//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLDatumStatsLintMappable.h
//
//	@doc:
//		Class for representing DXL datum of types having int64_t mapping
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumStatsLintMappable_H
#define GPDXL_CDXLDatumStatsLintMappable_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"

namespace gpdxl {
using namespace gpos;

// fwd decl

//---------------------------------------------------------------------------
//	@class:
//		CDXLDatumStatsLintMappable
//
//	@doc:
//		Class for representing DXL datum of types having int64_t mapping
//
//---------------------------------------------------------------------------
class CDXLDatumStatsLintMappable : public CDXLDatumGeneric {
 private:
  // for statistics computation, map to int64_t
  int64_t m_val;

 public:
  CDXLDatumStatsLintMappable(const CDXLDatumStatsLintMappable &) = delete;

  // ctor
  CDXLDatumStatsLintMappable(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, bool is_null,
                             uint8_t *byte_array, uint32_t length, int64_t value);

  // dtor
  ~CDXLDatumStatsLintMappable() override = default;

  // serialize the datum as the given element

  // datum type
  EdxldatumType GetDatumType() const override { return CDXLDatum::EdxldatumStatsLintMappable; }

  // conversion function
  static CDXLDatumStatsLintMappable *Cast(CDXLDatum *dxl_datum) {
    GPOS_ASSERT(nullptr != dxl_datum);
    GPOS_ASSERT(CDXLDatum::EdxldatumStatsLintMappable == dxl_datum->GetDatumType());

    return dynamic_cast<CDXLDatumStatsLintMappable *>(dxl_datum);
  }

  // statistics related APIs

  // can datum be mapped to int64_t
  bool IsDatumMappableToLINT() const override { return true; }

  // return the int64_t mapping needed for statistics computation
  int64_t GetLINTMapping() const override { return m_val; }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLDatumStatsLintMappable_H

// EOF
