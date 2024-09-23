//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLDatumStatsDoubleMappable.h
//
//	@doc:
//		Class for representing DXL datum of types having double mapping
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumStatsDoubleMappable_H
#define GPDXL_CDXLDatumStatsDoubleMappable_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"

namespace gpdxl {
using namespace gpos;

// fwd decl

//---------------------------------------------------------------------------
//	@class:
//		CDXLDatumStatsDoubleMappable
//
//	@doc:
//		Class for representing DXL datum of types having double mapping
//
//---------------------------------------------------------------------------
class CDXLDatumStatsDoubleMappable : public CDXLDatumGeneric {
 private:
  // for statistics computation, map to double
  CDouble m_val;

 public:
  CDXLDatumStatsDoubleMappable(const CDXLDatumStatsDoubleMappable &) = delete;

  // ctor
  CDXLDatumStatsDoubleMappable(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, bool is_null, uint8_t *data,
                               uint32_t length, CDouble val);

  // dtor
  ~CDXLDatumStatsDoubleMappable() override = default;

  // serialize the datum as the given element

  // datum type
  EdxldatumType GetDatumType() const override { return CDXLDatum::EdxldatumStatsDoubleMappable; }

  // statistics related APIs

  // can datum be mapped to double
  bool IsDatumMappableToDouble() const override { return true; }

  // return the double mapping needed for statistics computation
  CDouble GetDoubleMapping() const override { return m_val; }

  // conversion function
  static CDXLDatumStatsDoubleMappable *Cast(CDXLDatum *dxl_datum) {
    GPOS_ASSERT(nullptr != dxl_datum);
    GPOS_ASSERT(CDXLDatum::EdxldatumStatsDoubleMappable == dxl_datum->GetDatumType());

    return dynamic_cast<CDXLDatumStatsDoubleMappable *>(dxl_datum);
  }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLDatumStatsDoubleMappable_H

// EOF
