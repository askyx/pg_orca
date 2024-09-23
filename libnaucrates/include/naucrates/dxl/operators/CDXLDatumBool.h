//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumBool.h
//
//	@doc:
//		Class for representing DXL boolean datum
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumBool_H
#define GPDXL_CDXLDatumBool_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl {
using namespace gpos;

// fwd decl

//---------------------------------------------------------------------------
//	@class:
//		CDXLDatumBool
//
//	@doc:
//		Class for representing DXL boolean datums
//
//---------------------------------------------------------------------------
class CDXLDatumBool : public CDXLDatum {
 private:
  // boolean value
  bool m_value;

 public:
  CDXLDatumBool(const CDXLDatumBool &) = delete;

  // ctor
  CDXLDatumBool(CMemoryPool *mp, IMDId *mdid_type, bool is_null, bool value);

  // dtor
  ~CDXLDatumBool() override = default;

  // serialize the datum as the given element

  // datum type
  EdxldatumType GetDatumType() const override { return CDXLDatum::EdxldatumBool; }

  // accessor of boolean value
  bool GetValue() const { return m_value; }

  // conversion function
  static CDXLDatumBool *Cast(CDXLDatum *dxl_datum) {
    GPOS_ASSERT(nullptr != dxl_datum);
    GPOS_ASSERT(CDXLDatum::CDXLDatum::EdxldatumBool == dxl_datum->GetDatumType());

    return dynamic_cast<CDXLDatumBool *>(dxl_datum);
  }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLDatumBool_H

// EOF
