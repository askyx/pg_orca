//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumGeneric.h
//
//	@doc:
//		Class for representing DXL datum of type generic
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumGeneric_H
#define GPDXL_CDXLDatumGeneric_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl {
using namespace gpos;

// fwd decl

//---------------------------------------------------------------------------
//	@class:
//		CDXLDatumGeneric
//
//	@doc:
//		Class for representing DXL datum of type generic
//
//---------------------------------------------------------------------------
class CDXLDatumGeneric : public CDXLDatum {
 private:
 protected:
  // datum byte array
  uint8_t *m_byte_array;

 public:
  CDXLDatumGeneric(const CDXLDatumGeneric &) = delete;

  // ctor
  CDXLDatumGeneric(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, bool is_null, uint8_t *data,
                   uint32_t length);

  // dtor
  ~CDXLDatumGeneric() override;

  // byte array
  const uint8_t *GetByteArray() const;

  // serialize the datum as the given element

  // datum type
  EdxldatumType GetDatumType() const override { return CDXLDatum::EdxldatumGeneric; }

  // conversion function
  static CDXLDatumGeneric *Cast(CDXLDatum *dxl_datum) {
    GPOS_ASSERT(nullptr != dxl_datum);
    GPOS_ASSERT(CDXLDatum::EdxldatumGeneric == dxl_datum->GetDatumType() ||
                CDXLDatum::EdxldatumStatsDoubleMappable == dxl_datum->GetDatumType() ||
                CDXLDatum::EdxldatumStatsLintMappable == dxl_datum->GetDatumType());

    return dynamic_cast<CDXLDatumGeneric *>(dxl_datum);
  }

  // statistics related APIs

  // can datum be mapped to int64_t
  virtual bool IsDatumMappableToLINT() const { return false; }

  // return the lint mapping needed for statistics computation
  virtual int64_t GetLINTMapping() const {
    GPOS_ASSERT(IsDatumMappableToLINT());

    return 0;
  }

  // can datum be mapped to a double
  virtual bool IsDatumMappableToDouble() const { return false; }

  // return the double mapping needed for statistics computation
  virtual CDouble GetDoubleMapping() const {
    GPOS_ASSERT(IsDatumMappableToDouble());
    return 0;
  }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLDatumGeneric_H

// EOF
