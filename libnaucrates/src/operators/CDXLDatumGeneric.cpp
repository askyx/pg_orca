//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumGeneric.cpp
//
//	@doc:
//		Implementation of DXL datum of type generic
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumGeneric.h"

#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumGeneric::CDXLDatumGeneric
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumGeneric::CDXLDatumGeneric(CMemoryPool *mp, IMDId *mdid_type, INT type_modifier, BOOL is_null, BYTE *byte_array,
                                   ULONG length)
    : CDXLDatum(mp, mdid_type, type_modifier, is_null, length), m_byte_array(byte_array) {
  GPOS_ASSERT_IMP(m_is_null, (m_byte_array == nullptr) && (m_length == 0));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumGeneric::~CDXLDatumGeneric
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLDatumGeneric::~CDXLDatumGeneric() {
  GPOS_DELETE_ARRAY(m_byte_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumGeneric::GetByteArray
//
//	@doc:
//		Returns the bytearray of the datum
//
//---------------------------------------------------------------------------
const BYTE *CDXLDatumGeneric::GetByteArray() const {
  return m_byte_array;
}
