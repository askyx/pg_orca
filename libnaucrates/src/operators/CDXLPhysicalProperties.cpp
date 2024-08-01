//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalProperties.cpp
//
//	@doc:
//		Implementation of DXL physical operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalProperties.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalProperties::CDXLPhysicalProperties
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalProperties::CDXLPhysicalProperties(CDXLOperatorCost *cost) : CDXLProperties(), m_operator_cost_dxl(cost) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalProperties::~CDXLPhysicalProperties
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalProperties::~CDXLPhysicalProperties() {
  CRefCount::SafeRelease(m_operator_cost_dxl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalProperties::MakeDXLOperatorCost
//
//	@doc:
//		Return cost of operator
//
//---------------------------------------------------------------------------
CDXLOperatorCost *CDXLPhysicalProperties::GetDXLOperatorCost() const {
  return m_operator_cost_dxl;
}

// EOF
