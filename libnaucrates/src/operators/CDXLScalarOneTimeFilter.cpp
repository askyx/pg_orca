//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarOneTimeFilter.cpp
//
//	@doc:
//		Implementation of DXL physical one-time filter operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarOneTimeFilter.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarOneTimeFilter::CDXLScalarOneTimeFilter
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarOneTimeFilter::CDXLScalarOneTimeFilter(CMemoryPool *mp) : CDXLScalarFilter(mp) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarOneTimeFilter::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarOneTimeFilter::GetDXLOperator() const {
  return EdxlopScalarOneTimeFilter;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarOneTimeFilter::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarOneTimeFilter::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarOneTimeFilter);
}
