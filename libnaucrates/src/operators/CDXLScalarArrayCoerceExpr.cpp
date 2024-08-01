//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarArrayCoerceExpr.cpp
//
//	@doc:
//		Implementation of DXL scalar array coerce expr
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayCoerceExpr.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::CDXLScalarArrayCoerceExpr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArrayCoerceExpr::CDXLScalarArrayCoerceExpr(CMemoryPool *mp, IMDId *result_type_mdid, INT type_modifier,
                                                     EdxlCoercionForm coerce_format, INT location)
    : CDXLScalarCoerceBase(mp, result_type_mdid, type_modifier, coerce_format, location) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarArrayCoerceExpr::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarArrayCoerceExpr);
}
