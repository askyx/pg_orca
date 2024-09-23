//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryAny.cpp
//
//	@doc:
//		Implementation of subquery ANY
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarSubqueryAny.h"

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAny::CDXLScalarSubqueryAny
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryAny::CDXLScalarSubqueryAny(CMemoryPool *mp, IMDId *scalar_op_mdid, CMDName *scalar_op_mdname,
                                             uint32_t colid)
    : CDXLScalarSubqueryQuantified(mp, scalar_op_mdid, scalar_op_mdname, colid) {}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAny::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid CDXLScalarSubqueryAny::GetDXLOperator() const {
  return EdxlopScalarSubqueryAny;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAny::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *CDXLScalarSubqueryAny::GetOpNameStr() const {
  return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubqueryAny);
}

// EOF
