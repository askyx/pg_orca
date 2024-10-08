//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementUnionAll.cpp
//
//	@doc:
//		Implementation of union all operator
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementUnionAll.h"

#include "gpopt/exception.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CPhysicalUnionAllFactory.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementUnionAll::CXformImplementUnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementUnionAll::CXformImplementUnionAll(CMemoryPool *mp)
    :  // pattern
      CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalUnionAll(mp), GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp)))) {}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementUnionAll::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformImplementUnionAll::Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const {
  GPOS_ASSERT(nullptr != pxfctxt);
  GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
  GPOS_ASSERT(FCheckPattern(pexpr));

  CMemoryPool *mp = pxfctxt->Pmp();

  // extract components
  CLogicalUnionAll *popUnionAll = CLogicalUnionAll::PopConvert(pexpr->Pop());
  CPhysicalUnionAllFactory factory(popUnionAll);

  CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
  const uint32_t arity = pexpr->Arity();

  for (uint32_t ul = 0; ul < arity; ul++) {
    CExpression *pexprChild = (*pexpr)[ul];
    pexprChild->AddRef();
    pdrgpexpr->Append(pexprChild);
  }

  CPhysicalUnionAll *popPhysicalSerialUnionAll = factory.PopPhysicalUnionAll(mp, false);

  // assemble serial union physical operator
  CExpression *pexprSerialUnionAll = GPOS_NEW(mp) CExpression(mp, popPhysicalSerialUnionAll, pdrgpexpr);

  // add serial union alternative to results
  pxfres->Add(pexprSerialUnionAll);

  // parallel union alternative to the result if the GUC is on
  bool fParallel = GPOS_FTRACE(EopttraceEnableParallelAppend);

  if (fParallel) {
    CPhysicalUnionAll *popPhysicalParallelUnionAll = factory.PopPhysicalUnionAll(mp, true);

    pdrgpexpr->AddRef();

    // assemble physical parallel operator
    CExpression *pexprParallelUnionAll = GPOS_NEW(mp) CExpression(mp, popPhysicalParallelUnionAll, pdrgpexpr);

    // add parallel union alternative to results
    pxfres->Add(pexprParallelUnionAll);
  }
}

// EOF
