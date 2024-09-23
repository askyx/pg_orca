//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom
//
//	@filename:
//		CXformInnerJoinCommutativity.cpp
//
//	@doc:
//		Implementation of inner join commutativity transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInnerJoinCommutativity.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::CXformInnerJoinCommutativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoinCommutativity::CXformInnerJoinCommutativity(CMemoryPool *mp)
    : CXformExploration(
          // pattern
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
                                   GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
                                   GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
                                   GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)))  // predicate
      ) {}

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::FCompatible
//
//	@doc:
//		Compatibility function for join commutativity
//
//---------------------------------------------------------------------------
BOOL CXformInnerJoinCommutativity::FCompatible(CXform::EXformId exfid) {
  BOOL fCompatible = true;

  switch (exfid) {
    case CXform::ExfInnerJoinCommutativity:
      fCompatible = false;
      break;
    default:
      fCompatible = true;
  }

  return fCompatible;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformInnerJoinCommutativity::Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const {
  GPOS_ASSERT(nullptr != pxfctxt);
  GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
  GPOS_ASSERT(FCheckPattern(pexpr));

  CMemoryPool *mp = pxfctxt->Pmp();

  // extract components
  CExpression *pexprLeft = (*pexpr)[0];
  CExpression *pexprRight = (*pexpr)[1];
  CExpression *pexprScalar = (*pexpr)[2];

  // addref children
  pexprLeft->AddRef();
  pexprRight->AddRef();
  pexprScalar->AddRef();

  // assemble transformed expression
  CExpression *pexprAlt = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, pexprRight, pexprLeft, pexprScalar);

  // add alternative to transformation result
  pxfres->Add(pexprAlt);
}

// EOF
