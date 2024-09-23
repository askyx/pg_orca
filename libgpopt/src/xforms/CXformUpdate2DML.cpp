//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformUpdate2DML.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformUpdate2DML.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalUpdate.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CScalarDMLAction.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpos/base.h"
#include "naucrates/md/IMDTypeInt4.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::CXformUpdate2DML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformUpdate2DML::CXformUpdate2DML(CMemoryPool *mp)
    : CXformExploration(
          // pattern
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalUpdate(mp),
                                   GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)))) {}

//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformUpdate2DML::Exfp(CExpressionHandle &  // exprhdl
) const {
  return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformUpdate2DML::Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const {
  GPOS_ASSERT(nullptr != pxfctxt);
  GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
  GPOS_ASSERT(FCheckPattern(pexpr));

  CLogicalUpdate *popUpdate = CLogicalUpdate::PopConvert(pexpr->Pop());
  CMemoryPool *mp = pxfctxt->Pmp();

  // extract components for alternative

  CTableDescriptor *ptabdesc = popUpdate->Ptabdesc();
  CColRefArray *pdrgpcrInsert = popUpdate->PdrgpcrInsert();
  CColRef *pcrCtid = popUpdate->PcrCtid();
  CColRef *pcrSegmentId = popUpdate->PcrSegmentId();

  // child of update operator
  CExpression *pexprChild = (*pexpr)[0];
  pexprChild->AddRef();
  // generate the action column and split operator
  COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
  CMDAccessor *md_accessor = poctxt->Pmda();
  CColumnFactory *col_factory = poctxt->Pcf();

  const IMDType *pmdtype = md_accessor->PtMDType<IMDTypeInt4>();
  CColRef *pcrAction = col_factory->PcrCreate(pmdtype, default_type_modifier);

  CExpression *pexprSplit = nullptr;

  pexprSplit = pexprChild;

  // add assert checking that no NULL values are inserted for nullable columns or no check constraints are violated
  CExpression *pexprProject;

  pexprProject = pexprSplit;

  CExpression *pexprDML = nullptr;
  // create logical DML
  ptabdesc->AddRef();

  pdrgpcrInsert->AddRef();
  pexprDML =
      GPOS_NEW(mp) CExpression(mp,
                               GPOS_NEW(mp) CLogicalDML(mp, CLogicalDML::EdmlUpdate, ptabdesc, pdrgpcrInsert,
                                                        GPOS_NEW(mp) CBitSet(mp), pcrAction, pcrCtid, pcrSegmentId),
                               pexprProject);

  // TODO:  - Oct 30, 2012; detect and handle AFTER triggers on update

  pxfres->Add(pexprDML);
}

// EOF
