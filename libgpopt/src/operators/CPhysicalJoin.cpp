//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalJoin.cpp
//
//	@doc:
//		Implementation of physical join operator
//---------------------------------------------------------------------------

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"
#include "gpopt/operators/CPhysicalLeftOuterIndexNLJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarIsDistinctFrom.h"
#include "gpos/base.h"
#include "naucrates/md/IMDScalarOp.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::CPhysicalJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalJoin::CPhysicalJoin(CMemoryPool *mp, CXform::EXformId origin_xform)
    : CPhysical(mp), m_origin_xform(origin_xform) {}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
bool CPhysicalJoin::Matches(COperator *pop) const {
  return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PosPropagateToOuter
//
//	@doc:
//		Helper for propagating required sort order to outer child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalJoin::PosPropagateToOuter(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired) {
  // propagate the order requirement to the outer child only if all the columns
  // specified by the order requirement come from the outer child
  CColRefSet *pcrs = posRequired->PcrsUsed(mp);
  bool fOuterSortCols = exprhdl.DeriveOutputColumns(0)->ContainsAll(pcrs);
  pcrs->Release();
  if (fOuterSortCols) {
    return PosPassThru(mp, exprhdl, posRequired, 0 /*child_index*/);
  }

  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PcrsRequired
//
//	@doc:
//		Compute required output columns of n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalJoin::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                        uint32_t child_index,
                                        CDrvdPropArray *,  // pdrgpdpCtxt
                                        uint32_t           // ulOptReq
) {
  GPOS_ASSERT(child_index < 2 && "Required properties can only be computed on the relational child");

  return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, 2 /*ulScalarIndex*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalJoin::PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter, uint32_t child_index,
                                     CDrvdPropArray *pdrgpdpCtxt,
                                     uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(2 > child_index);

  return PcterNAry(mp, exprhdl, pcter, child_index, pdrgpdpCtxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FProvidesReqdCols
//
//	@doc:
//		Helper for checking if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalJoin::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                      uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);
  GPOS_ASSERT(3 == exprhdl.Arity());

  // union columns from relational children
  CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
  uint32_t arity = exprhdl.Arity();
  for (uint32_t i = 0; i < arity - 1; i++) {
    CColRefSet *pcrsChild = exprhdl.DeriveOutputColumns(i);
    pcrs->Union(pcrsChild);
  }

  bool fProvidesCols = pcrs->ContainsAll(pcrsRequired);
  pcrs->Release();

  return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FSortColsInOuterChild
//
//	@doc:
//		Helper for checking if required sort columns come from outer child
//
//----------------------------------------------------------------------------
bool CPhysicalJoin::FSortColsInOuterChild(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *pos) {
  GPOS_ASSERT(nullptr != pos);

  CColRefSet *pcrsSort = pos->PcrsUsed(mp);
  CColRefSet *pcrsOuterChild = exprhdl.DeriveOutputColumns(0 /*child_index*/);
  bool fSortColsInOuter = pcrsOuterChild->ContainsAll(pcrsSort);
  pcrsSort->Release();

  return fSortColsInOuter;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FOuterProvidesReqdCols
//
//	@doc:
//		Helper for checking if the outer input of a binary join operator
//		includes the required columns
//
//---------------------------------------------------------------------------
bool CPhysicalJoin::FOuterProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired) {
  GPOS_ASSERT(nullptr != pcrsRequired);
  GPOS_ASSERT(3 == exprhdl.Arity() && "expected binary join");

  CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns(0 /*child_index*/);

  return pcrsOutput->ContainsAll(pcrsRequired);
}

// Do each of the given predicate children use columns from a different
// join child? For this method to return true, either:
//   - pexprPredInner uses columns from pexprInner and pexprPredOuter uses
//     columns from pexprOuter; or
//   - pexprPredInner uses columns from pexprOuter and pexprPredOuter uses
//     columns from pexprInner
bool CPhysicalJoin::FPredKeysSeparated(CExpression *pexprInner, CExpression *pexprOuter, CExpression *pexprPredInner,
                                       CExpression *pexprPredOuter) {
  GPOS_ASSERT(nullptr != pexprOuter);
  GPOS_ASSERT(nullptr != pexprInner);
  GPOS_ASSERT(nullptr != pexprPredOuter);
  GPOS_ASSERT(nullptr != pexprPredInner);

  CColRefSet *pcrsUsedPredOuter = pexprPredOuter->DeriveUsedColumns();
  CColRefSet *pcrsUsedPredInner = pexprPredInner->DeriveUsedColumns();

  CColRefSet *outer_refs = pexprOuter->DeriveOutputColumns();
  CColRefSet *pcrsInner = pexprInner->DeriveOutputColumns();

  // make sure that each predicate child uses columns from a different join child
  // in order to reject predicates of the form 'X Join Y on f(X.a, Y.b) = 5'
  bool fPredOuterUsesJoinOuterChild = (0 < pcrsUsedPredOuter->Size()) && outer_refs->ContainsAll(pcrsUsedPredOuter);
  bool fPredOuterUsesJoinInnerChild = (0 < pcrsUsedPredOuter->Size()) && pcrsInner->ContainsAll(pcrsUsedPredOuter);
  bool fPredInnerUsesJoinOuterChild = (0 < pcrsUsedPredInner->Size()) && outer_refs->ContainsAll(pcrsUsedPredInner);
  bool fPredInnerUsesJoinInnerChild = (0 < pcrsUsedPredInner->Size()) && pcrsInner->ContainsAll(pcrsUsedPredInner);

  return (fPredOuterUsesJoinOuterChild && fPredInnerUsesJoinInnerChild) ||
         (fPredOuterUsesJoinInnerChild && fPredInnerUsesJoinOuterChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FHashJoinCompatible
//
//	@doc:
//		Is given predicate hash-join compatible?
//		the function returns True if predicate is of the form (Equality) or
//		(Is Not Distinct From), both operands are hashjoinable, AND the predicate
//		uses columns from both join children
//
//---------------------------------------------------------------------------
bool CPhysicalJoin::FHashJoinCompatible(CExpression *pexprPred,   // predicate in question
                                        CExpression *pexprOuter,  // outer child of the join
                                        CExpression *pexprInner   // inner child of the join
) {
  GPOS_ASSERT(nullptr != pexprPred);
  GPOS_ASSERT(nullptr != pexprOuter);
  GPOS_ASSERT(nullptr != pexprInner);
  GPOS_ASSERT(pexprOuter != pexprInner);

  CExpression *pexprPredOuter = nullptr;
  CExpression *pexprPredInner = nullptr;
  if (CPredicateUtils::IsEqualityOp(pexprPred)) {
    pexprPredOuter = (*pexprPred)[0];
    pexprPredInner = (*pexprPred)[1];
  } else if (CPredicateUtils::FINDF(pexprPred)) {
    CExpression *pexpr = (*pexprPred)[0];
    pexprPredOuter = (*pexpr)[0];
    pexprPredInner = (*pexpr)[1];
  } else {
    return false;
  }

  IMDId *pmdidTypeOuter = CScalar::PopConvert(pexprPredOuter->Pop())->MdidType();
  IMDId *pmdidTypeInner = CScalar::PopConvert(pexprPredInner->Pop())->MdidType();

  CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

  // This check is mainly added for RANGE TYPES; RANGE's are treated as
  // containers and whether a range type can support hashing is decided
  // based on hashing support of its subtype
  if (COperator::EopScalarCast == pexprPredOuter->Pop()->Eopid()) {
    pmdidTypeOuter = CScalar::PopConvert((*pexprPredOuter)[0]->Pop())->MdidType();
  }
  if (COperator::EopScalarCast == pexprPredInner->Pop()->Eopid()) {
    pmdidTypeInner = CScalar::PopConvert((*pexprPredInner)[0]->Pop())->MdidType();
  }

  if (md_accessor->RetrieveType(pmdidTypeOuter)->IsHashable() &&
      md_accessor->RetrieveType(pmdidTypeInner)->IsHashable()) {
    return FPredKeysSeparated(pexprInner, pexprOuter, pexprPredInner, pexprPredOuter);
  }

  return false;
}

bool CPhysicalJoin::FMergeJoinCompatible(CExpression *pexprPred,   // predicate in question
                                         CExpression *pexprOuter,  // outer child of the join
                                         CExpression *pexprInner   // inner child of the join
) {
  GPOS_ASSERT(nullptr != pexprPred);
  GPOS_ASSERT(nullptr != pexprOuter);
  GPOS_ASSERT(nullptr != pexprInner);
  GPOS_ASSERT(pexprOuter != pexprInner);

  CExpression *pexprPredOuter = nullptr;
  CExpression *pexprPredInner = nullptr;

  // Only merge join between ScalarIdents of the same types is currently supported
  if (CPredicateUtils::FEqIdentsOfSameType(pexprPred)) {
    pexprPredOuter = (*pexprPred)[0];
    pexprPredInner = (*pexprPred)[1];
    GPOS_ASSERT(CUtils::FScalarIdent(pexprPredOuter));
    GPOS_ASSERT(CUtils::FScalarIdent(pexprPredInner));
  } else {
    return false;
  }

  IMDId *pmdidTypeOuter = CScalar::PopConvert(pexprPredOuter->Pop())->MdidType();
  IMDId *pmdidTypeInner = CScalar::PopConvert(pexprPredInner->Pop())->MdidType();

  CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

  // MJ sends a distribution request for merge clauses on both sides, they
  // must, therefore, be hashable and merge joinable.

  if (mda->RetrieveType(pmdidTypeOuter)->IsHashable() && mda->RetrieveType(pmdidTypeInner)->IsHashable() &&
      mda->RetrieveType(pmdidTypeOuter)->IsMergeJoinable() && mda->RetrieveType(pmdidTypeInner)->IsMergeJoinable()) {
    return FPredKeysSeparated(pexprInner, pexprOuter, pexprPredInner, pexprPredOuter);
  }

  return false;
}

// Check for equality and INDFs in the predicates, and also aligns the expressions inner and outer keys with the
// predicates For example foo (a int, b int) and bar (c int, d int), will need to be aligned properly if the predicate
// is d = a)
void CPhysicalJoin::AlignJoinKeyOuterInner(CExpression *pexprPred, CExpression *pexprOuter,
#ifdef GPOS_DEBUG
                                           CExpression *pexprInner,
#else
                                           CExpression *,
#endif  // GPOS_DEBUG
                                           CExpression **ppexprKeyOuter, CExpression **ppexprKeyInner,
                                           IMDId **mdid_scop) {
  // we should not be here if there are outer references
  GPOS_ASSERT(nullptr != ppexprKeyOuter);
  GPOS_ASSERT(nullptr != ppexprKeyInner);

  CExpression *pexprPredOuter = nullptr;
  CExpression *pexprPredInner = nullptr;

  // extract left & right children from pexprPred for all supported ops
  if (CPredicateUtils::IsEqualityOp(pexprPred)) {
    pexprPredOuter = (*pexprPred)[0];
    pexprPredInner = (*pexprPred)[1];
    *mdid_scop = CScalarCmp::PopConvert(pexprPred->Pop())->MdIdOp();
  } else if (CPredicateUtils::FINDF(pexprPred)) {
    CExpression *pexpr = (*pexprPred)[0];
    pexprPredOuter = (*pexpr)[0];
    pexprPredInner = (*pexpr)[1];
    *mdid_scop = CScalarIsDistinctFrom::PopConvert(pexpr->Pop())->MdIdOp();
  } else {
    GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp,
               GPOS_WSZ_LIT("Invalid join expression in AlignJoinKeyOuterInner"));
  }

  GPOS_ASSERT(nullptr != pexprPredOuter);
  GPOS_ASSERT(nullptr != pexprPredInner);

  CColRefSet *pcrsOuter = pexprOuter->DeriveOutputColumns();
  CColRefSet *pcrsPredOuter = pexprPredOuter->DeriveUsedColumns();

#ifdef GPOS_DEBUG
  CColRefSet *pcrsInner = pexprInner->DeriveOutputColumns();
  CColRefSet *pcrsPredInner = pexprPredInner->DeriveUsedColumns();
#endif  // GPOS_DEBUG

  CExpression *pexprOuterKeyWithoutBCC = CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredOuter);
  CExpression *pexprInnerKeyWithoutBCC = CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredInner);

  if (pcrsOuter->ContainsAll(pcrsPredOuter)) {
    *ppexprKeyOuter = pexprOuterKeyWithoutBCC;
    GPOS_ASSERT(pcrsInner->ContainsAll(pcrsPredInner));

    *ppexprKeyInner = pexprInnerKeyWithoutBCC;
  } else {
    GPOS_ASSERT(pcrsOuter->ContainsAll(pcrsPredInner));
    *ppexprKeyOuter = pexprInnerKeyWithoutBCC;

    GPOS_ASSERT(pcrsInner->ContainsAll(pcrsPredOuter));
    *ppexprKeyInner = pexprOuterKeyWithoutBCC;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FProcessingChildWithPartConsumer
//
//	@doc:
//		Check whether the child being processed is the child that has the part consumer
//
//---------------------------------------------------------------------------
bool CPhysicalJoin::FProcessingChildWithPartConsumer(bool fOuterPartConsumerTest, uint32_t ulChildIndexToTestFirst,
                                                     uint32_t ulChildIndexToTestSecond, uint32_t child_index) {
  return (fOuterPartConsumerTest && ulChildIndexToTestFirst == child_index) ||
         (!fOuterPartConsumerTest && ulChildIndexToTestSecond == child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FFirstChildToOptimize
//
//	@doc:
//		Helper to check if given child index correspond to first
//		child to be optimized
//
//---------------------------------------------------------------------------
bool CPhysicalJoin::FFirstChildToOptimize(uint32_t child_index) const {
  GPOS_ASSERT(2 > child_index);

  EChildExecOrder eceo = Eceo();

  return (EceoLeftToRight == eceo && 0 == child_index) || (EceoRightToLeft == eceo && 1 == child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::UlDistrRequestsForCorrelatedJoin
//
//	@doc:
//		Return number of distribution requests for correlated join
//
//---------------------------------------------------------------------------
uint32_t CPhysicalJoin::UlDistrRequestsForCorrelatedJoin() {
  // Correlated Join creates two distribution requests to enforce distribution of its children:
  // Req(0): If incoming distribution is (Strict) Singleton, pass it through to all children
  //			to comply with correlated execution requirements
  // Req(1): Request outer child for ANY distribution, and then match it on the inner child

  return 2;
}
