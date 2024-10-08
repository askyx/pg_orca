//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalJoin.h
//
//	@doc:
//		Physical join base class
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalJoin_H
#define GPOPT_CPhysicalJoin_H

#include "gpopt/operators/CPhysical.h"
#include "gpopt/xforms/CXform.h"
#include "gpos/base.h"

namespace gpopt {

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalJoin
//
//	@doc:
//		Inner nested-loops join operator
//
//---------------------------------------------------------------------------
class CPhysicalJoin : public CPhysical {
 private:
  // private copy ctor
  CPhysicalJoin(const CPhysicalJoin &);

  // check whether the child being processed is the child that has the part consumer
  static bool FProcessingChildWithPartConsumer(bool fOuterPartConsumerTest, uint32_t ulChildIndexToTestFirst,
                                               uint32_t ulChildIndexToTestSecond, uint32_t child_index);

  // xform that join order originated from
  CXform::EXformId m_origin_xform;

 protected:
  // ctor
  explicit CPhysicalJoin(CMemoryPool *mp, CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CPhysicalJoin() override = default;

  // helper to check if given child index correspond to first child to be optimized
  bool FFirstChildToOptimize(uint32_t child_index) const;

  // helper for propagating required sort order to outer child
  static COrderSpec *PosPropagateToOuter(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired);

  // helper for checking if required sort columns come from outer child
  static bool FSortColsInOuterChild(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *pos);

  // helper for checking if the outer input of a binary join operator
  // includes the required columns
  static bool FOuterProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired);

  // Do each of the given predicate children use columns from a different
  // join child?
  static bool FPredKeysSeparated(CExpression *pexprOuter, CExpression *pexprInner, CExpression *pexprPredOuter,
                                 CExpression *pexprPredInner);

 public:
  // match function
  bool Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return true; }

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required output columns of the n-th child
  CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t child_index,
                           CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) override;

  // compute required ctes of the n-th child
  CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter, uint32_t child_index,
                        CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  // check if required columns are included in output columns
  bool FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t ulOptReq) const override;

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  // derive sort order from outer child
  COrderSpec *PosDerive(CMemoryPool *,  // mp
                        CExpressionHandle &exprhdl) const override {
    return PosDerivePassThruOuter(exprhdl);
  }

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return true if operator passes through stats obtained from children,
  // this is used when computing stats during costing
  bool FPassThruStats() const override { return false; }

  CXform::EXformId OriginXform() { return m_origin_xform; }
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // is given predicate hash-join compatible
  static bool FHashJoinCompatible(CExpression *pexprPred, CExpression *pexprOuter, CExpression *pexprInner);

  // is given predicate merge-join compatible
  static bool FMergeJoinCompatible(CExpression *pexprPred, CExpression *pexprOuter, CExpression *pexprInner);

  // return number of distribution requests for correlated join
  static uint32_t UlDistrRequestsForCorrelatedJoin();

  static void AlignJoinKeyOuterInner(CExpression *pexprConjunct, CExpression *pexprOuter, CExpression *pexprInner,
                                     CExpression **ppexprKeyOuter, CExpression **ppexprKeyInner, IMDId **mdid_scop);

};  // class CPhysicalJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalJoin_H

// EOF
