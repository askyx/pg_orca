//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalFilter.h
//
//	@doc:
//		Filter operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalFilter_H
#define GPOPT_CPhysicalFilter_H

#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalFilter
//
//	@doc:
//		Filter operator
//
//---------------------------------------------------------------------------
class CPhysicalFilter : public CPhysical {
 private:
 public:
  CPhysicalFilter(const CPhysicalFilter &) = delete;

  // ctor
  explicit CPhysicalFilter(CMemoryPool *mp);

  // dtor
  ~CPhysicalFilter() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalFilter; }

  const CHAR *SzId() const override { return "CPhysicalFilter"; }

  // match function
  BOOL Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  BOOL FInputOrderSensitive() const override { return true; }

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required output columns of the n-th child
  CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG child_index,
                           CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) override;

  // compute required ctes of the n-th child
  CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter, ULONG child_index,
                        CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

  // compute required sort order of the n-th child
  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired, ULONG child_index,
                          CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

  // check if required columns are included in output columns
  BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const override;

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  // derive sort order
  COrderSpec *PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return order property enforcing type for this operator
  CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

  // return true if operator passes through stats obtained from children,
  // this is used when computing stats during costing
  BOOL FPassThruStats() const override { return false; }

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // conversion function
  static CPhysicalFilter *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalFilter == pop->Eopid());

    return dynamic_cast<CPhysicalFilter *>(pop);
  }

};  // class CPhysicalFilter

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalFilter_H

// EOF
