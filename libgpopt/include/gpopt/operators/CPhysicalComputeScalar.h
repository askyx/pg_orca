//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalComputeScalar.h
//
//	@doc:
//		ComputeScalar operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalComputeScalar_H
#define GPOPT_CPhysicalComputeScalar_H

#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalComputeScalar
//
//	@doc:
//		ComputeScalar operator
//
//---------------------------------------------------------------------------
class CPhysicalComputeScalar : public CPhysical {
 private:
 public:
  CPhysicalComputeScalar(const CPhysicalComputeScalar &) = delete;

  // ctor
  explicit CPhysicalComputeScalar(CMemoryPool *mp);

  // dtor
  ~CPhysicalComputeScalar() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalComputeScalar; }

  const char *SzId() const override { return "CPhysicalComputeScalar"; }

  // match function
  bool Matches(COperator *) const override;

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

  // compute required sort order of the n-th child
  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired, uint32_t child_index,
                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  // check if required columns are included in output columns
  bool FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t ulOptReq) const override;

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
  bool FPassThruStats() const override { return false; }

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // conversion function
  static CPhysicalComputeScalar *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalComputeScalar == pop->Eopid());

    return dynamic_cast<CPhysicalComputeScalar *>(pop);
  }

};  // class CPhysicalComputeScalar

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalComputeScalar_H

// EOF
