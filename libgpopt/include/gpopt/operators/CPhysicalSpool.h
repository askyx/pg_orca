//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalSpool.h
//
//	@doc:
//		Spool operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalSpool_H
#define GPOPT_CPhysicalSpool_H

#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalSpool
//
//	@doc:
//		Spool operator
//
//---------------------------------------------------------------------------
class CPhysicalSpool : public CPhysical {
 private:
  // true if spool is blocking - consume all input the tuples, before yielding an
  // output tuple false if spool is streaming - stream the tuples as they come
  BOOL m_eager;

 public:
  CPhysicalSpool(const CPhysicalSpool &) = delete;

  // ctor
  explicit CPhysicalSpool(CMemoryPool *mp, BOOL eager);

  // dtor
  ~CPhysicalSpool() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalSpool; }

  const CHAR *SzId() const override { return "CPhysicalSpool"; }

  // match function
  BOOL Matches(COperator *) const override;

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
  BOOL FPassThruStats() const override { return true; }

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // conversion function
  static CPhysicalSpool *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalSpool == pop->Eopid());

    return dynamic_cast<CPhysicalSpool *>(pop);
  }

  BOOL FEager() const { return m_eager; }

  ULONG HashValue() const override;

  BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                     COptimizationContextArray *pdrgpocChild) const override;

  IOstream &OsPrint(IOstream &os) const override;

};  // class CPhysicalSpool

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalSpool_H

// EOF
