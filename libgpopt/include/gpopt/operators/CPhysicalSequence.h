//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSequence.h
//
//	@doc:
//		Physical sequence operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalSequence_H
#define GPOPT_CPhysicalSequence_H

#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalSequence
//
//	@doc:
//		Physical sequence operator
//
//---------------------------------------------------------------------------
class CPhysicalSequence : public CPhysical {
 private:
  // empty column set to be requested from all children except last child
  CColRefSet *m_pcrsEmpty;

 public:
  CPhysicalSequence(const CPhysicalSequence &) = delete;

  // ctor
  explicit CPhysicalSequence(CMemoryPool *mp);

  // dtor
  ~CPhysicalSequence() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalSequence; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalSequence"; }

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

  // compute required sort columns of the n-th child
  COrderSpec *PosRequired(CMemoryPool *,        // mp
                          CExpressionHandle &,  // exprhdl
                          COrderSpec *,         // posRequired
                          uint32_t,             // child_index
                          CDrvdPropArray *,     // pdrgpdpCtxt
                          uint32_t              // ulOptReq
  ) const override;

  // check if required columns are included in output columns
  bool FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t ulOptReq) const override;

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  // derive sort order from the last child
  COrderSpec *PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return order property enforcing type for this operator
  CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

  // return true if operator passes through stats obtained from children,
  // this is used when computing stats during costing
  bool FPassThruStats() const override { return false; }

};  // class CPhysicalSequence

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalSequence_H

// EOF
