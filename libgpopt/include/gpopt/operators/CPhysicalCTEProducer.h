//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalCTEProducer.h
//
//	@doc:
//		Physical CTE producer operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCTEProducer_H
#define GPOPT_CPhysicalCTEProducer_H

#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalCTEProducer
//
//	@doc:
//		CTE producer operator
//
//---------------------------------------------------------------------------
class CPhysicalCTEProducer : public CPhysical {
 private:
  // cte identifier
  uint32_t m_id;

  // cte columns
  CColRefArray *m_pdrgpcr;

  // set representation of cte columns
  CColRefSet *m_pcrs;

 public:
  CPhysicalCTEProducer(const CPhysicalCTEProducer &) = delete;

  // ctor
  CPhysicalCTEProducer(CMemoryPool *mp, uint32_t id, CColRefArray *colref_array);

  // dtor
  ~CPhysicalCTEProducer() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalCTEProducer; }

  const char *SzId() const override { return "CPhysicalCTEProducer"; }

  // cte identifier
  uint32_t UlCTEId() const { return m_id; }

  // cte columns
  CColRefArray *Pdrgpcr() const { return m_pdrgpcr; }

  // operator specific hash function
  uint32_t HashValue() const override;

  // match function
  bool Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return false; }

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

  // derive cte map
  CCTEMap *PcmDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

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
  static CPhysicalCTEProducer *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalCTEProducer == pop->Eopid());

    return dynamic_cast<CPhysicalCTEProducer *>(pop);
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

};  // class CPhysicalCTEProducer

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalCTEProducer_H

// EOF
