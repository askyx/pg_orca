//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalCTEConsumer.h
//
//	@doc:
//		Physical CTE consumer operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCTEConsumer_H
#define GPOPT_CPhysicalCTEConsumer_H

#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalCTEConsumer
//
//	@doc:
//		CTE consumer operator
//
//---------------------------------------------------------------------------
class CPhysicalCTEConsumer : public CPhysical {
 private:
  // cte identifier
  uint32_t m_id;

  // cte columns
  CColRefArray *m_pdrgpcr;

  // hashmap for all the columns in the CTE expression
  UlongToColRefMap *m_phmulcr;

 public:
  CPhysicalCTEConsumer(const CPhysicalCTEConsumer &) = delete;

  // ctor
  CPhysicalCTEConsumer(CMemoryPool *mp, uint32_t id, CColRefArray *colref_array, UlongToColRefMap *colref_mapping);

  // dtor
  ~CPhysicalCTEConsumer() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalCTEConsumer; }

  const char *SzId() const override { return "CPhysicalCTEConsumer"; }

  // cte identifier
  uint32_t UlCTEId() const { return m_id; }

  // cte columns
  CColRefArray *Pdrgpcr() const { return m_pdrgpcr; }

  // column mapping
  UlongToColRefMap *Phmulcr() const { return m_phmulcr; }

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
  static CPhysicalCTEConsumer *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalCTEConsumer == pop->Eopid());

    return dynamic_cast<CPhysicalCTEConsumer *>(pop);
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

};  // class CPhysicalCTEConsumer

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalCTEConsumer_H

// EOF
