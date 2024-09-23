//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalConstTableGet.h
//
//	@doc:
//		Physical const table get
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalConstTableGet_H
#define GPOPT_CPhysicalConstTableGet_H

#include "gpopt/operators/CLogicalConstTableGet.h"
#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalConstTableGet
//
//	@doc:
//		Physical const table get operator
//
//---------------------------------------------------------------------------
class CPhysicalConstTableGet : public CPhysical {
 private:
  // array of column descriptors: the schema of the const table
  CColumnDescriptorArray *m_pdrgpcoldesc;

  // array of datum arrays
  IDatum2dArray *m_pdrgpdrgpdatum;

  // output columns
  CColRefArray *m_pdrgpcrOutput;

 public:
  CPhysicalConstTableGet(const CPhysicalConstTableGet &) = delete;

  // ctor
  CPhysicalConstTableGet(CMemoryPool *mp, CColumnDescriptorArray *pdrgpcoldesc, IDatum2dArray *pdrgpdrgpconst,
                         CColRefArray *pdrgpcrOutput);

  // dtor
  ~CPhysicalConstTableGet() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalConstTableGet; }

  const char *SzId() const override { return "CPhysicalConstTableGet"; }

  // col descr accessor
  CColumnDescriptorArray *Pdrgpcoldesc() const { return m_pdrgpcoldesc; }

  // const table values accessor
  IDatum2dArray *Pdrgpdrgpdatum() const { return m_pdrgpdrgpdatum; }

  // output columns accessors
  CColRefArray *PdrgpcrOutput() const { return m_pdrgpcrOutput; }

  // match function
  bool Matches(COperator *) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override {
    GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
    return false;
  }

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
  static CPhysicalConstTableGet *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalConstTableGet == pop->Eopid());

    return dynamic_cast<CPhysicalConstTableGet *>(pop);
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

};  // class CPhysicalConstTableGet

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalConstTableGet_H

// EOF
