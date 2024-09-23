//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalTVF.h
//
//	@doc:
//		Physical Table-valued function
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalTVF_H
#define GPOPT_CPhysicalTVF_H

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalTVF
//
//	@doc:
//		Physical Table-valued function
//
//---------------------------------------------------------------------------
class CPhysicalTVF : public CPhysical {
 private:
  // function mdid
  IMDId *m_func_mdid;

  // return type
  IMDId *m_return_type_mdid;

  // function name
  CWStringConst *m_pstr;

  // MD cache info
  const IMDFunction *m_pmdfunc;

  // array of column descriptors: the schema of the function result
  CColumnDescriptorArray *m_pdrgpcoldesc;

  // output columns
  CColRefSet *m_pcrsOutput;

 public:
  CPhysicalTVF(const CPhysicalTVF &) = delete;

  // ctor
  CPhysicalTVF(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type, CWStringConst *str,
               CColumnDescriptorArray *pdrgpcoldesc, CColRefSet *pcrsOutput);

  // dtor
  ~CPhysicalTVF() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalTVF; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalTVF"; }

  // function mdid
  IMDId *FuncMdId() const { return m_func_mdid; }

  // return type
  IMDId *ReturnTypeMdId() const { return m_return_type_mdid; }

  // function name
  const CWStringConst *Pstr() const { return m_pstr; }

  // col descr accessor
  CColumnDescriptorArray *Pdrgpcoldesc() const { return m_pdrgpcoldesc; }

  // accessors
  CColRefSet *DeriveOutputColumns() const { return m_pcrsOutput; }

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override;

  // match function
  bool Matches(COperator *pop) const override;

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

  // derive partition propagation
  CPartitionPropagationSpec *PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

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
  static CPhysicalTVF *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalTVF == pop->Eopid());

    return dynamic_cast<CPhysicalTVF *>(pop);
  }

};  // class CPhysicalTVF

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalTVF_H

// EOF
