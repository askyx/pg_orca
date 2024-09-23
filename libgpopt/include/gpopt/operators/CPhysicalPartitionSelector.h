//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalPartitionSelector.h
//
//	@doc:
//		Physical partition selector operator used for property enforcement
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalPartitionSelector_H
#define GPOPT_CPhysicalPartitionSelector_H

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalPartitionSelector
//
//	@doc:
//		Physical partition selector operator used for property enforcement
//
//---------------------------------------------------------------------------
class CPhysicalPartitionSelector : public CPhysical {
 private:
  // Scan id
  uint32_t m_scan_id;

  // Unique id per Partition Selector created
  uint32_t m_selector_id;

  // mdid of partitioned table
  IMDId *m_mdid;

  // partition selection predicate
  CExpression *m_filter_expr;

 public:
  CPhysicalPartitionSelector(const CPhysicalPartitionSelector &) = delete;

  // ctor
  CPhysicalPartitionSelector(CMemoryPool *mp, uint32_t scan_id, uint32_t selector_id, IMDId *mdid,
                             CExpression *pexprScalar);

  // dtor
  ~CPhysicalPartitionSelector() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalPartitionSelector; }

  // operator name
  const char *SzId() const override { return "CPhysicalPartitionSelector"; }

  // scan id
  uint32_t ScanId() const { return m_scan_id; }

  uint32_t SelectorId() const { return m_selector_id; }

  // partitioned table mdid
  IMDId *MDId() const { return m_mdid; }

  // return the partition selection predicate
  CExpression *FilterExpr() const { return m_filter_expr; }

  // match function
  bool Matches(COperator *pop) const override;

  // hash function
  uint32_t HashValue() const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override {
    // operator has one child
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

  CPartitionPropagationSpec *PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                          CPartitionPropagationSpec *prsRequired, uint32_t child_index,
                                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  // check if required columns are included in output columns
  bool FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t ulOptReq) const override;

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  // derive sort order
  COrderSpec *PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

  CPartitionPropagationSpec *PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return order property enforcing type for this operator
  CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

  // return true if operator passes through stats obtained from children,
  // this is used when computing stats during costing
  bool FPassThruStats() const override { return true; }

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // conversion function
  static CPhysicalPartitionSelector *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalPartitionSelector == pop->Eopid());

    return dynamic_cast<CPhysicalPartitionSelector *>(pop);
  }

  // debug print
  IOstream &OsPrint(IOstream &os) const override;

};  // class CPhysicalPartitionSelector

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalPartitionSelector_H

// EOF
