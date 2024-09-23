//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalLimit.h
//
//	@doc:
//		Physical Limit operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLimit_H
#define GPOPT_CPhysicalLimit_H

#include "gpopt/base/COrderSpec.h"
#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalLimit
//
//	@doc:
//		Limit operator
//
//---------------------------------------------------------------------------
class CPhysicalLimit : public CPhysical {
 private:
  // order spec
  COrderSpec *m_pos;

  // global limit
  bool m_fGlobal;

  // does limit specify a number of rows?
  bool m_fHasCount;

  // this is a top limit right under a DML or CTAS operation
  bool m_top_limit_under_dml;

  // columns used by order spec
  CColRefSet *m_pcrsSort;

 public:
  CPhysicalLimit(const CPhysicalLimit &) = delete;

  // ctor
  CPhysicalLimit(CMemoryPool *mp, COrderSpec *pos, bool fGlobal, bool fHasCount, bool fTopLimitUnderDML);

  // dtor
  ~CPhysicalLimit() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalLimit; }

  const char *SzId() const override { return "CPhysicalLimit"; }

  // hash function
  uint32_t HashValue() const override {
    return gpos::CombineHashes(
        gpos::CombineHashes(COperator::HashValue(), m_pos->HashValue()),
        gpos::CombineHashes(gpos::HashValue<bool>(&m_fGlobal), gpos::HashValue<bool>(&m_fHasCount)));
  }

  // order spec
  COrderSpec *Pos() const { return m_pos; }

  // global limit
  bool FGlobal() const { return m_fGlobal; }

  // does limit specify a number of rows
  bool FHasCount() const { return m_fHasCount; }

  // must the limit be always kept
  bool IsTopLimitUnderDMLorCTAS() const { return m_top_limit_under_dml; }

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

  // print
  IOstream &OsPrint(IOstream &) const override;

  // conversion function
  static CPhysicalLimit *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalLimit == pop->Eopid());

    return dynamic_cast<CPhysicalLimit *>(pop);
  }

};  // class CPhysicalLimit

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalLimit_H

// EOF
