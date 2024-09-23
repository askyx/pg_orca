//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CPhysicalUnionAll_H
#define GPOPT_CPhysicalUnionAll_H

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt {
class CPhysicalUnionAll : public CPhysical {
 private:
  // output column array
  CColRefArray *const m_pdrgpcrOutput;

  // input column array
  CColRef2dArray *const m_pdrgpdrgpcrInput;

  // set representation of input columns
  CColRefSetArray *m_pdrgpcrsInput;

  // map given array of scalar ident expressions to positions of UnionAll input columns in the given child;
  ULongPtrArray *PdrgpulMap(CMemoryPool *mp, CExpressionArray *pdrgpexpr, uint32_t child_index) const;

  // map given ColRefSet, expressed in terms of outputs,
  // into an equivalent ColRefSet, expressed in terms
  // of input number n
  CColRefSet *MapOutputColRefsToInput(CMemoryPool *mp, CColRefSet *out_col_refs, uint32_t child_index);

 public:
  CPhysicalUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput, CColRef2dArray *pdrgpdrgpcrInput);

  ~CPhysicalUnionAll() override;

  // match function
  bool Matches(COperator *) const override;

  // ident accessors
  EOperatorId Eopid() const override = 0;

  const char *SzId() const override = 0;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override;

  // accessor of output column array
  CColRefArray *PdrgpcrOutput() const;

  // accessor of input column array
  CColRef2dArray *PdrgpdrgpcrInput() const;

  // return true if operator passes through stats obtained from children,
  // this is used when computing stats during costing
  bool FPassThruStats() const override;

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

  // conversion function
  static CPhysicalUnionAll *PopConvert(COperator *pop);

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
};
}  // namespace gpopt

#endif
