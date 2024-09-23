//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSequenceProject.h
//
//	@doc:
//		Physical Sequence Project operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalSequenceProject_H
#define GPOPT_CPhysicalSequenceProject_H

#include "gpopt/base/CWindowFrame.h"
#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declarations

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalSequenceProject
//
//	@doc:
//		Physical Sequence Project operator
//
//---------------------------------------------------------------------------
class CPhysicalSequenceProject : public CPhysical {
 private:
  // order specs of child window functions
  COrderSpecArray *m_pdrgpos;

  // frames of child window functions
  CWindowFrameArray *m_pdrgpwf;

  // order spec to request from child
  COrderSpec *m_pos;

  // required columns in order/frame specs
  CColRefSet *m_pcrsRequiredLocal;

  // create local order spec
  void CreateOrderSpec(CMemoryPool *mp);

  // compute local required columns
  void ComputeRequiredLocalColumns(CMemoryPool *mp);

 public:
  CPhysicalSequenceProject(const CPhysicalSequenceProject &) = delete;

  // ctor
  CPhysicalSequenceProject(CMemoryPool *mp, COrderSpecArray *pdrgpos, CWindowFrameArray *pdrgpwf);

  // dtor
  ~CPhysicalSequenceProject() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalSequenceProject; }

  // operator name
  const char *SzId() const override { return "CPhysicalSequenceProject"; }

  // order by keys
  COrderSpecArray *Pdrgpos() const { return m_pdrgpos; }

  // frame specifications
  CWindowFrameArray *Pdrgpwf() const { return m_pdrgpwf; }

  // match function
  bool Matches(COperator *pop) const override;

  // hashing function
  uint32_t HashValue() const override;

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
  IOstream &OsPrint(IOstream &os) const override;

  // conversion function
  static CPhysicalSequenceProject *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalSequenceProject == pop->Eopid());

    return dynamic_cast<CPhysicalSequenceProject *>(pop);
  }

};  // class CPhysicalSequenceProject

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalSequenceProject_H

// EOF
