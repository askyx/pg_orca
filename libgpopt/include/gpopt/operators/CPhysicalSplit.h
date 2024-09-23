//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSplit.h
//
//	@doc:
//		Physical split operator
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalSplit_H
#define GPOS_CPhysicalSplit_H

#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declaration

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalSplit
//
//	@doc:
//		Physical split operator
//
//---------------------------------------------------------------------------
class CPhysicalSplit : public CPhysical {
 private:
  // deletion columns
  CColRefArray *m_pdrgpcrDelete;

  // insertion columns
  CColRefArray *m_pdrgpcrInsert;

  // ctid column
  CColRef *m_pcrCtid;

  // segmentid column
  CColRef *m_pcrSegmentId;

  // action column
  CColRef *m_pcrAction;

  // required columns by local members
  CColRefSet *m_pcrsRequiredLocal;

 public:
  CPhysicalSplit(const CPhysicalSplit &) = delete;

  // ctor
  CPhysicalSplit(CMemoryPool *mp, CColRefArray *pdrgpcrDelete, CColRefArray *pdrgpcrInsert, CColRef *pcrCtid,
                 CColRef *pcrSegmentId, CColRef *pcrAction);

  // dtor
  ~CPhysicalSplit() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalSplit; }

  // return a string for operator name
  const CHAR *SzId() const override { return "CPhysicalSplit"; }

  // action column
  CColRef *PcrAction() const { return m_pcrAction; }

  // ctid column
  CColRef *PcrCtid() const { return m_pcrCtid; }

  // segmentid column
  CColRef *PcrSegmentId() const { return m_pcrSegmentId; }

  // deletion columns
  CColRefArray *PdrgpcrDelete() const { return m_pdrgpcrDelete; }

  // insertion columns
  CColRefArray *PdrgpcrInsert() const { return m_pdrgpcrInsert; }

  // match function
  BOOL Matches(COperator *pop) const override;

  // hash function
  ULONG HashValue() const override;

  // sensitivity to order of inputs
  BOOL FInputOrderSensitive() const override { return false; }

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required sort columns of the n-th child
  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired, ULONG child_index,
                          CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

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

  // compute required output columns of the n-th child
  CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG child_index,
                           CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) override;

  // compute required ctes of the n-th child
  CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter, ULONG child_index,
                        CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

  // check if required columns are included in output columns
  BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const override;

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return true if operator passes through stats obtained from children,
  // this is used when computing stats during costing
  BOOL FPassThruStats() const override { return false; }

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // conversion function
  static CPhysicalSplit *PopConvert(COperator *pop) {
    GPOS_ASSERT(COperator::EopPhysicalSplit == pop->Eopid());

    return dynamic_cast<CPhysicalSplit *>(pop);
  }

  // debug print
  IOstream &OsPrint(IOstream &os) const override;

};  // class CPhysicalSplit
}  // namespace gpopt

#endif  // !GPOS_CPhysicalSplit_H

// EOF
