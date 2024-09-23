//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalDML.h
//
//	@doc:
//		Physical DML operator
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalDML_H
#define GPOS_CPhysicalDML_H

#include "gpopt/operators/CLogicalDML.h"
#include "gpopt/operators/CPhysical.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declaration
class COptimizerConfig;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalDML
//
//	@doc:
//		Physical DML operator
//
//---------------------------------------------------------------------------
class CPhysicalDML : public CPhysical {
 private:
  // dml operator
  CLogicalDML::EDMLOperator m_edmlop;

  // table descriptor
  CTableDescriptor *m_ptabdesc;

  // array of source columns
  CColRefArray *m_pdrgpcrSource;

  // set of modified columns from the target table
  CBitSet *m_pbsModified;

  // action column
  CColRef *m_pcrAction;

  // ctid column
  CColRef *m_pcrCtid;

  // segmentid column
  CColRef *m_pcrSegmentId;

  // required order spec
  COrderSpec *m_pos;

  // required columns by local members
  CColRefSet *m_pcrsRequiredLocal;

  // Split Update
  BOOL m_fSplit;

  // compute required order spec
  COrderSpec *PosComputeRequired(CMemoryPool *mp, CTableDescriptor *ptabdesc);

  // compute local required columns
  void ComputeRequiredLocalColumns(CMemoryPool *mp);

 public:
  CPhysicalDML(const CPhysicalDML &) = delete;

  // ctor
  CPhysicalDML(CMemoryPool *mp, CLogicalDML::EDMLOperator edmlop, CTableDescriptor *ptabdesc,
               CColRefArray *pdrgpcrSource, CBitSet *pbsModified, CColRef *pcrAction, CColRef *pcrCtid,
               CColRef *pcrSegmentId);

  // dtor
  ~CPhysicalDML() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalDML; }

  // return a string for operator name
  const CHAR *SzId() const override { return "CPhysicalDML"; }

  // dml operator
  CLogicalDML::EDMLOperator Edmlop() const { return m_edmlop; }

  // table descriptor
  CTableDescriptor *Ptabdesc() const { return m_ptabdesc; }

  // action column
  CColRef *PcrAction() const { return m_pcrAction; }

  // ctid column
  CColRef *PcrCtid() const { return m_pcrCtid; }

  // segmentid column
  CColRef *PcrSegmentId() const { return m_pcrSegmentId; }

  // source columns
  virtual CColRefArray *PdrgpcrSource() const { return m_pdrgpcrSource; }

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
  static CPhysicalDML *PopConvert(COperator *pop) {
    GPOS_ASSERT(COperator::EopPhysicalDML == pop->Eopid());

    return dynamic_cast<CPhysicalDML *>(pop);
  }

  // debug print
  IOstream &OsPrint(IOstream &os) const override;

};  // class CPhysicalDML

}  // namespace gpopt

#endif  // !GPOS_CPhysicalDML_H

// EOF
