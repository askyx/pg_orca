//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalStreamAggDeduplicate.h
//
//	@doc:
//		Sort-based stream Aggregate operator for deduplicating join outputs
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalStreamAggDeduplicate_H
#define GPOS_CPhysicalStreamAggDeduplicate_H

#include "gpopt/operators/CPhysicalStreamAgg.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalStreamAggDeduplicate
//
//	@doc:
//		Sort-based aggregate operator for deduplicating join outputs
//
//---------------------------------------------------------------------------
class CPhysicalStreamAggDeduplicate : public CPhysicalStreamAgg {
 private:
  // array of keys from the join's child
  CColRefArray *m_pdrgpcrKeys;

 public:
  CPhysicalStreamAggDeduplicate(const CPhysicalStreamAggDeduplicate &) = delete;

  // ctor
  CPhysicalStreamAggDeduplicate(CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal,
                                COperator::EGbAggType egbaggtype, CColRefArray *pdrgpcrKeys, bool fGeneratesDuplicates,
                                bool fMultiStage, bool isAggFromSplitDQA, CLogicalGbAgg::EAggStage aggStage,
                                bool should_enforce_distribution);

  // dtor
  ~CPhysicalStreamAggDeduplicate() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalStreamAggDeduplicate; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalStreamAggDeduplicate"; }

  // array of keys from the join's child
  CColRefArray *PdrgpcrKeys() const { return m_pdrgpcrKeys; }

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required output columns of the n-th child
  CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t child_index,
                           CDrvdPropArray *,  // pdrgpdpCtxt,
                           uint32_t           // ulOptReq
                           ) override {
    return PcrsRequiredAgg(mp, exprhdl, pcrsRequired, child_index, m_pdrgpcrKeys);
  }

  // compute required sort columns of the n-th child
  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired, uint32_t child_index,
                          CDrvdPropArray *,  // pdrgpdpCtxt,
                          uint32_t           // ulOptReq
  ) const override {
    return PosRequiredStreamAgg(mp, exprhdl, posRequired, child_index, m_pdrgpcrKeys);
  }

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // debug print
  IOstream &OsPrint(IOstream &os) const override;

  // conversion function
  static CPhysicalStreamAggDeduplicate *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalStreamAggDeduplicate == pop->Eopid());

    return dynamic_cast<CPhysicalStreamAggDeduplicate *>(pop);
  }

};  // class CPhysicalStreamAggDeduplicate

}  // namespace gpopt

#endif  // !GPOS_CPhysicalStreamAggDeduplicate_H

// EOF
