//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalHashAgg.h
//
//	@doc:
//		Hash Aggregate operator
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalHashAgg_H
#define GPOS_CPhysicalHashAgg_H

#include "gpopt/operators/CPhysicalAgg.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declaration

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalHashAgg
//
//	@doc:
//		Hash-based aggregate operator
//
//---------------------------------------------------------------------------
class CPhysicalHashAgg : public CPhysicalAgg {
 private:
 public:
  CPhysicalHashAgg(const CPhysicalHashAgg &) = delete;

  // ctor
  CPhysicalHashAgg(CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal,
                   COperator::EGbAggType egbaggtype, bool fGeneratesDuplicates, CColRefArray *pdrgpcrArgDQA,
                   bool fMultiStage, bool isAggFromSplitDQA, CLogicalGbAgg::EAggStage aggStage,
                   bool should_enforce_distribution = true
                   // should_enforce_distribution should be set to false if
                   // 'local' and 'global' splits don't need to have different
                   // distributions. This flag is set to false if the local
                   // aggregate has been created by CXformEagerAgg.
  );

  // dtor
  ~CPhysicalHashAgg() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalHashAgg; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalHashAgg"; }

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required sort columns of the n-th child
  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired, uint32_t child_index,
                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

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

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // conversion function
  static CPhysicalHashAgg *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalHashAgg == pop->Eopid() || EopPhysicalHashAggDeduplicate == pop->Eopid());

    return dynamic_cast<CPhysicalHashAgg *>(pop);
  }

};  // class CPhysicalHashAgg

}  // namespace gpopt

#endif  // !GPOS_CPhysicalHashAgg_H

// EOF
