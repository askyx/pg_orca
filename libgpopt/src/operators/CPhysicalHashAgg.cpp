//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalHashAgg.cpp
//
//	@doc:
//		Implementation of hash aggregation operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalHashAgg.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAgg::CPhysicalHashAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalHashAgg::CPhysicalHashAgg(CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal,
                                   COperator::EGbAggType egbaggtype, bool fGeneratesDuplicates,
                                   CColRefArray *pdrgpcrArgDQA, bool fMultiStage, bool isAggFromSplitDQA,
                                   CLogicalGbAgg::EAggStage aggStage, bool should_enforce_distribution)
    : CPhysicalAgg(mp, colref_array, pdrgpcrMinimal, egbaggtype, fGeneratesDuplicates, pdrgpcrArgDQA, fMultiStage,
                   isAggFromSplitDQA, aggStage, should_enforce_distribution) {}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAgg::~CPhysicalHashAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalHashAgg::~CPhysicalHashAgg() = default;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAgg::PosRequired
//
//	@doc:
//		Compute required sort columns of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalHashAgg::PosRequired(CMemoryPool *mp,
                                          CExpressionHandle &,  // exprhdl
                                          COrderSpec *,         // posRequired
                                          uint32_t
#ifdef GPOS_DEBUG
                                              child_index
#endif  // GPOS_DEBUG
                                          ,
                                          CDrvdPropArray *,  // pdrgpdpCtxt
                                          uint32_t           // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  // return empty sort order
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAgg::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalHashAgg::PosDerive(CMemoryPool *mp,
                                        CExpressionHandle &  // exprhdl
) const {
  // return empty sort order
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAgg::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalHashAgg::EpetOrder(CExpressionHandle &,  // exprhdl
                                                          const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                              peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  return CEnfdProp::EpetRequired;
}

// EOF
