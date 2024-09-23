//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalNLJoin.cpp
//
//	@doc:
//		Implementation of base nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalNLJoin.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalCorrelatedInLeftSemiNLJoin.h"
#include "gpopt/operators/CPhysicalCorrelatedInnerNLJoin.h"
#include "gpopt/operators/CPhysicalCorrelatedLeftOuterNLJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::CPhysicalNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalNLJoin::CPhysicalNLJoin(CMemoryPool *mp) : CPhysicalJoin(mp) {
  // NLJ creates two partition propagation requests for children:
  // (0) push possible Dynamic Partition Elimination (DPE) predicates from join's predicate to
  //		outer child, since outer child executes first
  // (1) ignore DPE opportunities in join's predicate, and push incoming partition propagation
  //		request to both children,
  //		this request handles the case where the inner child needs to be broadcasted, which prevents
  //		DPE by outer child since a Motion operator gets in between PartitionSelector and DynamicScan

  SetPartPropagateRequests(2);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::~CPhysicalNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalNLJoin::~CPhysicalNLJoin() = default;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalNLJoin::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posInput,
                                         ULONG child_index,
                                         CDrvdPropArray *,  // pdrgpdpCtxt
                                         ULONG              // ulOptReq
) const {
  GPOS_ASSERT(child_index < 2 && "Required sort order can be computed on the relational child only");

  if (0 == child_index) {
    return PosPropagateToOuter(mp, exprhdl, posInput);
  }

  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::PcrsRequired
//
//	@doc:
//		Compute required output columns of n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalNLJoin::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                          ULONG child_index,
                                          CDrvdPropArray *,  // pdrgpdpCtxt
                                          ULONG              // ulOptReq
) {
  GPOS_ASSERT(child_index < 2 && "Required properties can only be computed on the relational child");

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
  pcrs->Include(pcrsRequired);

  // For subqueries in the projection list, the required columns from the outer child
  // are often pushed down to the inner child and are not visible at the top level
  // so we can use the outer refs of the inner child as required from outer child
  if (0 == child_index) {
    CColRefSet *outer_refs = exprhdl.DeriveOuterReferences(1);
    pcrs->Include(outer_refs);
  }

  // request inner child of correlated join to provide required inner columns
  if (1 == child_index && FCorrelated()) {
    pcrs->Include(PdrgPcrInner());
  }

  CColRefSet *pcrsReqd = PcrsChildReqd(mp, exprhdl, pcrs, child_index, 2 /*ulScalarIndex*/);
  pcrs->Release();

  return pcrsReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator;
//
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalNLJoin::EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  if (FSortColsInOuterChild(m_mp, exprhdl, peo->PosRequired())) {
    return CEnfdProp::EpetOptional;
  }

  return CEnfdProp::EpetRequired;
}

// EOF
