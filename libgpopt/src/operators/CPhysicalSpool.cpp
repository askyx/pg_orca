//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalSpool.cpp
//
//	@doc:
//		Implementation of spool operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalSpool.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::CPhysicalSpool
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSpool::CPhysicalSpool(CMemoryPool *mp, BOOL eager) : CPhysical(mp), m_eager(eager) {}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::~CPhysicalSpool
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSpool::~CPhysicalSpool() = default;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalSpool::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                         ULONG child_index,
                                         CDrvdPropArray *,  // pdrgpdpCtxt
                                         ULONG              // ulOptReq
) {
  GPOS_ASSERT(0 == child_index);

  return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, gpos::ulong_max);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSpool::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired,
                                        ULONG child_index,
                                        CDrvdPropArray *,  // pdrgpdpCtxt
                                        ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalSpool::PcteRequired(CMemoryPool *,        // mp,
                                      CExpressionHandle &,  // exprhdl,
                                      CCTEReq *pcter,
                                      ULONG
#ifdef GPOS_DEBUG
                                          child_index
#endif
                                      ,
                                      CDrvdPropArray *,  // pdrgpdpCtxt,
                                      ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);
  return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PosDerive
//
//	@doc:
//		Derive sort order
//
//--------------------------------------------------------------------------
COrderSpec *CPhysicalSpool::PosDerive(CMemoryPool *,  // mp
                                      CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL CPhysicalSpool::Matches(COperator *pop) const {
  if (Eopid() == pop->Eopid()) {
    CPhysicalSpool *popSpool = CPhysicalSpool::PopConvert(pop);
    return m_eager == popSpool->FEager();
  }

  return false;
}

ULONG
CPhysicalSpool::HashValue() const {
  ULONG hash = COperator::HashValue();
  return gpos::CombineHashes(hash, gpos::HashValue<BOOL>(&m_eager));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalSpool::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                       ULONG  // ulOptReq
) const {
  return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalSpool::EpetOrder(CExpressionHandle &,  // exprhdl
                                                        const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                            peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  // spool is order-preserving, sort enforcers have already been added
  return CEnfdProp::EpetUnnecessary;
}

BOOL CPhysicalSpool::FValidContext(CMemoryPool *, COptimizationContext *poc,
                                   COptimizationContextArray *pdrgpocChild) const {
  GPOS_ASSERT(nullptr != pdrgpocChild);
  GPOS_ASSERT(1 == pdrgpocChild->Size());

  COptimizationContext *pocChild = (*pdrgpocChild)[0];
  CCostContext *pccBest = pocChild->PccBest();
  GPOS_ASSERT(nullptr != pccBest);
  CDrvdPropPlan *pdpplanChild = pccBest->Pdpplan();

  // partition selections that happen outside of a physical spool does not do
  // any good on rescan: a physical spool blocks the rescan from the entire
  // subtree (in particular, any dynamic scan) underneath it. That means when
  // we have a dynamic scan under a spool, and a corresponding partition
  // selector outside the spool, we run the risk of materializing the wrong
  // results.

  // For example, the following plan is invalid because the partition selector
  // won't be able to influence inner side of the nested loop join as intended
  // ("blocked" by the spool):

  // +--CPhysicalMotionGather(coordinator)
  //    +--CPhysicalInnerNLJoin
  //       |--CPhysicalPartitionSelector
  //       |  +--CPhysicalMotionBroadcast
  //       |     +--CPhysicalTableScan "foo" ("foo")
  //       |--CPhysicalSpool
  //       |  +--CPhysicalLeftOuterHashJoin
  //       |     |--CPhysicalDynamicTableScan "pt" ("pt")
  //       |     |--CPhysicalMotionHashDistribute
  //       |     |  +--CPhysicalTableScan "bar" ("bar")
  //       |     +--CScalarCmp (=)
  //       |        |--CScalarIdent "d" (19)
  //       |        +--CScalarIdent "dk" (9)
  //       +--CScalarCmp (<)
  //          |--CScalarIdent "a" (0)
  //          +--CScalarIdent "partkey" (10)
  CPartitionPropagationSpec *pps_req = poc->Prpp()->Pepp()->PppsRequired();
  if (pdpplanChild->Ppps()->IsUnsupportedPartSelector(pps_req)) {
    return false;
  }

  return true;
}

IOstream &CPhysicalSpool::OsPrint(IOstream &os) const {
  os << SzId() << " (";
  if (FEager()) {
    os << "Blocking)";
  } else {
    os << "Streaming)";
  }

  return os;
}

// EOF
