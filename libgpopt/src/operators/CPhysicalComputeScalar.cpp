//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalComputeScalar.cpp
//
//	@doc:
//		Implementation of ComputeScalar operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalComputeScalar.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::CPhysicalComputeScalar
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalComputeScalar::CPhysicalComputeScalar(CMemoryPool *mp) : CPhysical(mp) {
  // When ComputeScalar does not contain volatile functions and includes no outer references, or if the
  // parent node explicitly allows outer refs, we create two optimization requests to enforce
  // distribution of its child:
  // (1) Any: impose no distribution requirement on the child in order to push scalar computation below
  // Motions, and then enforce required distribution on top of ComputeScalar if needed
  // (2) Pass-Thru: impose distribution requirement on child, and then perform scalar computation after
  // Motions are enforced, this is more efficient for Coordinator-Only plans below ComputeScalar

  // Otherwise, correlated execution has to be enforced.
  // In this case, we create two child optimization requests to guarantee correct evaluation of parameters
  // (1) Broadcast
  // (2) Singleton

  SetDistrRequests(2 /*ulDistrReqs*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::~CPhysicalComputeScalar
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalComputeScalar::~CPhysicalComputeScalar() = default;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL CPhysicalComputeScalar::Matches(COperator *pop) const {
  // ComputeScalar doesn't contain any members as of now
  return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalComputeScalar::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                                 ULONG child_index,
                                                 CDrvdPropArray *,  // pdrgpdpCtxt
                                                 ULONG              // ulOptReq
) {
  GPOS_ASSERT(0 == child_index && "Required properties can only be computed on the relational child");

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);
  CColRefSet *pcrsChildReqd = PcrsChildReqd(mp, exprhdl, pcrs, child_index, 1 /*ulScalarIndex*/);
  pcrs->Release();

  return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalComputeScalar::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired,
                                                ULONG child_index,
                                                CDrvdPropArray *,  // pdrgpdpCtxt
                                                ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  CColRefSet *pcrsSort = posRequired->PcrsUsed(m_mp);
  BOOL fUsesDefinedCols = FUnaryUsesDefinedColumns(pcrsSort, exprhdl);
  pcrsSort->Release();

  if (fUsesDefinedCols) {
    // if required order uses any column defined by ComputeScalar, we cannot
    // request it from child, and we pass an empty order spec;
    // order enforcer function takes care of enforcing this order on top of
    // ComputeScalar operator
    return GPOS_NEW(mp) COrderSpec(mp);
  }

  // otherwise, we pass through required order
  return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalComputeScalar::PcteRequired(CMemoryPool *,        // mp,
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
//		CPhysicalComputeScalar::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalComputeScalar::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                               ULONG  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);
  GPOS_ASSERT(2 == exprhdl.Arity());

  CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
  // include defined columns by scalar project list
  pcrs->Union(exprhdl.DeriveDefinedColumns(1));

  // include output columns of the relational child
  pcrs->Union(exprhdl.DeriveOutputColumns(0 /*child_index*/));

  BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
  pcrs->Release();

  return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalComputeScalar::PosDerive(CMemoryPool *,  // mp
                                              CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalComputeScalar::EpetOrder(CExpressionHandle &exprhdl,
                                                                const CEnfdOrder *peo) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
  if (peo->FCompatible(pos)) {
    return CEnfdProp::EpetUnnecessary;
  }

  // Sort has to go above ComputeScalar if sort columns use any column
  // defined by ComputeScalar, otherwise, Sort can either go above or below ComputeScalar
  CColRefSet *pcrsSort = peo->PosRequired()->PcrsUsed(m_mp);
  BOOL fUsesDefinedCols = FUnaryUsesDefinedColumns(pcrsSort, exprhdl);
  pcrsSort->Release();
  if (fUsesDefinedCols) {
    return CEnfdProp::EpetRequired;
  }

  return CEnfdProp::EpetOptional;
}
