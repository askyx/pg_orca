//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalPartitionSelector.cpp
//
//	@doc:
//		Implementation of physical partition selector
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalPartitionSelector.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::CPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::CPhysicalPartitionSelector(CMemoryPool *mp, ULONG scan_id, ULONG selector_id, IMDId *mdid,
                                                       CExpression *pexprScalar)
    : CPhysical(mp), m_scan_id(scan_id), m_selector_id(selector_id), m_mdid(mdid), m_filter_expr(pexprScalar) {
  GPOS_ASSERT(0 < scan_id);
  GPOS_ASSERT(mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::~CPhysicalPartitionSelector
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::~CPhysicalPartitionSelector() {
  m_mdid->Release();
  CRefCount::SafeRelease(m_filter_expr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL CPhysicalPartitionSelector::Matches(COperator *pop) const {
  if (Eopid() != pop->Eopid()) {
    return false;
  }

  CPhysicalPartitionSelector *popPartSelector = CPhysicalPartitionSelector::PopConvert(pop);

  BOOL fScanIdCmp = popPartSelector->ScanId() == m_scan_id;
  BOOL fMdidCmp = popPartSelector->MDId()->Equals(MDId());

  return fScanIdCmp && fMdidCmp;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::HashValue
//
//	@doc:
//		Hash operator
//
//---------------------------------------------------------------------------
ULONG
CPhysicalPartitionSelector::HashValue() const {
  return gpos::CombineHashes(Eopid(), gpos::CombineHashes(m_scan_id, MDId()->HashValue()));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalPartitionSelector::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput,
                                                     ULONG child_index,
                                                     CDrvdPropArray *,  // pdrgpdpCtxt
                                                     ULONG              // ulOptReq
) {
  GPOS_ASSERT(0 == child_index && "Required properties can only be computed on the relational child");

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsInput);
  pcrs->Union(m_filter_expr->DeriveUsedColumns());
  pcrs->Intersection(exprhdl.DeriveOutputColumns(child_index));

  return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalPartitionSelector::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                    COrderSpec *posRequired, ULONG child_index,
                                                    CDrvdPropArray *,  // pdrgpdpCtxt
                                                    ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalPartitionSelector::PcteRequired(CMemoryPool *,        // mp,
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

CPartitionPropagationSpec *CPhysicalPartitionSelector::PppsRequired(CMemoryPool *mp, CExpressionHandle &,
                                                                    CPartitionPropagationSpec *pppsRequired,
                                                                    ULONG child_index GPOS_ASSERTS_ONLY,
                                                                    CDrvdPropArray *, ULONG) const {
  GPOS_ASSERT(child_index == 0);

  CPartitionPropagationSpec *pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
  pps_result->InsertAllExcept(pppsRequired, m_scan_id);
  return pps_result;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalPartitionSelector::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                                   ULONG  // ulOptReq
) const {
  return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalPartitionSelector::PosDerive(CMemoryPool *,  // mp
                                                  CExpressionHandle &exprhdl) const {
  return PosDerivePassThruOuter(exprhdl);
}

CPartitionPropagationSpec *CPhysicalPartitionSelector::PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  CPartitionPropagationSpec *pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
  CPartitionPropagationSpec *pps_child = exprhdl.Pdpplan(0 /* child_index */)->Ppps();

  CBitSet *selector_ids = GPOS_NEW(mp) CBitSet(mp);
  selector_ids->ExchangeSet(m_selector_id);

  pps_result->InsertAll(pps_child);
  pps_result->Insert(m_scan_id, CPartitionPropagationSpec::EpptPropagator, m_mdid, selector_ids, nullptr);
  selector_ids->Release();

  return pps_result;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalPartitionSelector::EpetOrder(CExpressionHandle &,  // exprhdl,
                                                                    const CEnfdOrder *    // ped
) const {
  return CEnfdProp::EpetOptional;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalPartitionSelector::OsPrint(IOstream &os) const {
  os << SzId() << ", Id: " << SelectorId() << ", Scan Id: " << m_scan_id << ", Part Table: ";
  MDId()->OsPrint(os);

  return os;
}

// EOF
