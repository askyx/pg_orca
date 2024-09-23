//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalLimit.cpp
//
//	@doc:
//		Implementation of limit operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLimit.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::CPhysicalLimit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLimit::CPhysicalLimit(CMemoryPool *mp, COrderSpec *pos, bool fGlobal, bool fHasCount, bool fTopLimitUnderDML)
    : CPhysical(mp),
      m_pos(pos),
      m_fGlobal(fGlobal),
      m_fHasCount(fHasCount),
      m_top_limit_under_dml(fTopLimitUnderDML),
      m_pcrsSort(nullptr) {
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != pos);

  m_pcrsSort = m_pos->PcrsUsed(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::~CPhysicalLimit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalLimit::~CPhysicalLimit() {
  m_pos->Release();
  m_pcrsSort->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
bool CPhysicalLimit::Matches(COperator *pop) const {
  if (pop->Eopid() == Eopid()) {
    CPhysicalLimit *popLimit = CPhysicalLimit::PopConvert(pop);

    if (popLimit->FGlobal() == m_fGlobal && popLimit->FHasCount() == m_fHasCount) {
      // match if order specs match
      return m_pos->Matches(popLimit->m_pos);
    }
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::PcrsRequired
//
//	@doc:
//		Columns required by Limit's relational child
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalLimit::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                         uint32_t child_index,
                                         CDrvdPropArray *,  // pdrgpdpCtxt
                                         uint32_t           // ulOptReq
) {
  GPOS_ASSERT(0 == child_index);

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsSort);
  pcrs->Union(pcrsRequired);

  CColRefSet *pcrsChildReqd = PcrsChildReqd(mp, exprhdl, pcrs, child_index, UINT32_MAX);
  pcrs->Release();

  return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalLimit::PosRequired(CMemoryPool *,        // mp
                                        CExpressionHandle &,  // exprhdl
                                        COrderSpec *,         // posInput
                                        uint32_t
#ifdef GPOS_DEBUG
                                            child_index
#endif  // GPOS_DEBUG
                                        ,
                                        CDrvdPropArray *,  // pdrgpdpCtxt
                                        uint32_t           // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  // limit requires its internal order spec to be satisfied by its child;
  // an input required order to the limit operator is always enforced on
  // top of limit
  m_pos->AddRef();

  return m_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalLimit::PcteRequired(CMemoryPool *,        // mp,
                                      CExpressionHandle &,  // exprhdl,
                                      CCTEReq *pcter,
                                      uint32_t
#ifdef GPOS_DEBUG
                                          child_index
#endif
                                      ,
                                      CDrvdPropArray *,  // pdrgpdpCtxt,
                                      uint32_t           // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);
  return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalLimit::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                       uint32_t  // ulOptReq
) const {
  return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalLimit::PosDerive(CMemoryPool *,       // mp
                                      CExpressionHandle &  // exprhdl
) const {
  m_pos->AddRef();

  return m_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalLimit::EpetOrder(CExpressionHandle &,  // exprhdl
                                                        const CEnfdOrder *peo) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  if (peo->FCompatible(m_pos)) {
    // required order will be established by the limit operator
    return CEnfdProp::EpetUnnecessary;
  }

  // required order will be enforced on limit's output
  return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::OsPrint
//
//	@doc:
//		Print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalLimit::OsPrint(IOstream &os) const {
  os << SzId() << " " << (*m_pos) << " " << (m_fGlobal ? "global" : "local");

  return os;
}

// EOF
