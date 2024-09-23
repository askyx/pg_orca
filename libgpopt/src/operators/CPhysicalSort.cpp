//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalSort.cpp
//
//	@doc:
//		Implementation of physical sort operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalSort.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::CPhysicalSort
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSort::CPhysicalSort(CMemoryPool *mp, COrderSpec *pos)
    : CPhysical(mp),
      m_pos(pos),  // caller must add-ref pos
      m_pcrsSort(nullptr) {
  GPOS_ASSERT(nullptr != pos);

  m_pcrsSort = Pos()->PcrsUsed(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::~CPhysicalSort
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSort::~CPhysicalSort() {
  m_pos->Release();
  m_pcrsSort->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::Matches
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL CPhysicalSort::Matches(COperator *pop) const {
  if (Eopid() != pop->Eopid()) {
    return false;
  }

  CPhysicalSort *popSort = CPhysicalSort::PopConvert(pop);
  return m_pos->Matches(popSort->Pos());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalSort::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                        ULONG child_index,
                                        CDrvdPropArray *,  // pdrgpdpCtxt
                                        ULONG              // ulOptReq
) {
  GPOS_ASSERT(0 == child_index);

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsSort);
  pcrs->Union(pcrsRequired);
  CColRefSet *pcrsChildReqd = PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
  pcrs->Release();

  return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSort::PosRequired(CMemoryPool *mp,
                                       CExpressionHandle &,  // exprhdl
                                       COrderSpec *,         // posRequired
                                       ULONG
#ifdef GPOS_DEBUG
                                           child_index
#endif  // GPOS_DEBUG
                                       ,
                                       CDrvdPropArray *,  // pdrgpdpCtxt
                                       ULONG              // ulOptReq
) const {
  GPOS_ASSERT(0 == child_index);

  // sort operator is order-establishing and does not require child to deliver
  // any sort order; we return an empty sort order as child requirement
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalSort::PcteRequired(CMemoryPool *,        // mp,
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
//		CPhysicalSort::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalSort::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                      ULONG  // ulOptReq
) const {
  return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalSort::PosDerive(CMemoryPool *,       // mp
                                     CExpressionHandle &  // exprhdl
) const {
  m_pos->AddRef();
  return m_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalSort::EpetOrder(CExpressionHandle &,  // exprhdl
                                                       const CEnfdOrder *peo) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  if (peo->FCompatible(m_pos)) {
    // required order is already established by sort operator
    return CEnfdProp::EpetUnnecessary;
  }

  // required order is incompatible with the order established by the
  // sort operator, prohibit adding another sort operator on top
  return CEnfdProp::EpetProhibited;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalSort::OsPrint(IOstream &os) const {
  os << SzId() << "  ";
  return Pos()->OsPrint(os);
}

// EOF
