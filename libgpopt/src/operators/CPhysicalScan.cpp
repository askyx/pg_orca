//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalScan.cpp
//
//	@doc:
//		Implementation of base scan operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalScan.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::CPhysicalScan
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysicalScan::CPhysicalScan(CMemoryPool *mp, const CName *pnameAlias, CTableDescriptor *ptabdesc,
                             CColRefArray *pdrgpcrOutput)
    : CPhysical(mp),
      m_pnameAlias(pnameAlias),
      m_ptabdesc(ptabdesc),
      m_pdrgpcrOutput(pdrgpcrOutput),
      m_pstatsBaseTable(nullptr) {
  GPOS_ASSERT(nullptr != ptabdesc);
  GPOS_ASSERT(nullptr != pnameAlias);
  GPOS_ASSERT(nullptr != pdrgpcrOutput);

  ComputeTableStats(m_mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::~CPhysicalScan
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CPhysicalScan::~CPhysicalScan() {
  m_ptabdesc->Release();
  m_pdrgpcrOutput->Release();
  m_pstatsBaseTable->Release();
  GPOS_DELETE(m_pnameAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
bool CPhysicalScan::FInputOrderSensitive() const {
  GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool CPhysicalScan::FProvidesReqdCols(CExpressionHandle &,  // exprhdl
                                      CColRefSet *pcrsRequired,
                                      uint32_t  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);

  CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
  pcrs->Include(m_pdrgpcrOutput);

  bool result = pcrs->ContainsAll(pcrsRequired);
  pcrs->Release();

  return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalScan::EpetOrder(CExpressionHandle &,  // exprhdl
                                                       const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                           peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::ComputeTableStats
//
//	@doc:
//		Compute stats of underlying table
//
//---------------------------------------------------------------------------
void CPhysicalScan::ComputeTableStats(CMemoryPool *mp) {
  GPOS_ASSERT(nullptr == m_pstatsBaseTable);

  CColRefSet *pcrsHist = GPOS_NEW(mp) CColRefSet(mp);
  CColRefSet *pcrsWidth = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcrOutput);
  CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
  m_pstatsBaseTable = md_accessor->Pstats(mp, m_ptabdesc->MDId(), pcrsHist, pcrsWidth);
  GPOS_ASSERT(nullptr != m_pstatsBaseTable);

  pcrsHist->Release();
  pcrsWidth->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalScan *CPhysicalScan::PopConvert(COperator *pop) {
  GPOS_ASSERT(nullptr != pop);
  GPOS_ASSERT(CUtils::FPhysicalScan(pop));

  return dynamic_cast<CPhysicalScan *>(pop);
}

// EOF
