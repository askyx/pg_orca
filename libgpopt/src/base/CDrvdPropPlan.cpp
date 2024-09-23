//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdPropPlan.cpp
//
//	@doc:
//		Derived plan properties
//---------------------------------------------------------------------------

#include "gpopt/base/CDrvdPropPlan.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/CPhysicalCTEConsumer.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CDrvdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::CDrvdPropPlan() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::~CDrvdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::~CDrvdPropPlan() {
  CRefCount::SafeRelease(m_pos);
  CRefCount::SafeRelease(m_ppps);
  CRefCount::SafeRelease(m_pcm);
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Pdpplan
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropPlan *CDrvdPropPlan::Pdpplan(CDrvdProp *pdp) {
  GPOS_ASSERT(nullptr != pdp);
  GPOS_ASSERT(EptPlan == pdp->Ept() && "This is not a plan properties container");

  return dynamic_cast<CDrvdPropPlan *>(pdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Derive
//
//	@doc:
//		Derive plan props
//
//---------------------------------------------------------------------------
void CDrvdPropPlan::Derive(CMemoryPool *mp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt) {
  CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
  if (nullptr != pdpctxt && COperator::EopPhysicalCTEConsumer == popPhysical->Eopid()) {
    CopyCTEProducerPlanProps(mp, pdpctxt, popPhysical);
  } else {
    // call property derivation functions on the operator
    m_pos = popPhysical->PosDerive(mp, exprhdl);
    m_ppps = popPhysical->PppsDerive(mp, exprhdl);
  }

  m_pcm = popPhysical->PcmDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CopyCTEProducerPlanProps
//
//	@doc:
//		Copy CTE producer plan properties from given context to current object
//
//---------------------------------------------------------------------------
void CDrvdPropPlan::CopyCTEProducerPlanProps(CMemoryPool *mp, CDrvdPropCtxt *pdpctxt, COperator *pop) {
  CDrvdPropCtxtPlan *pdpctxtplan = CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt);
  CPhysicalCTEConsumer *popCTEConsumer = CPhysicalCTEConsumer::PopConvert(pop);
  uint32_t ulCTEId = popCTEConsumer->UlCTEId();
  UlongToColRefMap *colref_mapping = popCTEConsumer->Phmulcr();
  CDrvdPropPlan *pdpplan = pdpctxtplan->PdpplanCTEProducer(ulCTEId);
  if (nullptr != pdpplan) {
    // copy producer plan properties after remapping columns
    m_pos = pdpplan->Pos()->PosCopyWithRemappedColumns(mp, colref_mapping, true /*must_exist*/);

    // no need to copy the part index map. return an empty one. This is to
    // distinguish between a CTE consumer and the inlined expression
    m_ppps = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
bool CDrvdPropPlan::FSatisfies(const CReqdPropPlan *prpp) const {
  GPOS_ASSERT(nullptr != prpp);
  GPOS_ASSERT(nullptr != prpp->Peo());
  GPOS_ASSERT(nullptr != prpp->Pcter());

  return m_pos->FSatisfies(prpp->Peo()->PosRequired()) && m_ppps->FSatisfies(prpp->Pepp()->PppsRequired()) &&
         m_pcm->FSatisfies(prpp->Pcter());
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
uint32_t CDrvdPropPlan::HashValue() const {
  uint32_t ulHash = gpos::CombineHashes(m_pos->HashValue(), m_pcm->HashValue());

  return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
uint32_t CDrvdPropPlan::Equals(const CDrvdPropPlan *pdpplan) const {
  return m_pos->Matches(pdpplan->Pos()) && m_ppps->Equals(pdpplan->Ppps()) && m_pcm->Equals(pdpplan->GetCostModel());
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CDrvdPropPlan::OsPrint(IOstream &os) const {
  os << "Drvd Plan Props (" << "ORD: " << (*m_pos) << ", PART PROP:" << (*m_ppps) << ")" << ", CTE Map: [" << *m_pcm
     << "]";

  return os;
}

// EOF
