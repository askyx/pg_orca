//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalAgg.cpp
//
//	@doc:
//		Implementation of basic aggregate operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalAgg.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::CPhysicalAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalAgg::CPhysicalAgg(CMemoryPool *mp, CColRefArray *colref_array,
                           CColRefArray *pdrgpcrMinimal,  // minimal grouping columns based on FD's
                           COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates, CColRefArray *pdrgpcrArgDQA,
                           BOOL fMultiStage, BOOL isAggFromSplitDQA, CLogicalGbAgg::EAggStage aggStage,
                           BOOL should_enforce_distribution)
    : CPhysical(mp),
      m_pdrgpcr(colref_array),
      m_egbaggtype(egbaggtype),
      m_isAggFromSplitDQA(isAggFromSplitDQA),
      m_aggStage(aggStage),
      m_pdrgpcrMinimal(nullptr),
      m_fGeneratesDuplicates(fGeneratesDuplicates),
      m_pdrgpcrArgDQA(pdrgpcrArgDQA),
      m_fMultiStage(fMultiStage),
      m_should_enforce_distribution(should_enforce_distribution) {
  GPOS_ASSERT(nullptr != colref_array);
  GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
  GPOS_ASSERT_IMP(EgbaggtypeGlobal != egbaggtype, fMultiStage);

  ULONG ulDistrReqs = 1;
  if (pdrgpcrMinimal == nullptr || 0 == pdrgpcrMinimal->Size()) {
    colref_array->AddRef();
    m_pdrgpcrMinimal = colref_array;
  } else {
    pdrgpcrMinimal->AddRef();
    m_pdrgpcrMinimal = pdrgpcrMinimal;
  }

  if (COperator::EgbaggtypeLocal == egbaggtype) {
    // If the local aggregate has no distinct columns we generate
    // two optimization requests for its children:
    // (1) Any distribution requirement
    //
    // (2)	Random distribution requirement; this is needed to alleviate
    //		possible data skew

    ulDistrReqs = 2;
    if (pdrgpcrArgDQA != nullptr && 0 != pdrgpcrArgDQA->Size()) {
      // If the local aggregate has distinct columns we generate
      // two optimization requests for its children:
      // (1) hash distribution on the distinct columns only
      // (2) hash distribution on the grouping and distinct
      //     columns (only if the grouping columns are not empty)
      if (0 == m_pdrgpcr->Size()) {
        ulDistrReqs = 1;
      }
    }
  } else if (COperator::EgbaggtypeIntermediate == egbaggtype) {
    GPOS_ASSERT(nullptr != pdrgpcrArgDQA);
    GPOS_ASSERT(pdrgpcrArgDQA->Size() <= colref_array->Size());
    // Intermediate Agg generates two optimization requests for its children:
    // (1) Hash distribution on the group by columns + distinct column
    // (2) Hash distribution on the group by columns

    ulDistrReqs = 2;

    if (pdrgpcrArgDQA->Size() == colref_array->Size() || GPOS_FTRACE(EopttraceForceAggSkewAvoidance)) {
      // scalar aggregates so we only request the first case
      ulDistrReqs = 1;
    }
  } else if (COperator::EgbaggtypeGlobal == egbaggtype) {
    // Global Agg generates two optimization requests for its children:
    // (1) Singleton distribution, if child has volatile functions
    // (2) Hash distribution on the group by columns
    ulDistrReqs = 2;
  }

  // Split DQA generates a 2-stage aggregate to handle the case where
  // hash aggregate has a distinct agg func. Here we need to be careful
  // not to prohibit distribution property enforcement.
  m_should_enforce_distribution &=
      !(isAggFromSplitDQA && aggStage == CLogicalGbAgg::EasTwoStageScalarDQA && colref_array->Size() > 0);

  SetDistrRequests(ulDistrReqs);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::~CPhysicalAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalAgg::~CPhysicalAgg() {
  m_pdrgpcr->Release();
  m_pdrgpcrMinimal->Release();
  CRefCount::SafeRelease(m_pdrgpcrArgDQA);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalAgg::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                       ULONG child_index,
                                       CDrvdPropArray *,  // pdrgpdpCtxt
                                       ULONG              // ulOptReq
) {
  return PcrsRequiredAgg(mp, exprhdl, pcrsRequired, child_index, m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcrsRequiredAgg
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *CPhysicalAgg::PcrsRequiredAgg(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                          ULONG child_index, CColRefArray *pdrgpcrGrp) {
  GPOS_ASSERT(nullptr != pdrgpcrGrp);
  GPOS_ASSERT(0 == child_index && "Required properties can only be computed on the relational child");

  CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

  // include grouping columns
  pcrs->Include(pdrgpcrGrp);
  pcrs->Union(pcrsRequired);

  CColRefSet *pcrsOutput = PcrsChildReqd(mp, exprhdl, pcrs, child_index, 1 /*ulScalarIndex*/);
  pcrs->Release();

  return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *CPhysicalAgg::PcteRequired(CMemoryPool *,        // mp,
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
//		CPhysicalAgg::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL CPhysicalAgg::FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                     ULONG  // ulOptReq
) const {
  GPOS_ASSERT(nullptr != pcrsRequired);
  GPOS_ASSERT(2 == exprhdl.Arity());

  CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);

  // include grouping columns
  pcrs->Include(PdrgpcrGroupingCols());

  // include defined columns by scalar child
  pcrs->Union(exprhdl.DeriveDefinedColumns(1));
  BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
  pcrs->Release();

  return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalAgg::HashValue() const {
  ULONG ulHash = COperator::HashValue();
  const ULONG arity = m_pdrgpcr->Size();
  ULONG ulGbaggtype = (ULONG)m_egbaggtype;
  ULONG ulaggstage = (ULONG)m_aggStage;
  for (ULONG ul = 0; ul < arity; ul++) {
    CColRef *colref = (*m_pdrgpcr)[ul];
    ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(colref));
  }

  ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<ULONG>(&ulGbaggtype));
  ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<ULONG>(&ulaggstage));

  return gpos::CombineHashes(ulHash, gpos::HashValue<BOOL>(&m_fGeneratesDuplicates));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::Matches
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL CPhysicalAgg::Matches(COperator *pop) const {
  if (pop->Eopid() != Eopid()) {
    return false;
  }

  CPhysicalAgg *popAgg = dynamic_cast<CPhysicalAgg *>(pop);

  if (FGeneratesDuplicates() != popAgg->FGeneratesDuplicates()) {
    return false;
  }

  if (popAgg->Egbaggtype() == m_egbaggtype && m_pdrgpcr->Equals(popAgg->m_pdrgpcr)) {
    if (CColRef::Equals(m_pdrgpcrMinimal, popAgg->m_pdrgpcrMinimal)) {
      return (m_pdrgpcrArgDQA == nullptr || 0 == m_pdrgpcrArgDQA->Size()) ||
             CColRef::Equals(m_pdrgpcrArgDQA, popAgg->PdrgpcrArgDQA());
    }
  }

  return false;
}

BOOL CPhysicalAgg::IsTwoStageScalarDQA() const {
  return (m_aggStage == CLogicalGbAgg::EasTwoStageScalarDQA);
}

BOOL CPhysicalAgg::IsThreeStageScalarDQA() const {
  return (m_aggStage == CLogicalGbAgg::EasThreeStageScalarDQA);
}

BOOL CPhysicalAgg::IsAggFromSplitDQA() const {
  return m_isAggFromSplitDQA;
}
//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalAgg::OsPrint(IOstream &os) const {
  if (m_fPattern) {
    return COperator::OsPrint(os);
  }

  os << SzId() << "( ";
  CLogicalGbAgg::OsPrintGbAggType(os, m_egbaggtype);
  if (m_fMultiStage) {
    os << ", multi-stage";
  }
  os << " )";

  os << " Grp Cols: [";

  CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
  os << "]" << ", Minimal Grp Cols:[";
  CUtils::OsPrintDrgPcr(os, m_pdrgpcrMinimal);
  os << "]";

  if (COperator::EgbaggtypeIntermediate == m_egbaggtype) {
    os << ", Distinct Cols:[";
    CUtils::OsPrintDrgPcr(os, m_pdrgpcrArgDQA);
    os << "]";
  }
  os << ", Generates Duplicates :[ " << FGeneratesDuplicates() << " ] ";

  // note: 2-stage Scalar DQA and 3-stage scalar DQA are created by CXformSplitDQA only
  if (IsTwoStageScalarDQA()) {
    os << ", m_aggStage :[ Two Stage Scalar DQA ] ";
  }

  if (IsThreeStageScalarDQA()) {
    os << ", m_aggStage :[ Three Stage Scalar DQA ] ";
  }

  return os;
}

// EOF
