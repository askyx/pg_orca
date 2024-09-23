//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalStreamAggDeduplicate.cpp
//
//	@doc:
//		Implementation of stream aggregation operator for deduplicating join outputs
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalStreamAggDeduplicate.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAggDeduplicate::CPhysicalStreamAggDeduplicate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalStreamAggDeduplicate::CPhysicalStreamAggDeduplicate(
    CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal, COperator::EGbAggType egbaggtype,
    CColRefArray *pdrgpcrKeys, bool fGeneratesDuplicates, bool fMultiStage, bool isAggFromSplitDQA,
    CLogicalGbAgg::EAggStage aggStage, bool should_enforce_distribution)
    : CPhysicalStreamAgg(mp, colref_array, pdrgpcrMinimal, egbaggtype, fGeneratesDuplicates,
                         nullptr /*pdrgpcrGbMinusDistinct*/, fMultiStage, isAggFromSplitDQA, aggStage,
                         should_enforce_distribution),
      m_pdrgpcrKeys(pdrgpcrKeys) {
  GPOS_ASSERT(nullptr != pdrgpcrKeys);
  InitOrderSpec(mp, m_pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAggDeduplicate::~CPhysicalStreamAggDeduplicate
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalStreamAggDeduplicate::~CPhysicalStreamAggDeduplicate() {
  m_pdrgpcrKeys->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAggDeduplicate::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalStreamAggDeduplicate::OsPrint(IOstream &os) const {
  if (m_fPattern) {
    return COperator::OsPrint(os);
  }

  os << SzId() << "( ";
  CLogicalGbAgg::OsPrintGbAggType(os, Egbaggtype());
  os << " )" << " Grp Cols: [";

  CUtils::OsPrintDrgPcr(os, PdrgpcrGroupingCols());
  os << "]" << ", Key Cols:[";
  CUtils::OsPrintDrgPcr(os, m_pdrgpcrKeys);
  os << "]";

  os << ", Generates Duplicates :[ " << FGeneratesDuplicates() << " ] ";

  return os;
}

// EOF
