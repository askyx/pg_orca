//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalHashAggDeduplicate.cpp
//
//	@doc:
//		Implementation of Hash Aggregate operator for deduplicating join outputs
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalHashAggDeduplicate.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAggDeduplicate::CPhysicalHashAggDeduplicate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalHashAggDeduplicate::CPhysicalHashAggDeduplicate(CMemoryPool *mp, CColRefArray *colref_array,
                                                         CColRefArray *pdrgpcrMinimal, COperator::EGbAggType egbaggtype,
                                                         CColRefArray *pdrgpcrKeys, bool fGeneratesDuplicates,
                                                         bool fMultiStage, bool isAggFromSplitDQA,
                                                         CLogicalGbAgg::EAggStage aggStage,
                                                         bool should_enforce_distribution)
    : CPhysicalHashAgg(mp, colref_array, pdrgpcrMinimal, egbaggtype, fGeneratesDuplicates,
                       nullptr /*pdrgpcrGbMinusDistinct*/, fMultiStage, isAggFromSplitDQA, aggStage,
                       should_enforce_distribution),
      m_pdrgpcrKeys(pdrgpcrKeys) {
  GPOS_ASSERT(nullptr != pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAggDeduplicate::~CPhysicalHashAggDeduplicate
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalHashAggDeduplicate::~CPhysicalHashAggDeduplicate() {
  m_pdrgpcrKeys->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashAggDeduplicate::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &CPhysicalHashAggDeduplicate::OsPrint(IOstream &os) const {
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

// EOF
