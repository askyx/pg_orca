//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2023 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CScanHint.cpp
//
//	@doc:
//		Container of plan hint objects
//---------------------------------------------------------------------------

#include "gpopt/hints/CScanHint.h"

#include "gpopt/exception.h"
#include "gpopt/hints/CHintUtils.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpopt;

BOOL CScanHint::SatisfiesOperator(COperator *op) {
  BOOL is_satisfied = true;

  CEHintTypeSetSetIter type_info(*m_types);
  while (type_info.Advance() && is_satisfied) {
    switch (type_info.TBit()) {
      case SeqScan: {
        is_satisfied = op->Eopid() == COperator::EopLogicalGet;
        break;
      }
      case NoSeqScan: {
        is_satisfied = op->Eopid() != COperator::EopLogicalGet;
        break;
      }
      case IndexScan: {
        is_satisfied = op->Eopid() == COperator::EopLogicalIndexGet;
        break;
      }
      case NoIndexScan: {
        is_satisfied = op->Eopid() != COperator::EopLogicalIndexGet;
        break;
      }
      case IndexOnlyScan: {
        is_satisfied = op->Eopid() == COperator::EopLogicalIndexOnlyGet;
        break;
      }
      case NoIndexOnlyScan: {
        is_satisfied = op->Eopid() != COperator::EopLogicalIndexOnlyGet;
        break;
      }
      case BitmapScan: {
        is_satisfied = op->Eopid() == COperator::EopScalarBitmapIndexProbe;
        break;
      }
      case NoBitmapScan: {
        is_satisfied = op->Eopid() != COperator::EopScalarBitmapIndexProbe;
        break;
      }
      default: {
        CWStringDynamic *error_message = GPOS_NEW(m_mp) CWStringDynamic(m_mp, GPOS_WSZ_LIT("Unknown scan type: "));

        GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, error_message->GetBuffer());
      }
    }
  }
  if (is_satisfied) {
    this->SetHintStatus(IHint::HINT_STATE_USED);
  }
  return is_satisfied;
}

IOstream &CScanHint::OsPrint(IOstream &os) const {
  os << "ScanHint: " << m_name->GetBuffer() << "[";
  os << "indexes:";
  CWStringDynamic *indexes = CDXLUtils::SerializeToCommaSeparatedString(m_mp, GetIndexNames());
  os << indexes->GetBuffer();
  GPOS_DELETE(indexes);

  os << " types:";
  CWStringDynamic *hints = CDXLUtils::Serialize(m_mp, m_types, CHintUtils::ScanHintEnumToString);
  os << hints->GetBuffer();
  GPOS_DELETE(hints);

  os << "]";
  return os;
}
