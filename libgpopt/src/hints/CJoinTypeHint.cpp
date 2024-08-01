//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2024 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CJoinTypeHint.cpp
//
//	@doc:
//		Container of join type hint objects
//---------------------------------------------------------------------------

#include "gpopt/hints/CJoinTypeHint.h"

#include "gpopt/exception.h"
#include "gpopt/hints/CHintUtils.h"
#include "gpopt/operators/CPhysicalNLJoin.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpopt;

BOOL CJoinTypeHint::SatisfiesOperator(COperator *op) {
  BOOL is_satisfied = true;

  switch (m_type) {
    case HINT_KEYWORD_NESTLOOP: {
      is_satisfied = CUtils::FNLJoin(op);
      break;
    }
    case HINT_KEYWORD_NONESTLOOP: {
      is_satisfied = !CUtils::FNLJoin(op);
      break;
    }
    case HINT_KEYWORD_MERGEJOIN: {
      is_satisfied = op->Eopid() == COperator::EopPhysicalFullMergeJoin;
      break;
    }
    case HINT_KEYWORD_NOMERGEJOIN: {
      is_satisfied = op->Eopid() != COperator::EopPhysicalFullMergeJoin;
      break;
    }
    case HINT_KEYWORD_HASHJOIN: {
      is_satisfied = CUtils::FHashJoin(op);
      break;
    }
    case HINT_KEYWORD_NOHASHJOIN: {
      is_satisfied = !CUtils::FHashJoin(op);
      break;
    }
    case SENTINEL: {
      // join type not specified
      break;
    }
  }
  if (is_satisfied) {
    this->SetHintStatus(IHint::HINT_STATE_USED);
  }
  return is_satisfied;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinTypeHint::GetAliasNames
//
//	@doc:
//		Returns a sorted array containing all table (alias) names specified in
//		the hint.
//---------------------------------------------------------------------------
const StringPtrArray *CJoinTypeHint::GetAliasNames() const {
  return m_aliases;
}

IOstream &CJoinTypeHint::OsPrint(IOstream &os) const {
  os << "JoinTypeHint:";

  os << " aliases:";
  CWStringDynamic *aliases = CDXLUtils::SerializeToCommaSeparatedString(m_mp, GetAliasNames());
  os << aliases->GetBuffer();
  GPOS_DELETE(aliases);

  os << " type:";
  os << CHintUtils::JoinTypeHintEnumToString(m_type);

  return os;
}
