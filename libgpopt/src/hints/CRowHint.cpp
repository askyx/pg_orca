//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2023 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CRowHint.cpp
//
//	@doc:
//		Container of plan hint objects
//---------------------------------------------------------------------------

#include "gpopt/hints/CRowHint.h"

#include "naucrates/dxl/CDXLUtils.h"

using namespace gpopt;

IOstream &CRowHint::OsPrint(IOstream &os) const {
  CWStringDynamic *aliases = CDXLUtils::SerializeToCommaSeparatedString(m_mp, GetAliasNames());

  os << "RowHint: " << aliases->GetBuffer();

  switch (m_type) {
    case CRowHint::RVT_ABSOLUTE: {
      os << " " << GPOS_WSZ_LIT("#");
      break;
    }
    case CRowHint::RVT_ADD: {
      os << " " << GPOS_WSZ_LIT("+");
      break;
    }
    case CRowHint::RVT_SUB: {
      os << " " << GPOS_WSZ_LIT("-");
      break;
    }
    case CRowHint::RVT_MULTI: {
      os << " " << GPOS_WSZ_LIT("*");
      break;
    }
    default: {
    }
  }

  os << m_rows;

  GPOS_DELETE(aliases);
  return os;
}
