//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2024 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CJoinHint.cpp
//
//	@doc:
//		Container of join hint objects
//---------------------------------------------------------------------------

#include "gpopt/hints/CJoinHint.h"

#include "gpopt/exception.h"
#include "gpopt/hints/CHintUtils.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		SerializeJoinOrderHint
//
//	@doc:
//		Serialize a JoinNode into the original Leading hint string. Return the
//		serialized string.
//---------------------------------------------------------------------------
static CWStringDynamic *SerializeJoinOrderHint(CMemoryPool *mp, const CJoinHint::JoinNode *join_pair) {
  CWStringDynamic *result = GPOS_NEW(mp) CWStringDynamic(mp);

  if (nullptr != join_pair->GetName()) {
    result->AppendFormat(GPOS_WSZ_LIT("%ls"), join_pair->GetName()->GetBuffer());
  } else {
    if (join_pair->IsDirected()) {
      result->AppendFormat(GPOS_WSZ_LIT("%ls"), GPOS_WSZ_LIT("("));
    }

    CWStringDynamic *str_outer = SerializeJoinOrderHint(mp, join_pair->GetOuter());
    result->AppendFormat(GPOS_WSZ_LIT("%ls"), str_outer->GetBuffer());
    GPOS_DELETE(str_outer);

    result->AppendFormat(GPOS_WSZ_LIT("%ls"), GPOS_WSZ_LIT(" "));

    CWStringDynamic *str_inner = SerializeJoinOrderHint(mp, join_pair->GetInner());
    result->AppendFormat(GPOS_WSZ_LIT("%ls"), str_inner->GetBuffer());
    GPOS_DELETE(str_inner);

    if (join_pair->IsDirected()) {
      result->AppendFormat(GPOS_WSZ_LIT("%ls"), GPOS_WSZ_LIT(")"));
    }
  }
  return result;
}

IOstream &CJoinHint::JoinNode::OsPrint(IOstream &os) const {
  CAutoMemoryPool amp;
  CWStringDynamic *dxl_string = SerializeJoinOrderHint(amp.Pmp(), this);
  os << dxl_string->GetBuffer();

  GPOS_DELETE(dxl_string);

  return os;
}

CJoinHint::CJoinHint(CMemoryPool *mp, JoinNode *join_pair) : m_mp(mp), m_join_node(join_pair) {}

IOstream &CJoinHint::OsPrint(IOstream &os) const {
  os << "JoinHint:";

  m_join_node->OsPrint(os);

  return os;
}
