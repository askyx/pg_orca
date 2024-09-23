//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalFullMergeJoin.cpp
//
//	@doc:
//		Implementation of full merge join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalFullMergeJoin.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/base.h"

using namespace gpopt;

#define GPOPT_MAX_HASH_DIST_REQUESTS 6

// ctor
CPhysicalFullMergeJoin::CPhysicalFullMergeJoin(CMemoryPool *mp, CExpressionArray *outer_merge_clauses,
                                               CExpressionArray *inner_merge_clauses, IMdIdArray *, bool,
                                               CXform::EXformId origin_xform)
    : CPhysicalJoin(mp, origin_xform),
      m_outer_merge_clauses(outer_merge_clauses),
      m_inner_merge_clauses(inner_merge_clauses) {
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != outer_merge_clauses);
  GPOS_ASSERT(nullptr != inner_merge_clauses);
  GPOS_ASSERT(outer_merge_clauses->Size() == inner_merge_clauses->Size());

  // There is one request per col, up to the max number of requests
  // plus an additional request for all the cols, and one for the singleton.
  uint32_t num_hash_reqs = std::min((uint32_t)GPOPT_MAX_HASH_DIST_REQUESTS, outer_merge_clauses->Size());
  SetDistrRequests(num_hash_reqs + 2);
}

// dtor
CPhysicalFullMergeJoin::~CPhysicalFullMergeJoin() {
  m_outer_merge_clauses->Release();
  m_inner_merge_clauses->Release();
}

COrderSpec *CPhysicalFullMergeJoin::PosRequired(CMemoryPool *mp,
                                                CExpressionHandle &,  // exprhdl,
                                                COrderSpec *,         // posInput
                                                uint32_t child_index,
                                                CDrvdPropArray *,  // pdrgpdpCtxt
                                                uint32_t           // ulOptReq
) const {
  // Merge joins require their input to be sorted on corresponsing join clauses. Without
  // making dangerous assumptions of the implementation of the merge joins, it is difficult
  // to predict the order of the output of the merge join. (This may not be true). In that
  // case, it is better to not push down any order requests from above.

  COrderSpec *os = GPOS_NEW(mp) COrderSpec(mp);

  CExpressionArray *clauses;
  if (child_index == 0) {
    clauses = m_outer_merge_clauses;
  } else {
    GPOS_ASSERT(child_index == 1);
    clauses = m_inner_merge_clauses;
  }

  for (uint32_t ul = 0; ul < clauses->Size(); ++ul) {
    CExpression *expr = (*clauses)[ul];

    GPOS_ASSERT(CUtils::FScalarIdent(expr));
    const CColRef *colref = CCastUtils::PcrExtractFromScIdOrCastScId(expr);

    // Make sure that the corresponding properties (mergeStrategies, mergeNullsFirst)
    // in CTranslatorDXLToPlStmt::TranslateDXLMergeJoin() match.
    //
    // NB: The operator used for sorting here is the '<' operator in the
    // default btree opfamily of the column's type. For this to work correctly,
    // the '=' operator of the merge join clauses must also belong to the same
    // opfamily, which in this case, is the default of the type.
    // See FMergeJoinCompatible() where predicates using a different opfamily
    // are rejected from merge clauses.
    gpmd::IMDId *mdid = colref->RetrieveType()->GetMdidForCmpType(IMDType::EcmptL);
    mdid->AddRef();
    os->Append(mdid, colref, COrderSpec::EntLast);
  }

  return os;
}

// return order property enforcing type for this operator
CEnfdProp::EPropEnforcingType CPhysicalFullMergeJoin::EpetOrder(CExpressionHandle &, const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                                                         peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  // merge join is not order-preserving, at least in
  // the sense that nulls maybe interleaved;
  // any order requirements have to be enforced on top
  return CEnfdProp::EpetRequired;
}
