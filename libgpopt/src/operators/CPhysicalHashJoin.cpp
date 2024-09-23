//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalHashJoin.cpp
//
//	@doc:
//		Implementation of hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalHashJoin.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/base.h"

using namespace gpopt;

// number of non-redistribute requests created by hash join
#define GPOPT_NON_HASH_DIST_REQUESTS 3

// maximum number of redistribute requests on single hash join keys
#define GPOPT_MAX_HASH_DIST_REQUESTS 6

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CPhysicalHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalHashJoin::CPhysicalHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
                                     CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
                                     BOOL is_null_aware, CXform::EXformId origin_xform)
    : CPhysicalJoin(mp, origin_xform),
      m_pdrgpexprOuterKeys(pdrgpexprOuterKeys),
      m_pdrgpexprInnerKeys(pdrgpexprInnerKeys),
      m_hash_opfamilies(nullptr),
      m_is_null_aware(is_null_aware) {
  GPOS_ASSERT(nullptr != mp);
  GPOS_ASSERT(nullptr != pdrgpexprOuterKeys);
  GPOS_ASSERT(nullptr != pdrgpexprInnerKeys);
  GPOS_ASSERT(pdrgpexprOuterKeys->Size() == pdrgpexprInnerKeys->Size());

  if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution)) {
    GPOS_ASSERT(nullptr != hash_opfamilies);
    m_hash_opfamilies = hash_opfamilies;
    GPOS_ASSERT(pdrgpexprOuterKeys->Size() == m_hash_opfamilies->Size());
  }

  CreateOptRequests();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::~CPhysicalHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalHashJoin::~CPhysicalHashJoin() {
  m_pdrgpexprOuterKeys->Release();
  m_pdrgpexprInnerKeys->Release();
  CRefCount::SafeRelease(m_hash_opfamilies);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *CPhysicalHashJoin::PosRequired(CMemoryPool *mp,
                                           CExpressionHandle &,  // exprhdl
                                           COrderSpec *,         // posInput,
                                           ULONG
#ifdef GPOS_DEBUG
                                               child_index
#endif  // GPOS_DEBUG
                                           ,
                                           CDrvdPropArray *,  // pdrgpdpCtxt
                                           ULONG              // ulOptReq
) const {
  GPOS_ASSERT(child_index < 2 && "Required sort order can be computed on the relational child only");

  // hash join does not have order requirements to both children, and it
  // does not preserve any sort order
  return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		FIdenticalExpression
//
//	@doc:
//		Check whether the expressions match based on column name, instead of
//		column identifier. This is to accommodate the case of self-joins where
//		column names match, but the column identifier may not.
//---------------------------------------------------------------------------
static BOOL FIdenticalExpression(CExpression *left, CExpression *right) {
  if (left->Pop()->Eopid() == COperator::EopScalarIdent && right->Pop()->Eopid() == COperator::EopScalarIdent) {
    // skip colid check. just make sure that names are same.
    return CWStringConst::Equals(CScalarIdent::PopConvert(left->Pop())->Pcr()->Name().Pstr(),
                                 CScalarIdent::PopConvert(right->Pop())->Pcr()->Name().Pstr());
  } else if (!left->Pop()->Matches(right->Pop()) || left->Arity() != right->Arity()) {
    return false;
  } else {
    for (ULONG ul = 0; ul < left->Arity(); ul++) {
      if (!FIdenticalExpression((*left)[ul], (*right)[ul])) {
        return false;
      }
    }
    return true;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		FIdenticalExpressionArrays
//
//	@doc:
//		Check whether the expressions in input arrays match, *not* including
//		colid.
//---------------------------------------------------------------------------
static BOOL FIdenticalExpressionArrays(const CExpressionArray *outer, const CExpressionArray *inner) {
  GPOS_ASSERT(outer->Size() == inner->Size());

  for (ULONG ul = 0; ul < outer->Size(); ul++) {
    if (!FIdenticalExpression((*outer)[ul], (*inner)[ul])) {
      return false;
    }
  }
  return true;
}

BOOL CPhysicalHashJoin::FSelfJoinWithMatchingJoinKeys(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  // There may be duplicate mdids because the hash key is unique on a
  // combination of mdid and alias. Here we do not care about duplicate
  // aliases because joining the same table with different alias is still a
  // self-join.
  CTableDescriptorHashSet *outertabs =
      CUtils::RemoveDuplicateMdids(mp, exprhdl.DeriveTableDescriptor(0 /*child_index*/));
  CTableDescriptorHashSet *innertabs =
      CUtils::RemoveDuplicateMdids(mp, exprhdl.DeriveTableDescriptor(1 /*child_index*/));

  BOOL result = false;

  // Check that this is a self join. Size() of 1 means that there is 1 unique
  // table on each side of the join. Check whether it is the same table.
  if (outertabs->Size() == 1 && innertabs->Size() == 1 &&
      outertabs->First()->MDId()->Equals(innertabs->First()->MDId())) {
    // Check that the join keys are identical
    if (FIdenticalExpressionArrays(PdrgpexprInnerKeys(), PdrgpexprOuterKeys())) {
      result = true;
    }
  }
  outertabs->Release();
  innertabs->Release();

  return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator;
//
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType CPhysicalHashJoin::EpetOrder(CExpressionHandle &,  // exprhdl
                                                           const CEnfdOrder *
#ifdef GPOS_DEBUG
                                                               peo
#endif  // GPOS_DEBUG
) const {
  GPOS_ASSERT(nullptr != peo);
  GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

  // hash join is not order-preserving;
  // any order requirements have to be enforced on top
  return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::FNullableInnerHashKeys
//
//	@doc:
//		Check whether the hash keys from one child are nullable. pcrsNotNull must
//		be all the "not null" columns coming from that child
//
//---------------------------------------------------------------------------
BOOL CPhysicalHashJoin::FNullableHashKeys(CColRefSet *pcrsNotNull, BOOL fInner) const {
  ULONG ulHashKeys = 0;
  if (fInner) {
    ulHashKeys = m_pdrgpexprInnerKeys->Size();
  } else {
    ulHashKeys = m_pdrgpexprOuterKeys->Size();
  }

  for (ULONG ul = 0; ul < ulHashKeys; ul++) {
    if (FNullableHashKey(ul, pcrsNotNull, fInner)) {
      return true;
    }
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::FNullableHashKey
//
//	@doc:
//		Check whether a hash key is nullable
//
//---------------------------------------------------------------------------
BOOL CPhysicalHashJoin::FNullableHashKey(ULONG ulKey, CColRefSet *pcrsNotNull, BOOL fInner) const {
  COperator *pop = nullptr;
  if (fInner) {
    pop = (*m_pdrgpexprInnerKeys)[ulKey]->Pop();
  } else {
    pop = (*m_pdrgpexprOuterKeys)[ulKey]->Pop();
  }
  EOperatorId op_id = pop->Eopid();

  if (COperator::EopScalarIdent == op_id) {
    const CColRef *colref = CScalarIdent::PopConvert(pop)->Pcr();
    return (!pcrsNotNull->FMember(colref));
  }

  if (COperator::EopScalarConst == op_id) {
    return CScalarConst::PopConvert(pop)->GetDatum()->IsNull();
  }

  // be conservative for all other scalar expressions where we cannot easily
  // determine nullability
  return true;
}

void CPhysicalHashJoin::CreateOptRequests() {
  // With DP enabled, there are several (max 10 controlled by macro)
  // alternatives generated for a join tree and during optimization of those
  // alternatives expressions PS is inserted in almost all the groups possibly.
  // However, if DP is turned off, i.e in query or greedy join order,
  // PS must be inserted in the group with DTS else in some cases HJ plan
  // cannot be created. So, to ensure pushing PS without DPE 2 partition
  // propagation request are required if DP is disabled.
  //    Req 0 => Push PS with considering DPE possibility
  //    Req 1 => Push PS without considering DPE possibility
  // Ex case: select * from non_part_tbl1 t1, part_tbl t2, non_part_tbl2 t3
  // where t1.b = t2.b and t2.b = t3.b;
  // Note: b is the partitioned column for part_tbl. If DP is turned off, HJ
  // will not be created for the above query if we send only 1 request.
  // Also, increasing the number of request increases the optimization time, so
  // set 2 only when needed.
  //
  // There are also cases where greedy does generate a better plan
  // without DPE. This adds some overhead (<10%)to optimization time in
  // some cases, but can create better alternatives to DPE, so
  // we also generate this additional request for expressions that originated
  // from CXformExpandNAryJoinGreedy.
  CPhysicalJoin *physical_join = dynamic_cast<CPhysicalJoin *>(this);
  if ((GPOPT_FDISABLED_XFORM(CXform::ExfExpandNAryJoinDP) && GPOPT_FDISABLED_XFORM(CXform::ExfExpandNAryJoinDPv2)) ||
      physical_join->OriginXform() == CXform::ExfExpandNAryJoinGreedy) {
    SetPartPropagateRequests(2);
  } else {
    SetPartPropagateRequests(1);
  }
}

CExpression *CPhysicalHashJoin::PexprJoinPredOnPartKeys(CMemoryPool *mp, CExpression *pexprScalar,
                                                        CPartKeysArray *pdrgppartkeys,
                                                        CColRefSet *pcrsAllowedRefs) const {
  GPOS_ASSERT(nullptr != pcrsAllowedRefs);

  CExpression *pexprPred = nullptr;
  for (ULONG ulKey = 0; nullptr == pexprPred && ulKey < pdrgppartkeys->Size(); ulKey++) {
    // get partition key
    CColRef2dArray *pdrgpdrgpcrPartKeys = (*pdrgppartkeys)[ulKey]->Pdrgpdrgpcr();

    // try to generate a request with dynamic partition selection
    pexprPred = CPredicateUtils::PexprExtractPredicatesOnPartKeys(mp, pexprScalar, pdrgpdrgpcrPartKeys, pcrsAllowedRefs,
                                                                  true  // fUseConstraints
    );
  }

  return pexprPred;
}

CPartitionPropagationSpec *CPhysicalHashJoin::PppsRequiredForJoins(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                                   CPartitionPropagationSpec *pppsRequired,
                                                                   ULONG child_index, CDrvdPropArray *pdrgpdpCtxt,
                                                                   ULONG ulOptReq) const {
  GPOS_ASSERT(nullptr != pppsRequired);
  GPOS_ASSERT(nullptr != pdrgpdpCtxt);

  CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2 /*child_index*/);

  CColRefSet *pcrsOutputInner = exprhdl.DeriveOutputColumns(1);

  CPartitionPropagationSpec *pps_result;
  if (ulOptReq == 0) {
    // DPE: create a new request
    pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);

    // Extract the partition info of the outer child.
    // Info in CPartInfo is added at the logical level. We add information
    // about consumers during that stage. During the physical implementation,
    // for every consumer, we check if we have to insert a consumer/propagator
    // in PppsRequired()
    CPartInfo *part_info_outer = exprhdl.DerivePartitionInfo(0);

    // Extracting the information of existing partition table consumers.
    // For every consumer(Dynamic Table Scan, identified by scan-id),
    // if PppsRequired() is called for inner child, we can add a propagator.
    // if PppsRequired() is called for outer child, we can add a consumer.
    for (ULONG ul = 0; ul < part_info_outer->UlConsumers(); ++ul) {
      ULONG scan_id = part_info_outer->ScanId(ul);
      IMDId *rel_mdid = part_info_outer->GetRelMdId(ul);
      CPartKeysArray *part_keys_array = part_info_outer->Pdrgppartkeys(ul);

      CExpression *pexprCmp =
          PexprJoinPredOnPartKeys(mp, pexprScalar, part_keys_array, pcrsOutputInner /* pcrsAllowedRefs*/);

      // If we don't have predicate on partition keys, then partition
      // elimination won't work, so we don't add a Consumer or Propagator
      if (pexprCmp == nullptr) {
        continue;
      }

      // For outer child(index=0), we insert Consumer, if a Partition
      // Selector exist in the inner child for a scan-id.
      if (child_index == 0) {
        // For the inner child, we extract the derived PPS.
        CPartitionPropagationSpec *pps_inner = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Ppps();

        // In the derived plan properties of the inner child,
        // we check if a partition selector exist for the given scan-id
        // If found, we insert a corresponding 'Consumer' in the outer child
        CBitSet *selector_ids = GPOS_NEW(mp) CBitSet(mp, *pps_inner->SelectorIds(scan_id));

        // For the identified 'partition selector' we insert a consumer.
        // This will a form part of our required properties, i.e. for the
        // given 'partition selector', we require a 'Consumer'
        pps_result->Insert(scan_id, CPartitionPropagationSpec::EpptConsumer, rel_mdid, selector_ids,
                           nullptr /* expr */);
        selector_ids->Release();
      } else {
        // For inner child (index=1), we insert a propagator given that
        // we have predicate on the partition keys
        GPOS_ASSERT(child_index == 1);
        pps_result->Insert(scan_id, CPartitionPropagationSpec::EpptPropagator, rel_mdid, nullptr, pexprCmp);
      }
      pexprCmp->Release();
    }

    // Now for the input 'pppsRequired' & 'child_index' we check if any
    // other consumer is required to be added in the pps_result.
    // 1. We prepare a list of allowed scan-ids for the input child. These scan-ids
    // we defined at the logical level.
    // 2. For each of the scan-ids, that exist in pppsRequired, we check if
    // they exist in 'allowed list' and are of type Consumer.
    // 3. For all such scan-ids, we update our computed required props in pps_result
    // Thus based we have computed the required properties for the operator based
    // on the input from higher level(using input pppsRequired) and our own
    // operator specific rules (as in the for loop above)
    CBitSet *allowed_scan_ids = GPOS_NEW(mp) CBitSet(mp);
    CPartInfo *part_info = exprhdl.DerivePartitionInfo(child_index);
    for (ULONG ul = 0; ul < part_info->UlConsumers(); ++ul) {
      ULONG scan_id = part_info->ScanId(ul);
      allowed_scan_ids->ExchangeSet(scan_id);
    }
    pps_result->InsertAllowedConsumers(pppsRequired, allowed_scan_ids);
    allowed_scan_ids->Release();
  } else {
    // No DPE: pass through requests
    pps_result = CPhysical::PppsRequired(mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, ulOptReq);
  }
  return pps_result;
}

// In the following function, we are generating the Derived property :
// "Partition Propagation Spec" of the join.
// Since Property derivation takes place in a bottom-up fashion, this operator
// derives the information from its child and passes it up. In this function
// we are focussing only on the "Partition Propagation Spec" of the children
CPartitionPropagationSpec *CPhysicalHashJoin::PppsDeriveForJoins(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
  CPartitionPropagationSpec *pps_outer = exprhdl.Pdpplan(0)->Ppps();
  CPartitionPropagationSpec *pps_inner = exprhdl.Pdpplan(1)->Ppps();

  CPartitionPropagationSpec *pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
  pps_result->InsertAll(pps_outer);
  pps_result->InsertAllResolve(pps_inner);

  return pps_result;
}
// EOF
