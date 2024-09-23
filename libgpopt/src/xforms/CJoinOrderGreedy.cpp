//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CJoinOrderGreedy.cpp
//
//	@doc:
//		Implementation of cardinality-based join order generation with
//		delayed cross joins
//---------------------------------------------------------------------------

#include "gpopt/xforms/CJoinOrderGreedy.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/clibwrapper.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGreedy::CJoinOrderGreedy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderGreedy::CJoinOrderGreedy(CMemoryPool *pmp, CExpressionArray *pdrgpexprComponents,
                                   CExpressionArray *pdrgpexprConjuncts)
    : CJoinOrder(pmp, pdrgpexprComponents, pdrgpexprConjuncts, true /* m_include_loj_childs */),
      m_pcompResult(nullptr) {
#ifdef GPOS_DEBUG
  for (uint32_t ul = 0; ul < m_ulComps; ul++) {
    GPOS_ASSERT(nullptr != m_rgpcomp[ul]->m_pexpr->Pstats() && "stats were not derived on input component");
  }
#endif  // GPOS_DEBUG
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGreedy::~CJoinOrderGreedy
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderGreedy::~CJoinOrderGreedy() {
  CRefCount::SafeRelease(m_pcompResult);
}

// function to get the minimal cardinality join pair as the starting pair
CJoinOrder::SComponent *CJoinOrderGreedy::GetStartingJoins() {
  CDouble dMinRows(0.0);
  uint32_t ul1Counter = 0;
  uint32_t ul2Counter = 0;
  CJoinOrder::SComponent *pcompBest = GPOS_NEW(m_mp) SComponent(m_mp, nullptr /*pexpr*/);

  for (uint32_t ul1 = 0; ul1 < m_ulComps; ul1++) {
    for (uint32_t ul2 = ul1 + 1; ul2 < m_ulComps; ul2++) {
      SComponent *comp1 = m_rgpcomp[ul1];
      SComponent *comp2 = m_rgpcomp[ul2];

      if (!IsValidJoinCombination(comp1, comp2)) {
        continue;
      }

      CJoinOrder::SComponent *compTemp = PcompCombine(comp1, comp2);

      // exclude cross joins to be considered as late as possible in the join order
      if (CUtils::FCrossJoin(compTemp->m_pexpr)) {
        compTemp->Release();
        continue;
      }
      DeriveStats(compTemp->m_pexpr);
      CDouble dRows = compTemp->m_pexpr->Pstats()->Rows();
      if (dMinRows <= 0 || dRows < dMinRows) {
        ul1Counter = ul1;
        ul2Counter = ul2;
        dMinRows = dRows;
        compTemp->AddRef();
        CRefCount::SafeRelease(pcompBest);
        pcompBest = compTemp;
      }
      compTemp->Release();
    }
  }

  if ((ul1Counter == 0) && (ul2Counter == 0)) {
    pcompBest->Release();
    return nullptr;
  }

  SComponent *comp1 = m_rgpcomp[ul1Counter];
  comp1->m_fUsed = true;
  SComponent *comp2 = m_rgpcomp[ul2Counter];
  comp2->m_fUsed = true;
  pcompBest->m_fUsed = true;

  return pcompBest;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGreedy::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *CJoinOrderGreedy::PexprExpand() {
  GPOS_ASSERT(nullptr == m_pcompResult && "join order is already expanded");

  m_pcompResult = GetStartingJoins();

  if (nullptr != m_pcompResult) {
    // found atleast one non cross join
    MarkUsedEdges(m_pcompResult);
  } else {
    // every join combination is a cross join
    m_pcompResult = GPOS_NEW(m_mp) SComponent(m_mp, nullptr /*pexpr*/);
  }

  // create a bitset for all the unused components
  CBitSet *unused_components_set = GPOS_NEW(m_mp) CBitSet(m_mp);
  for (uint32_t ul = 0; ul < m_ulComps; ul++) {
    if (!m_rgpcomp[ul]->m_fUsed) {
      unused_components_set->ExchangeSet(ul);
    }
  }

  while (unused_components_set->Size() > 0) {
    // get a set of components which can be joined with m_pcompResult
    CBitSet *candidate_comp_set = GetAdjacentComponentsToJoinCandidate();

    // index for the best component that we will pick
    uint32_t best_comp_idx = UINT32_MAX;

    // if there are components available which can be joined with m_pcompResult
    // avoiding cross joins
    if (candidate_comp_set->Size() > 0) {
      // find the best join component.
      // once we find a best_comp_idx, we should re-evaluate the
      // candidate_component_set, which is done in GetAdjacentComponentsToJoinCandidate.
      // GetAdjacentComponentsToJoinCandidate identifies the connected
      // components of the last m_pcompResult which is updated in PickBestJoin.
      best_comp_idx = PickBestJoin(candidate_comp_set);
    }

    if (candidate_comp_set->Size() == 0 || UINT32_MAX == best_comp_idx) {
      // only cross joins are available. pick the unused component which will
      // result in minimal cardinality
      best_comp_idx = PickBestJoin(unused_components_set);
    }

    if (UINT32_MAX == best_comp_idx) {
      // could not pick a component to create the join tree
      unused_components_set->Release();
      candidate_comp_set->Release();
      return nullptr;
    } else {
      // remove the best component from the unused component set
      unused_components_set->ExchangeClear(best_comp_idx);
    }
    candidate_comp_set->Release();
    GPOS_ASSERT(UINT32_MAX != best_comp_idx);
  }
  unused_components_set->Release();
  GPOS_ASSERT(nullptr != m_pcompResult->m_pexpr);

  CExpression *pexprResult = m_pcompResult->m_pexpr;
  pexprResult->AddRef();

  return pexprResult;
}

/*
 * This function picks the component from the candidate_comp_set which will give
 * the minimum cardinality when joined with the m_pcompResult component
 * It then updates m_pcompResult with the best join and returns the index of
 * the component which was picked
 */
uint32_t CJoinOrderGreedy::PickBestJoin(CBitSet *candidate_comp_set) {
  SComponent *pcompBestComponent = nullptr;  // component which gives minimum cardinality when joined with m_pcompResult
  SComponent *pcompBest = nullptr;  // resulting join component using pcompBestComponent and original m_pcompResult
                                    // which gives minimum cardinality
  CDouble dMinRows = 0.0;
  uint32_t best_comp_idx = UINT32_MAX;

  CBitSetIter iter(*candidate_comp_set);
  while (iter.Advance()) {
    SComponent *pcompCurrent = m_rgpcomp[iter.Bit()];
    if (!IsValidJoinCombination(m_pcompResult, pcompCurrent)) {
      continue;
    }

    SComponent *pcompTemp = PcompCombine(m_pcompResult, pcompCurrent);
    DeriveStats(pcompTemp->m_pexpr);
    CDouble dRows = pcompTemp->m_pexpr->Pstats()->Rows();

    // pick the component which will give the lowest cardinality
    if (nullptr == pcompBestComponent || dRows < dMinRows) {
      dMinRows = dRows;
      best_comp_idx = iter.Bit();
      pcompBestComponent = pcompCurrent;
      pcompTemp->AddRef();
      CRefCount::SafeRelease(pcompBest);
      pcompBest = pcompTemp;
    }
    pcompTemp->Release();
  }

  // component could not be found
  if (UINT32_MAX == best_comp_idx) {
    return UINT32_MAX;
  }

  GPOS_ASSERT(UINT32_MAX != best_comp_idx);
  GPOS_ASSERT(nullptr != pcompBest);
  GPOS_ASSERT(!pcompBestComponent->m_fUsed);
  pcompBestComponent->m_fUsed = true;
  m_pcompResult->Release();
  m_pcompResult = pcompBest;
  MarkUsedEdges(m_pcompResult);

  return best_comp_idx;
}

/*
 * Get components that are reachable from the result component by a single edge
 */
CBitSet *CJoinOrderGreedy::GetAdjacentComponentsToJoinCandidate() {
  // iterator over index of edges in m_rgpedge array associated with this component
  CBitSetIter edges_iter(*(m_pcompResult->m_edge_set));
  CBitSet *candidate_component_set = GPOS_NEW(m_mp) CBitSet(m_mp);

  while (edges_iter.Advance()) {
    SEdge *edge = m_rgpedge[edges_iter.Bit()];
    if (!edge->m_fUsed) {
      // components connected via the edges
      candidate_component_set->Union(edge->m_pbs);
      candidate_component_set->Difference(m_pcompResult->m_pbs);
    }
  }

  return candidate_component_set;
}

// EOF
