//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPartialPlan.h
//
//	@doc:
//
//		A partial plan is a group expression where none (or not all) of its
//		optimal child plans are discovered yet,
//		by assuming the smallest possible cost of unknown child plans, a partial
//		plan's cost gives a lower bound on the cost of the corresponding complete plan,
//		this information is used to prune the optimization search space during branch
//		and bound search
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartialPlan_H
#define GPOPT_CPartialPlan_H

#include "gpopt/base/CReqdProp.h"
#include "gpopt/cost/ICostModel.h"
#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

namespace gpopt {
using namespace gpos;

class CGroupExpression;
class CCost;
class CCostContext;

//---------------------------------------------------------------------------
//	@class:
//		CPartialPlan
//
//	@doc:
//		Description of partial plans created during optimization
//
//---------------------------------------------------------------------------
class CPartialPlan : public CRefCount {
 private:
  // root group expression
  CGroupExpression *m_pgexpr;

  // required plan properties of root operator
  CReqdPropPlan *m_prpp;

  // cost context of known child plan -- can be null if no child plans are known
  CCostContext *m_pccChild;

  // index of known child plan
  uint32_t m_ulChildIndex;

  // extract costing info from children
  void ExtractChildrenCostingInfo(CMemoryPool *mp, ICostModel *pcm, CExpressionHandle &exprhdl,
                                  ICostModel::SCostingInfo *pci);

 public:
  CPartialPlan(const CPartialPlan &) = delete;

  // ctor
  CPartialPlan(CGroupExpression *pgexpr, CReqdPropPlan *prpp, CCostContext *pccChild, uint32_t child_index);

  // dtor
  ~CPartialPlan() override;

  // group expression accessor
  CGroupExpression *Pgexpr() const { return m_pgexpr; }

  // plan properties accessor
  CReqdPropPlan *Prpp() const { return m_prpp; }

  // child cost context accessor
  CCostContext *PccChild() const { return m_pccChild; }

  // child index accessor
  uint32_t UlChildIndex() const { return m_ulChildIndex; }

  // compute partial plan cost
  CCost CostCompute(CMemoryPool *mp);

  // hash function used for cost bounding
  static uint32_t HashValue(const CPartialPlan *ppp);

  // equality function used for for cost bounding
  static bool Equals(const CPartialPlan *pppFst, const CPartialPlan *pppSnd);

};  // class CPartialPlan
}  // namespace gpopt

#endif  // !GPOPT_CPartialPlan_H

// EOF
