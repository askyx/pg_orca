//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CEnumeratorConfig.h
//
//	@doc:
//		Configurations of plan enumerator
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnumeratorConfig_H
#define GPOPT_CEnumeratorConfig_H

#include "gpopt/cost/CCost.h"
#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/traceflags/traceflags.h"

#define GPOPT_UNBOUNDED_COST_THRESHOLD 0.0

namespace gpos {
class CWStringDynamic;
}

namespace gpopt {
using namespace gpos;

// fwd declarations
class CExpression;

// type definition of plan checker
using FnPlanChecker = bool(CExpression *);

//---------------------------------------------------------------------------
//	@class:
//		CEnumeratorConfig
//
//	@doc:
//		Configurations of plan enumerator
//
//---------------------------------------------------------------------------
class CEnumeratorConfig : public CRefCount {
 private:
  //---------------------------------------------------------------------------
  //	@class:
  //		SSamplePlan
  //
  //	@doc:
  //		Internal structure to represent samples of plan space
  //
  //---------------------------------------------------------------------------
  struct SSamplePlan {
   private:
    // plan id
    uint64_t m_plan_id;

    // plan cost
    CCost m_cost;

   public:
    // ctor
    SSamplePlan(uint64_t plan_id, CCost cost) : m_plan_id(plan_id), m_cost(cost) {}

    // dtor
    virtual ~SSamplePlan() = default;

    // return plan id
    uint64_t GetPlanId() const { return m_plan_id; }

    // return plan cost
    CCost Cost() const { return m_cost; }

  };  // struct SSamplePlan

  // array og unsigned long long int
  using SSamplePlanArray = CDynamicPtrArray<SSamplePlan, CleanupDelete>;

  // memory pool
  CMemoryPool *m_mp;

  // identifier of chosen plan
  uint64_t m_plan_id;

  // size of plan space
  uint64_t m_ullSpaceSize;

  // number of required samples
  uint64_t m_ullInputSamples;

  // cost of best plan found
  CCost m_costBest;

  // max cost of a created plan sample
  CCost m_costMax;

  // max cost of accepted samples as a ratio to best plan cost
  CDouble m_dCostThreshold;

  // sampled plans
  SSamplePlanArray *m_pdrgpsp;

  // step value used in fitting cost distribution
  CDouble m_dStep;

  // x-values of fitted cost distribution
  double *m_pdX;

  // y-values of fitted cost distribution
  double *m_pdY;

  // size of fitted cost distribution
  uint32_t m_ulDistrSize;

  // restrict plan sampling to plans satisfying required properties
  bool m_fSampleValidPlans;

  // plan checker function
  FnPlanChecker *m_pfpc;

  // initialize size of cost distribution
  void InitCostDistrSize();

  // compute Gaussian probability value
  static double DGaussian(double d, double dMean, double dStd);

 public:
  CEnumeratorConfig(const CEnumeratorConfig &) = delete;

  // ctor
  CEnumeratorConfig(CMemoryPool *mp, uint64_t plan_id, uint64_t ullSamples,
                    CDouble cost_threshold = GPOPT_UNBOUNDED_COST_THRESHOLD);

  // dtor
  ~CEnumeratorConfig() override;

  // return plan id
  uint64_t GetPlanId() const { return m_plan_id; }

  // return enumerated space size
  uint64_t GetPlanSpaceSize() const { return m_ullSpaceSize; }

  // set plan space size
  void SetPlanSpaceSize(uint64_t ullSpaceSize) { m_ullSpaceSize = ullSpaceSize; }

  // return number of required samples
  uint64_t UllInputSamples() const { return m_ullInputSamples; }

  // return number of created samples
  uint32_t UlCreatedSamples() const { return m_pdrgpsp->Size(); }

  // set plan id
  void SetPlanId(uint64_t plan_id) { m_plan_id = plan_id; }

  // return cost threshold
  CDouble DCostThreshold() const { return m_dCostThreshold; }

  // return id of a plan sample
  uint64_t UllPlanSample(uint32_t ulPos) const { return (*m_pdrgpsp)[ulPos]->GetPlanId(); }

  // set cost of best plan found
  void SetBestCost(CCost cost) { m_costBest = cost; }

  // return cost of best plan found
  CCost CostBest() const { return m_costBest; }

  // return cost of a plan sample
  CCost CostPlanSample(uint32_t ulPos) const { return (*m_pdrgpsp)[ulPos]->Cost(); }

  // add a new plan to sample
  bool FAddSample(uint64_t plan_id, CCost cost);

  // clear samples
  void ClearSamples();

  // return x-value of cost distribution
  CDouble DCostDistrX(uint32_t ulPos) const;

  // return y-value of cost distribution
  CDouble DCostDistrY(uint32_t ulPos) const;

  // fit cost distribution on generated samples
  void FitCostDistribution();

  // return size of fitted cost distribution
  uint32_t UlCostDistrSize() const { return m_ulDistrSize; }

  // is enumeration enabled?
  static bool FEnumerate() { return GPOS_FTRACE(EopttraceEnumeratePlans); }

  // is sampling enabled?
  static bool FSample() { return GPOS_FTRACE(EopttraceSamplePlans); }

  // return plan checker function
  FnPlanChecker *Pfpc() const { return m_pfpc; }

  // set plan checker function
  void SetPlanChecker(FnPlanChecker *pfpc) {
    GPOS_ASSERT(nullptr != pfpc);

    m_pfpc = pfpc;
  }

  // restrict sampling to plans satisfying required properties
  // we need to change settings for testing
  void SetSampleValidPlans(bool fSampleValidPlans) { m_fSampleValidPlans = fSampleValidPlans; }

  // return true if sampling can only generate valid plans
  bool FSampleValidPlans() const { return m_fSampleValidPlans; }

  // check given plan using PlanChecker function
  bool FCheckPlan(CExpression *pexpr) const {
    GPOS_ASSERT(nullptr != pexpr);

    if (nullptr != m_pfpc) {
      return m_pfpc(pexpr);
    }

    return true;
  }

  // dump fitted cost distribution to an output file
  void DumpCostDistr(CWStringDynamic *str, uint32_t ulSessionId, uint32_t ulCommandId);

  // print ids of plans in the generated sample
  void PrintPlanSample() const;

  // compute Gaussian kernel density
  static void GussianKernelDensity(const double *pdObervationX, const double *pdObervationY, uint32_t ulObservations,
                                   const double *pdX, double *pdY, uint32_t size);

  // generate default enumerator configurations
  static CEnumeratorConfig *PecDefault(CMemoryPool *mp) {
    return GPOS_NEW(mp) CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/);
  }

  // generate enumerator configuration for a given plan id
  static CEnumeratorConfig *GetEnumeratorCfg(CMemoryPool *mp, uint64_t plan_id) {
    return GPOS_NEW(mp) CEnumeratorConfig(mp, plan_id, 0 /*ullSamples*/);
  }

};  // class CEnumeratorConfig

}  // namespace gpopt

#endif  // !GPOPT_CEnumeratorConfig_H

// EOF
