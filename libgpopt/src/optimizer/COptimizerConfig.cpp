//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		COptimizerConfig.cpp
//
//	@doc:
//		Implementation of configuration used by the optimizer
//---------------------------------------------------------------------------

#include "gpopt/optimizer/COptimizerConfig.h"

#include "gpopt/cost/ICostModel.h"
#include "gpos/base.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/string/CWStringDynamic.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::COptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptimizerConfig::COptimizerConfig(CEnumeratorConfig *pec, CStatisticsConfig *stats_config, CCTEConfig *pcteconf,
                                   ICostModel *cost_model, CHint *phint, CWindowOids *pwindowoids)
    : m_enumerator_cfg(pec),
      m_stats_conf(stats_config),
      m_cte_conf(pcteconf),
      m_cost_model(cost_model),
      m_hint(phint),
      m_window_oids(pwindowoids) {
  GPOS_ASSERT(nullptr != pec);
  GPOS_ASSERT(nullptr != stats_config);
  GPOS_ASSERT(nullptr != pcteconf);
  GPOS_ASSERT(nullptr != m_cost_model);
  GPOS_ASSERT(nullptr != phint);
  GPOS_ASSERT(nullptr != m_window_oids);
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::~COptimizerConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COptimizerConfig::~COptimizerConfig() {
  m_enumerator_cfg->Release();
  m_stats_conf->Release();
  m_cte_conf->Release();
  m_cost_model->Release();
  m_hint->Release();
  m_window_oids->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration
//
//---------------------------------------------------------------------------
COptimizerConfig *COptimizerConfig::PoconfDefault(CMemoryPool *mp) {
  return GPOS_NEW(mp)
      COptimizerConfig(GPOS_NEW(mp) CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/),
                       CStatisticsConfig::PstatsconfDefault(mp), CCTEConfig::PcteconfDefault(mp),
                       ICostModel::PcmDefault(mp), CHint::PhintDefault(mp), CWindowOids::GetWindowOids(mp));
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration with the given cost model
//
//---------------------------------------------------------------------------
COptimizerConfig *COptimizerConfig::PoconfDefault(CMemoryPool *mp, ICostModel *pcm) {
  GPOS_ASSERT(nullptr != pcm);

  return GPOS_NEW(mp) COptimizerConfig(GPOS_NEW(mp) CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/),
                                       CStatisticsConfig::PstatsconfDefault(mp), CCTEConfig::PcteconfDefault(mp), pcm,
                                       CHint::PhintDefault(mp), CWindowOids::GetWindowOids(mp));
}
