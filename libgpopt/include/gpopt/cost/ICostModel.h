//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		ICostModel.h
//
//	@doc:
//		Interface for the underlying cost model
//---------------------------------------------------------------------------

#ifndef GPOPT_ICostModel_H
#define GPOPT_ICostModel_H

#include "CCost.h"
#include "ICostModelParams.h"
#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/statistics/IStatistics.h"

// default number of rebinds (number of times a plan is executed due to rebinding to external parameters)
#define GPOPT_DEFAULT_REBINDS 1

namespace gpopt {
// fwd declarations
class CExpressionHandle;

using namespace gpos;
using namespace gpmd;
using namespace gpnaucrates;

// dynamic array of cost model params
using ICostModelParamsArray = CDynamicPtrArray<ICostModelParams::SCostParam, CleanupDelete>;

//---------------------------------------------------------------------------
//	@class:
//		ICostModel
//
//	@doc:
//		Interface for the underlying cost model
//
//---------------------------------------------------------------------------
class ICostModel : public CRefCount {
 public:
  enum ECostModelType { EcmtGPDBLegacy = 0, EcmtGPDBCalibrated = 1, EcmtGPDBExperimental = 2, EcmtSentinel = 3 };

  //---------------------------------------------------------------------------
  //	@class:
  //		SCostingStas
  //
  //	@doc:
  //		Stast information used during cost computation
  //
  //---------------------------------------------------------------------------
  class CCostingStats : public CRefCount {
   private:
    // stats of the root
    IStatistics *m_pstats;

   public:
    // ctor
    CCostingStats(IStatistics *stats) : m_pstats(stats) { GPOS_ASSERT(nullptr != stats); }

    // dtor
    ~CCostingStats() override { m_pstats->Release(); }

    // the risk of errors in cardinality estimation
    uint32_t StatsEstimationRisk() const { return m_pstats->StatsEstimationRisk(); }

    // look up the number of distinct values of a particular column
    CDouble GetNDVs(const CColRef *colref) { return m_pstats->GetNDVs(colref); }

    // look up the number of distinct values of a particular column
    CDouble GetNullFreq(const CColRef *colref) { return m_pstats->GetNullFreq(colref); }

    // root stats getter
    IStatistics *Pstats() { return m_pstats; }
  };  // class CCostingStats

  //---------------------------------------------------------------------------
  //	@class:
  //		SCostingInfo
  //
  //	@doc:
  //		Information used during cost computation
  //
  //---------------------------------------------------------------------------
  struct SCostingInfo {
   private:
    // number of children excluding scalar children
    uint32_t m_ulChildren;

    // stats of the root
    CCostingStats *m_pcstats;

    // row estimate of root
    double m_rows;

    // width estimate of root
    double m_width;

    // number of rebinds of root
    double m_num_rebinds;

    // row estimates of child operators
    double *m_pdRowsChildren;

    // width estimates of child operators
    double *m_pdWidthChildren;

    // number of rebinds of child operators
    double *m_pdRebindsChildren;

    // computed cost of child operators
    double *m_pdCostChildren;

    // stats of the children
    CCostingStats **m_pdrgstatsChildren;

   public:
    // ctor
    SCostingInfo(CMemoryPool *mp, uint32_t ulChildren, CCostingStats *pcstats)
        : m_ulChildren(ulChildren),
          m_pcstats(pcstats),
          m_rows(0),
          m_width(0),
          m_num_rebinds(GPOPT_DEFAULT_REBINDS),
          m_pdRowsChildren(nullptr),
          m_pdWidthChildren(nullptr),
          m_pdRebindsChildren(nullptr),
          m_pdCostChildren(nullptr),
          m_pdrgstatsChildren(nullptr) {
      GPOS_ASSERT(nullptr != pcstats);
      if (0 < ulChildren) {
        m_pdRowsChildren = GPOS_NEW_ARRAY(mp, double, ulChildren);
        m_pdWidthChildren = GPOS_NEW_ARRAY(mp, double, ulChildren);
        m_pdRebindsChildren = GPOS_NEW_ARRAY(mp, double, ulChildren);
        m_pdCostChildren = GPOS_NEW_ARRAY(mp, double, ulChildren);
        m_pdrgstatsChildren = GPOS_NEW_ARRAY(mp, CCostingStats *, ulChildren);

        for (uint32_t ul = 0; ul < m_ulChildren; ul++) {
          m_pdrgstatsChildren[ul] = nullptr;
        }
      }
    }

    // dtor
    ~SCostingInfo() {
      GPOS_DELETE_ARRAY(m_pdRowsChildren);
      GPOS_DELETE_ARRAY(m_pdWidthChildren);
      GPOS_DELETE_ARRAY(m_pdRebindsChildren);
      GPOS_DELETE_ARRAY(m_pdCostChildren);

      for (uint32_t ul = 0; ul < m_ulChildren; ul++) {
        CRefCount::SafeRelease(m_pdrgstatsChildren[ul]);
      }

      GPOS_DELETE_ARRAY(m_pdrgstatsChildren);
      m_pcstats->Release();
    }

    // children accessor
    uint32_t ChildCount() const { return m_ulChildren; }

    // rows accessor
    double Rows() const { return m_rows; }

    // rows setter
    void SetRows(double rows) {
      GPOS_ASSERT(0 <= rows);

      m_rows = rows;
    }

    // width accessor
    double Width() const { return m_width; }

    // width setter
    void SetWidth(double width) {
      GPOS_ASSERT(0 <= width);

      m_width = width;
    }

    // rebinds accessor
    double NumRebinds() const { return m_num_rebinds; }

    // rebinds setter
    void SetRebinds(double num_rebinds) {
      GPOS_ASSERT(GPOPT_DEFAULT_REBINDS <= num_rebinds);

      m_num_rebinds = num_rebinds;
    }

    // children rows accessor
    double *PdRows() const { return m_pdRowsChildren; }

    // child rows setter
    void SetChildRows(uint32_t ulPos, double dRowsChild) {
      GPOS_ASSERT(0 <= dRowsChild);
      GPOS_ASSERT(ulPos < m_ulChildren);

      m_pdRowsChildren[ulPos] = dRowsChild;
    }

    // children width accessor
    double *GetWidth() const { return m_pdWidthChildren; }

    // child width setter
    void SetChildWidth(uint32_t ulPos, double dWidthChild) {
      GPOS_ASSERT(0 <= dWidthChild);
      GPOS_ASSERT(ulPos < m_ulChildren);

      m_pdWidthChildren[ulPos] = dWidthChild;
    }

    // children rebinds accessor
    double *PdRebinds() const { return m_pdRebindsChildren; }

    // child rebinds setter
    void SetChildRebinds(uint32_t ulPos, double dRebindsChild) {
      GPOS_ASSERT(GPOPT_DEFAULT_REBINDS <= dRebindsChild);
      GPOS_ASSERT(ulPos < m_ulChildren);

      m_pdRebindsChildren[ulPos] = dRebindsChild;
    }

    // children cost accessor
    double *PdCost() const { return m_pdCostChildren; }

    // child cost setter
    void SetChildCost(uint32_t ulPos, double dCostChild) {
      GPOS_ASSERT(0 <= dCostChild);
      GPOS_ASSERT(ulPos < m_ulChildren);

      m_pdCostChildren[ulPos] = dCostChild;
    }

    // child stats setter
    void SetChildStats(uint32_t ulPos, CCostingStats *child_stats) { m_pdrgstatsChildren[ulPos] = child_stats; }

    // return additional cost statistics
    CCostingStats *Pcstats() const { return m_pcstats; }

    // return additional child statistics
    CCostingStats *Pcstats(uint32_t child_index) const { return m_pdrgstatsChildren[child_index]; }

  };  // struct SCostingInfo

  // return number of rows per host
  virtual CDouble DRowsPerHost(CDouble dRowsTotal) const = 0;

  // return cost model parameters
  virtual ICostModelParams *GetCostModelParams() const = 0;

  // main driver for cost computation
  virtual CCost Cost(CExpressionHandle &exprhdl, const SCostingInfo *pci) const = 0;

  // cost model type
  virtual ECostModelType Ecmt() const = 0;

  // set cost model params
  void SetParams(ICostModelParamsArray *pdrgpcp) const;

  // create a default cost model instance
  static ICostModel *PcmDefault(CMemoryPool *mp);
};
}  // namespace gpopt

#endif  // !GPOPT_ICostModel_H

// EOF
