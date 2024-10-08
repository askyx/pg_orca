//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CPhysicalIndexScan.h
//
//	@doc:
//		Base class for physical index scan operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalIndexScan_H
#define GPOPT_CPhysicalIndexScan_H

#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declarations
class CTableDescriptor;
class CIndexDescriptor;
class CName;
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalIndexScan
//
//	@doc:
//		Base class for physical index scan operators
//
//---------------------------------------------------------------------------
class CPhysicalIndexScan : public CPhysicalScan {
 private:
  // index descriptor
  CIndexDescriptor *m_pindexdesc;

  // origin operator id -- UINT32_MAX if operator was not generated via a transformation
  uint32_t m_ulOriginOpId;

  // order
  COrderSpec *m_pos;

  // Number of predicate not applicable on the index
  uint32_t m_ulUnindexedPredColCount;

  // index scan direction
  EIndexScanDirection m_scan_direction;

 public:
  CPhysicalIndexScan(const CPhysicalIndexScan &) = delete;

  // ctors
  CPhysicalIndexScan(CMemoryPool *mp, CIndexDescriptor *pindexdesc, CTableDescriptor *ptabdesc, uint32_t ulOriginOpId,
                     const CName *pnameAlias, CColRefArray *colref_array, COrderSpec *pos,
                     uint32_t ulUnindexedPredColCount, EIndexScanDirection scan_direction);

  // dtor
  ~CPhysicalIndexScan() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalIndexScan; }

  // operator name
  const char *SzId() const override { return "CPhysicalIndexScan"; }

  // table alias name
  const CName &NameAlias() const { return *m_pnameAlias; }

  // origin operator id -- UINT32_MAX if operator was not generated via a transformation
  uint32_t UlOriginOpId() const { return m_ulOriginOpId; }

  // index scan direction is only used for B-tree indices.
  EIndexScanDirection IndexScanDirection() const { return m_scan_direction; }

  // operator specific hash function
  uint32_t HashValue() const override;

  // match function
  bool Matches(COperator *pop) const override;

  // index descriptor
  CIndexDescriptor *Pindexdesc() const { return m_pindexdesc; }

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return true; }

  // number of predicate not applicable on the index
  uint32_t ResidualPredicateSize() const { return m_ulUnindexedPredColCount; }

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  // derive sort order
  COrderSpec *PosDerive(CMemoryPool *,       // mp
                        CExpressionHandle &  // exprhdl
  ) const override {
    m_pos->AddRef();
    return m_pos;
  }

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return order property enforcing type for this operator
  CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

  // conversion function
  static CPhysicalIndexScan *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalIndexScan == pop->Eopid());

    return dynamic_cast<CPhysicalIndexScan *>(pop);
  }

  // statistics derivation during costing
  IStatistics *PstatsDerive(CMemoryPool *,        // mp
                            CExpressionHandle &,  // exprhdl
                            CReqdPropPlan *,      // prpplan
                            IStatisticsArray *    // stats_ctxt
  ) const override {
    GPOS_ASSERT(!"stats derivation during costing for index scan is invalid");

    return nullptr;
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

};  // class CPhysicalIndexScan

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalIndexScan_H

// EOF
