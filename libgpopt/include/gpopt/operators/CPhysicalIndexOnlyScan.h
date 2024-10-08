//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//
//	@filename:
//		CPhysicalIndexOnlyScan.h
//
//	@doc:
//		Base class for physical index only scan operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalIndexOnlyScan_H
#define GPOPT_CPhysicalIndexOnlyScan_H

#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declarations
class CName;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalIndexOnlyScan
//
//	@doc:
//		Base class for physical index only scan operators
//
//---------------------------------------------------------------------------
class CPhysicalIndexOnlyScan : public CPhysicalScan {
 private:
  // index descriptor
  CIndexDescriptor *m_pindexdesc;

  // origin operator id -- UINT32_MAX if operator was not generated via a transformation
  uint32_t m_ulOriginOpId;

  // order
  COrderSpec *m_pos;

  // index scan direction
  EIndexScanDirection m_scan_direction;

 public:
  CPhysicalIndexOnlyScan(const CPhysicalIndexOnlyScan &) = delete;

  // ctors
  CPhysicalIndexOnlyScan(CMemoryPool *mp, CIndexDescriptor *pindexdesc, CTableDescriptor *ptabdesc,
                         uint32_t ulOriginOpId, const CName *pnameAlias, CColRefArray *colref_array, COrderSpec *pos,
                         EIndexScanDirection scan_direction);

  // dtor
  ~CPhysicalIndexOnlyScan() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalIndexOnlyScan; }

  // operator name
  const char *SzId() const override { return "CPhysicalIndexOnlyScan"; }

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
  static CPhysicalIndexOnlyScan *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalIndexOnlyScan == pop->Eopid());

    return dynamic_cast<CPhysicalIndexOnlyScan *>(pop);
  }

  // statistics derivation during costing
  IStatistics *PstatsDerive(CMemoryPool *,        // mp
                            CExpressionHandle &,  // exprhdl
                            CReqdPropPlan *,      // prpplan
                            IStatisticsArray *    // stats_ctxt
  ) const override {
    GPOS_ASSERT(!"stats derivation during costing for index only scan is invalid");

    return nullptr;
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

};  // class CPhysicalIndexOnlyScan

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalIndexOnlyScan_H

// EOF
