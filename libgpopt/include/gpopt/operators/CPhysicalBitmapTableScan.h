//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalBitmapTableScan.h
//
//	@doc:
//		Bitmap table scan physical operator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CPhysicalBitmapTableScan_H
#define GPOPT_CPhysicalBitmapTableScan_H

#include "gpopt/operators/CPhysicalScan.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declarations
class CTableDescriptor;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalBitmapTableScan
//
//	@doc:
//		Bitmap table scan physical operator
//
//---------------------------------------------------------------------------
class CPhysicalBitmapTableScan : public CPhysicalScan {
 private:
  // origin operator id -- UINT32_MAX if operator was not generated via a transformation
  uint32_t m_ulOriginOpId;

 public:
  CPhysicalBitmapTableScan(const CPhysicalBitmapTableScan &) = delete;

  // ctor
  CPhysicalBitmapTableScan(CMemoryPool *mp, CTableDescriptor *ptabdesc, uint32_t ulOriginOpId,
                           const CName *pnameTableAlias, CColRefArray *pdrgpcrOutput);

  // dtor
  ~CPhysicalBitmapTableScan() override = default;

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalBitmapTableScan; }

  // operator name
  const char *SzId() const override { return "CPhysicalBitmapTableScan"; }

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return true; }

  // origin operator id -- UINT32_MAX if operator was not generated via a transformation
  uint32_t UlOriginOpId() const { return m_ulOriginOpId; }

  // operator specific hash function
  uint32_t HashValue() const override;

  // match function
  bool Matches(COperator *pop) const override;

  // statistics derivation during costing
  IStatistics *PstatsDerive(CMemoryPool *,        // mp
                            CExpressionHandle &,  // exprhdl
                            CReqdPropPlan *,      // prpplan
                            IStatisticsArray *    // stats_ctxt
  ) const override {
    GPOS_ASSERT(!"stats derivation during costing for bitmap table scan is invalid");

    return nullptr;
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

  // conversion function
  static CPhysicalBitmapTableScan *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalBitmapTableScan == pop->Eopid());

    return dynamic_cast<CPhysicalBitmapTableScan *>(pop);
  }
};
}  // namespace gpopt

#endif  // !GPOPT_CPhysicalBitmapTableScan_H

// EOF
