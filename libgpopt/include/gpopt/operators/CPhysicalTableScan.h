//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalTableScan.h
//
//	@doc:
//		Table scan operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalTableScan_H
#define GPOPT_CPhysicalTableScan_H

#include "gpopt/operators/CPhysicalScan.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalTableScan
//
//	@doc:
//		Table scan operator
//
//---------------------------------------------------------------------------
class CPhysicalTableScan : public CPhysicalScan {
 private:
  // private copy ctor
  CPhysicalTableScan(const CPhysicalTableScan &);

 public:
  // ctors
  explicit CPhysicalTableScan(CMemoryPool *mp);
  CPhysicalTableScan(CMemoryPool *, const CName *, CTableDescriptor *, CColRefArray *);

  // ident accessors
  EOperatorId Eopid() const override { return EopPhysicalTableScan; }

  // return a string for operator name
  const char *SzId() const override { return "CPhysicalTableScan"; }

  // operator specific hash function
  uint32_t HashValue() const override;

  // match function
  bool Matches(COperator *) const override;

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // debug print
  IOstream &OsPrint(IOstream &) const override;

  // conversion function
  static CPhysicalTableScan *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopPhysicalTableScan == pop->Eopid() || EopPhysicalForeignScan == pop->Eopid());

    return dynamic_cast<CPhysicalTableScan *>(pop);
  }

  // statistics derivation during costing
  IStatistics *PstatsDerive(CMemoryPool *,        // mp
                            CExpressionHandle &,  // exprhdl
                            CReqdPropPlan *,      // prpplan
                            IStatisticsArray *    // stats_ctxt
  ) const override {
    GPOS_ASSERT(!"stats derivation during costing for table scan is invalid");

    return nullptr;
  }

};  // class CPhysicalTableScan

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalTableScan_H

// EOF
