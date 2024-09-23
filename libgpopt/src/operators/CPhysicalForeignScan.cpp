//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalForeignScan.cpp
//
//	@doc:
//		Implementation of foreign scan operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalForeignScan.h"

#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalForeignScan::CPhysicalForeignScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalForeignScan::CPhysicalForeignScan(CMemoryPool *mp, const CName *pnameAlias, CTableDescriptor *ptabdesc,
                                           CColRefArray *pdrgpcrOutput)
    : CPhysicalTableScan(mp, pnameAlias, ptabdesc, pdrgpcrOutput) {}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalForeignScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
bool CPhysicalForeignScan::Matches(COperator *pop) const {
  if (Eopid() != pop->Eopid()) {
    return false;
  }

  CPhysicalForeignScan *popForeignScan = CPhysicalForeignScan::PopConvert(pop);
  return m_ptabdesc->MDId()->Equals(popForeignScan->Ptabdesc()->MDId()) &&
         m_pdrgpcrOutput->Equals(popForeignScan->PdrgpcrOutput());
}
