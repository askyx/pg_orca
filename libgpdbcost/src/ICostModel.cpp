//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		ICostModel.cpp
//
//	@doc:
//		Cost model implementation
//---------------------------------------------------------------------------

#include "gpdbcost/CCostModelGPDB.h"
#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"

using namespace gpopt;
using namespace gpdbcost;

//---------------------------------------------------------------------------
//	@function:
//		ICostModel::PcmDefault
//
//	@doc:
//		Create default cost model
//
//---------------------------------------------------------------------------
ICostModel *ICostModel::PcmDefault(CMemoryPool *mp) {
  return GPOS_NEW(mp) CCostModelGPDB(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		ICostModel::SetParams
//
//	@doc:
//		Set cost model params
//
//---------------------------------------------------------------------------
void ICostModel::SetParams(ICostModelParamsArray *pdrgpcp) const {
  if (nullptr == pdrgpcp) {
    return;
  }

  // overwrite default values of cost model parameters
  const uint32_t size = pdrgpcp->Size();
  for (uint32_t ul = 0; ul < size; ul++) {
    ICostModelParams::SCostParam *pcp = (*pdrgpcp)[ul];
    GetCostModelParams()->SetParam(pcp->Id(), pcp->Get(), pcp->GetLowerBoundVal(), pcp->GetUpperBoundVal());
  }
}

// EOF
