//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSerialUnionAll.cpp
//
//	@doc:
//		Implementation of physical union all operator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalSerialUnionAll.h"

#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::CPhysicalSerialUnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSerialUnionAll::CPhysicalSerialUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
                                                 CColRef2dArray *pdrgpdrgpcrInput)
    : CPhysicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput) {
  // UnionAll creates 3 distribution requests to enforce
  // distribution of its children:
  //
  // Request 1: HASH
  // Pass hashed distribution (requested from above) to child
  // operators and match request exactly
  //
  // Request 2: NON-SINGLETON, matching_dist
  // Request NON-SINGLETON from the outer child, and match the
  // requests on the rest children based what dist spec the outer
  // child NOW delivers (derived from property plan). Note, the
  // NON-SINGLETON that we request from the outer child is not
  // satisfiable by REPLICATED.
  //
  // Request 3: ANY, matching_dist
  // Request ANY distribution from the outer child, and match the
  // requests on the rest children based on what dist spec the outer
  // child delivers. Note, no enforcement should ever be applied to
  // the outer child, because ANY is satisfiable by all specs.
  //
  // If request 1 falls through, request 3 serves as the
  // backup request. Duplicate requests would eventually be
  // deduplicated.

  SetDistrRequests(3 /*ulDistrReq*/);
  GPOS_ASSERT(0 < UlDistrRequests());
}

CPhysicalSerialUnionAll::~CPhysicalSerialUnionAll() = default;
