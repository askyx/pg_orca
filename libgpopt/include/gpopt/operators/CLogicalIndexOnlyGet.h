//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2023 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CLogicalIndexOnlyGet.h
//
//	@doc:
//		Basic index accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIndexOnlyGet_H
#define GPOPT_CLogicalIndexOnlyGet_H

#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CLogicalIndexGet.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declarations
class CName;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalIndexOnlyGet
//
//	@doc:
//		Basic index accessor
//
//---------------------------------------------------------------------------
class CLogicalIndexOnlyGet : public CLogicalIndexGet {
 public:
  CLogicalIndexOnlyGet(const CLogicalIndexOnlyGet &) = delete;

  // ctors
  explicit CLogicalIndexOnlyGet(CMemoryPool *mp) : CLogicalIndexGet(mp) {}

  CLogicalIndexOnlyGet(CMemoryPool *mp, const IMDIndex *pmdindex, CTableDescriptor *ptabdesc, uint32_t ulOriginOpId,
                       const CName *pnameAlias, CColRefArray *pdrgpcrOutput, uint32_t ulUnindexedPredColCount,
                       EIndexScanDirection scan_direction)
      : CLogicalIndexGet(mp, pmdindex, ptabdesc, ulOriginOpId, pnameAlias, pdrgpcrOutput, ulUnindexedPredColCount,
                         scan_direction) {}

  // ident accessors
  EOperatorId Eopid() const override { return EopLogicalIndexOnlyGet; }

  // return a string for operator name
  const char *SzId() const override { return "CLogicalIndexOnlyGet"; }

  //---------------------------------------------------------------------------
  //	@function:
  //		CLogicalIndexGet::Matches
  //
  //	@doc:
  //		Match function on operator level
  //
  //---------------------------------------------------------------------------
  bool Matches(COperator *pop) const override { return CUtils::FMatchIndex(this, pop); }

  //---------------------------------------------------------------------------
  //	@function:
  //		CLogicalIndexGet::PxfsCandidates
  //
  //	@doc:
  //		Get candidate xforms
  //
  //---------------------------------------------------------------------------
  CXformSet *PxfsCandidates(CMemoryPool *mp) const override {
    CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

    (void)xform_set->ExchangeSet(CXform::ExfIndexOnlyGet2IndexOnlyScan);

    return xform_set;
  }

  //-------------------------------------------------------------------------------------
  // conversion function
  //-------------------------------------------------------------------------------------

  static CLogicalIndexOnlyGet *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopLogicalIndexOnlyGet == pop->Eopid());

    return dynamic_cast<CLogicalIndexOnlyGet *>(pop);
  }

};  // class CLogicalIndexOnlyGet

}  // namespace gpopt

#endif  // !GPOPT_CLogicalIndexOnlyGet_H

// EOF
