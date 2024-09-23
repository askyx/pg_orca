//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarProjectList.h
//
//	@doc:
//		Projection list
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarProjectList_H
#define GPOPT_CScalarProjectList_H

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarProjectList
//
//	@doc:
//		Projection list operator
//
//---------------------------------------------------------------------------
class CScalarProjectList : public CScalar {
 private:
 public:
  CScalarProjectList(const CScalarProjectList &) = delete;

  // ctor
  explicit CScalarProjectList(CMemoryPool *mp);

  // dtor
  ~CScalarProjectList() override = default;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarProjectList; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarProjectList"; }

  // match function
  bool Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override;

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *,       // mp,
                                        UlongToColRefMap *,  // colref_mapping,
                                        bool                 // must_exist
                                        ) override {
    return PopCopyDefault();
  }

  // conversion function
  static CScalarProjectList *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarProjectList == pop->Eopid());

    return dynamic_cast<CScalarProjectList *>(pop);
  }

  IMDId *MdidType() const override {
    GPOS_ASSERT(!"Invalid function call: CScalarProjectList::MdidType()");
    return nullptr;
  }

  // return number of distinct aggs in project list attached to given handle
  static uint32_t UlDistinctAggs(CExpressionHandle &exprhdl);

  // return number of ordered aggs in project list attached to given handle
  static uint32_t UlOrderedAggs(CExpressionHandle &exprhdl);

  // check if a project list has multiple distinct aggregates
  static bool FHasMultipleDistinctAggs(CExpressionHandle &exprhdl);

  // check if a project list has a scalar func
  static bool FHasScalarFunc(CExpressionHandle &exprhdl);

  // check if a project list has only replication safe agg funcs
  static bool FContainsOnlyReplicationSafeAggFuncs(CExpressionHandle &exprhdl);
};  // class CScalarProjectList

}  // namespace gpopt

#endif  // !GPOPT_CScalarProjectList_H

// EOF
