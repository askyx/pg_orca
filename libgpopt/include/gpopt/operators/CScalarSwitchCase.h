//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarSwitchCase.h
//
//	@doc:
//		Scalar SwitchCase operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSwitchCase_H
#define GPOPT_CScalarSwitchCase_H

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarSwitchCase
//
//	@doc:
//		Scalar SwitchCase operator
//
//---------------------------------------------------------------------------
class CScalarSwitchCase : public CScalar {
 private:
 public:
  CScalarSwitchCase(const CScalarSwitchCase &) = delete;

  // ctor
  explicit CScalarSwitchCase(CMemoryPool *mp);

  // dtor
  ~CScalarSwitchCase() override = default;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarSwitchCase; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarSwitchCase"; }

  // match function
  bool Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return true; }

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *,       // mp,
                                        UlongToColRefMap *,  // colref_mapping,
                                        bool                 // must_exist
                                        ) override {
    return PopCopyDefault();
  }

  IMDId *MdidType() const override {
    GPOS_ASSERT(!"Invalid function call: CScalarSwitchCase::MdidType()");
    return nullptr;
  }

  // boolean expression evaluation
  EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const override {
    return EberNullOnAllNullChildren(pdrgpulChildren);
  }

  // conversion function
  static CScalarSwitchCase *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarSwitchCase == pop->Eopid());

    return dynamic_cast<CScalarSwitchCase *>(pop);
  }

};  // class CScalarSwitchCase

}  // namespace gpopt

#endif  // !GPOPT_CScalarSwitchCase_H

// EOF
