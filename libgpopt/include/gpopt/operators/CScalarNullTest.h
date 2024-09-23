//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarNullTest.h
//
//	@doc:
//		Scalar null test
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarNullTest_H
#define GPOPT_CScalarNullTest_H

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarNullTest
//
//	@doc:
//		Scalar null test operator
//
//---------------------------------------------------------------------------
class CScalarNullTest : public CScalar {
 private:
 public:
  CScalarNullTest(const CScalarNullTest &) = delete;

  // ctor
  explicit CScalarNullTest(CMemoryPool *mp) : CScalar(mp) {}

  // dtor
  ~CScalarNullTest() override = default;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarNullTest; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarNullTest"; }

  // match function
  bool Matches(COperator *) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return false; }

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *,       // mp,
                                        UlongToColRefMap *,  // colref_mapping,
                                        bool                 // must_exist
                                        ) override {
    return PopCopyDefault();
  }

  // the type of the scalar expression
  IMDId *MdidType() const override;

  // boolean expression evaluation
  EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const override;

  // conversion function
  static CScalarNullTest *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarNullTest == pop->Eopid());

    return dynamic_cast<CScalarNullTest *>(pop);
  }

};  // class CScalarNullTest

}  // namespace gpopt

#endif  // !GPOPT_CScalarNullTest_H

// EOF
