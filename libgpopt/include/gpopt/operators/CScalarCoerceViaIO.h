//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CScalarCoerceViaIO.h
//
//	@doc:
//		Scalar CoerceViaIO operator,
//		the operator captures coercing a value from one type to another, by
//		calling the output function of the argument type, and passing the
//		result to the input function of the result type.
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoerceViaIO_H
#define GPOPT_CScalarCoerceViaIO_H

#include "gpopt/operators/CScalarCoerceBase.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarCoerceViaIO
//
//	@doc:
//		Scalar CoerceViaIO operator
//
//---------------------------------------------------------------------------
class CScalarCoerceViaIO : public CScalarCoerceBase {
 private:
 public:
  CScalarCoerceViaIO(const CScalarCoerceViaIO &) = delete;

  // ctor
  CScalarCoerceViaIO(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, ECoercionForm dxl_coerce_format,
                     int32_t location);

  // dtor
  ~CScalarCoerceViaIO() override = default;

  EOperatorId Eopid() const override { return EopScalarCoerceViaIO; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarCoerceViaIO"; }

  // match function
  bool Matches(COperator *) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return false; }

  // conversion function
  static CScalarCoerceViaIO *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarCoerceViaIO == pop->Eopid());

    return dynamic_cast<CScalarCoerceViaIO *>(pop);
  }

};  // class CScalarCoerceViaIO

}  // namespace gpopt

#endif  // !GPOPT_CScalarCoerceViaIO_H

// EOF
