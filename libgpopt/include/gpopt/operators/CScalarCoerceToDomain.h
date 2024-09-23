//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CScalarCoerceToDomain.h
//
//	@doc:
//		Scalar CoerceToDomain operator,
//		the operator captures coercing a value to a domain type,
//
//		at runtime, the precise set of constraints to be checked against
//		value are determined,
//		if the value passes, it is returned as the result, otherwise an error
//		is raised.

//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoerceToDomain_H
#define GPOPT_CScalarCoerceToDomain_H

#include "gpopt/operators/CScalarCoerceBase.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarCoerceToDomain
//
//	@doc:
//		Scalar CoerceToDomain operator
//
//---------------------------------------------------------------------------
class CScalarCoerceToDomain : public CScalarCoerceBase {
 private:
  // does operator return NULL on NULL input?
  bool m_returns_null_on_null_input;

 public:
  CScalarCoerceToDomain(const CScalarCoerceToDomain &) = delete;

  // ctor
  CScalarCoerceToDomain(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, ECoercionForm dxl_coerce_format,
                        int32_t location);

  // dtor
  ~CScalarCoerceToDomain() override = default;

  EOperatorId Eopid() const override { return EopScalarCoerceToDomain; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarCoerceToDomain"; }

  // match function
  bool Matches(COperator *) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override { return false; }

  // boolean expression evaluation
  EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const override;

  // conversion function
  static CScalarCoerceToDomain *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarCoerceToDomain == pop->Eopid());

    return dynamic_cast<CScalarCoerceToDomain *>(pop);
  }

};  // class CScalarCoerceToDomain

}  // namespace gpopt

#endif  // !GPOPT_CScalarCoerceToDomain_H

// EOF
