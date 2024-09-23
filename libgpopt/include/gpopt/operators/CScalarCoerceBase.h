//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CScalarCoerceBase.h
//
//	@doc:
//		Scalar coerce operator base class
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoerceBase_H
#define GPOPT_CScalarCoerceBase_H

#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarCoerceBase
//
//	@doc:
//		Scalar coerce operator base class
//
//---------------------------------------------------------------------------
class CScalarCoerceBase : public CScalar {
 private:
  // catalog MDId of the result type
  IMDId *m_result_type_mdid;

  // output type modifier
  int32_t m_type_modifier;

  // coercion form
  ECoercionForm m_ecf;

  // location of token to be coerced
  int32_t m_location;

 public:
  CScalarCoerceBase(const CScalarCoerceBase &) = delete;

  // ctor
  CScalarCoerceBase(CMemoryPool *mp, IMDId *mdid_type, int32_t type_modifier, ECoercionForm dxl_coerce_format,
                    int32_t location);

  // dtor
  ~CScalarCoerceBase() override;

  // the type of the scalar expression
  IMDId *MdidType() const override;

  // return type modifier
  int32_t TypeModifier() const override;

  // return coercion form
  ECoercionForm Ecf() const;

  // return token location
  int32_t Location() const;

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, bool must_exist) override;

};  // class CScalarCoerceBase

}  // namespace gpopt

#endif  // !GPOPT_CScalarCoerceBase_H

// EOF
