//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarNullIf.h
//
//	@doc:
//		Scalar NullIf Operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarNullIf_H
#define GPOPT_CScalarNullIf_H

#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarNullIf
//
//	@doc:
//		Scalar NullIf operator
//
//---------------------------------------------------------------------------
class CScalarNullIf : public CScalar {
 private:
  // operator id
  IMDId *m_mdid_op;

  // return type
  IMDId *m_mdid_type;

  // does operator return NULL on NULL input?
  bool m_returns_null_on_null_input;

  // is operator return type bool?
  bool m_fBoolReturnType;

 public:
  CScalarNullIf(const CScalarNullIf &) = delete;

  // ctor
  CScalarNullIf(CMemoryPool *mp, IMDId *mdid_op, IMDId *mdid_type);

  // dtor
  ~CScalarNullIf() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarNullIf; }

  // operator id
  virtual IMDId *MdIdOp() const { return m_mdid_op; }

  // return type
  IMDId *MdidType() const override { return m_mdid_type; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarNullIf"; }

  // operator specific hash function
  uint32_t HashValue() const override;

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

  // boolean expression evaluation
  EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const override;

  // conversion function
  static CScalarNullIf *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarNullIf == pop->Eopid());

    return dynamic_cast<CScalarNullIf *>(pop);
  }

};  // class CScalarNullIf

}  // namespace gpopt

#endif  // !GPOPT_CScalarNullIf_H

// EOF
