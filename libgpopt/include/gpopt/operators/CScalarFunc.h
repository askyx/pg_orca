//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarFunc.h
//
//	@doc:
//		Class for scalar function calls
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarFunc_H
#define GPOPT_CScalarFunc_H

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CScalarFunc
//
//	@doc:
//		scalar function operator
//
//---------------------------------------------------------------------------
class CScalarFunc : public CScalar {
 protected:
  // func id
  IMDId *m_func_mdid;

  // return type
  IMDId *m_return_type_mdid;

  const int32_t m_return_type_modifier;

  // function name
  const CWStringConst *m_pstrFunc;

  // function stability
  IMDFunction::EFuncStbl m_efs;

  // can the function return multiple rows?
  bool m_returns_set;

  // does operator return NULL on NULL input?
  bool m_returns_null_on_null_input;

  // is operator return type bool?
  bool m_fBoolReturnType;

  //  It is true if in the function, variadic arguments have been
  //	combined into an array last argument
  bool m_funcvariadic;

 private:
 public:
  CScalarFunc(const CScalarFunc &) = delete;

  explicit CScalarFunc(CMemoryPool *mp);

  // ctor
  CScalarFunc(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type, int32_t return_type_modifier,
              const CWStringConst *pstrFunc, bool funcvariadic);

  // dtor
  ~CScalarFunc() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarFunc; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarFunc"; }

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

  // derive function properties
  CFunctionProp *DeriveFunctionProperties(CMemoryPool *mp, CExpressionHandle &exprhdl) const override {
    return PfpDeriveFromChildren(mp, exprhdl, m_efs, false /*fHasVolatileFunctionScan*/, false /*fScan*/);
  }

  // derive non-scalar function existence
  bool FHasNonScalarFunction(CExpressionHandle &exprhdl) override;

  // conversion function
  static CScalarFunc *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarFunc == pop->Eopid());

    return dynamic_cast<CScalarFunc *>(pop);
  }

  // function name
  const CWStringConst *PstrFunc() const;

  // func id
  IMDId *FuncMdId() const;

  int32_t TypeModifier() const override;

  // the type of the scalar expression
  IMDId *MdidType() const override;

  // function stability
  IMDFunction::EFuncStbl EfsGetFunctionStability() const;

  // boolean expression evaluation
  EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const override;

  // print
  IOstream &OsPrint(IOstream &os) const override;

  // Is variadic flag set
  bool IsFuncVariadic() const;

};  // class CScalarFunc

}  // namespace gpopt

#endif  // !GPOPT_CScalarFunc_H

// EOF
