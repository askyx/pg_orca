//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarIdent.h
//
//	@doc:
//		Scalar column identifier
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarIdent_H
#define GPOPT_CScalarIdent_H

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarIdent
//
//	@doc:
//		scalar identifier operator
//
//---------------------------------------------------------------------------
class CScalarIdent : public CScalar {
 private:
  // column
  const CColRef *m_pcr;

 public:
  CScalarIdent(const CScalarIdent &) = delete;

  // ctor
  CScalarIdent(CMemoryPool *mp, const CColRef *colref) : CScalar(mp), m_pcr(colref) {}

  // dtor
  ~CScalarIdent() override = default;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarIdent; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarIdent"; }

  // accessor
  const CColRef *Pcr() const { return m_pcr; }

  // operator specific hash function
  uint32_t HashValue() const override;

  static uint32_t HashValue(const CScalarIdent *pscalarIdent) { return CColRef::HashValue(pscalarIdent->Pcr()); }

  static bool Equals(const CScalarIdent *left, const CScalarIdent *right) {
    return CColRef::Equals(left->Pcr(), right->Pcr());
  }

  // match function
  bool Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override;

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, bool must_exist) override;

  // return locally used columns
  CColRefSet *PcrsUsed(CMemoryPool *mp,
                       CExpressionHandle &  // exprhdl

                       ) override {
    CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
    pcrs->Include(m_pcr);

    return pcrs;
  }

  // conversion function
  static CScalarIdent *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarIdent == pop->Eopid());

    return dynamic_cast<CScalarIdent *>(pop);
  }

  // the type of the scalar expression
  IMDId *MdidType() const override;

  // the type modifier of the scalar expression
  int32_t TypeModifier() const override;

  // print
  IOstream &OsPrint(IOstream &os) const override;

  // is the given expression a scalar cast of a scalar identifier
  static bool FCastedScId(CExpression *pexpr);

  // is the given expression a scalar cast of given scalar identifier
  static bool FCastedScId(CExpression *pexpr, CColRef *colref);

  // is the given expression a scalar func allowed for Partition selection of given scalar identifier
  static bool FAllowedFuncScId(CExpression *pexpr);

  // is the given expression a scalar func allowed for Partition selection of given scalar identifier
  static bool FAllowedFuncScId(CExpression *pexpr, CColRef *colref);

};  // class CScalarIdent

}  // namespace gpopt

#endif  // !GPOPT_CScalarIdent_H

// EOF
