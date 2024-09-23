//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 Broadcom
//
//	@filename:
//		CScalarParam.h
//
//	@doc:
//		Scalar paramater
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarParam_H
#define GPOPT_CScalarParam_H

#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarParam
//
//	@doc:
//		scalar parameter
//
//---------------------------------------------------------------------------
class CScalarParam : public CScalar {
 private:
  // param id
  const uint32_t m_id;

  // param type
  IMDId *m_type;

  // param type modifier
  int32_t m_type_modifier;

 public:
  CScalarParam(const CScalarParam &) = delete;

  // ctor
  CScalarParam(CMemoryPool *mp, uint32_t id, IMDId *type, int32_t type_modifier)
      : CScalar(mp), m_id(id), m_type(type), m_type_modifier(type_modifier) {}

  // dtor
  ~CScalarParam() override;

  // param accessors
  EOperatorId Eopid() const override { return EopScalarParam; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarParam"; }

  // operator specific hash function
  uint32_t HashValue() const override;

  uint32_t Id() const { return m_id; }
  // the type of the scalar expression
  IMDId *MdidType() const override { return m_type; }

  // the type modifier of the scalar expression
  int32_t TypeModifier() const override { return m_type_modifier; }

  // match function
  bool Matches(COperator *pop) const override;

  bool FInputOrderSensitive() const override {
    GPOS_ASSERT(!"Unexpected call of function FInputOrderSensitive");
    return false;
  }

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *,       // mp,
                                        UlongToColRefMap *,  // colref_mapping,
                                        bool                 // must_exist
                                        ) override {
    return PopCopyDefault();
  }

  static bool Equals(const CScalarParam *left, const CScalarParam *right) { return left->Id() == right->Id(); }

  // conversion function
  static CScalarParam *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarParam == pop->Eopid());

    return dynamic_cast<CScalarParam *>(pop);
  }

  // print
  IOstream &OsPrint(IOstream &os) const override;

};  // class CScalarParam

}  // namespace gpopt

#endif  // !GPOPT_CScalarParam_H

// EOF
