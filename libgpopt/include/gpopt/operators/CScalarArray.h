//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarArray.h
//
//	@doc:
//		Class for scalar arrays
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArray_H
#define GPOPT_CScalarArray_H

#include "gpopt/operators/CScalar.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

using CScalarConstArray = CDynamicPtrArray<CScalarConst, CleanupRelease>;

//---------------------------------------------------------------------------
//	@class:
//		CScalarArray
//
//	@doc:
//		Scalar array
//
//---------------------------------------------------------------------------
class CScalarArray : public CScalar {
 private:
  // element type id
  IMDId *m_pmdidElem;

  // array type id
  IMDId *m_pmdidArray;

  // is array multidimensional
  bool m_fMultiDimensional;

  // const values
  CScalarConstArray *m_pdrgPconst;

 public:
  CScalarArray(const CScalarArray &) = delete;

  // ctor
  CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid, bool is_multidimenstional);

  // ctor
  CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid, bool is_multidimenstional,
               CScalarConstArray *pdrgPconst);

  // dtor
  ~CScalarArray() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarArray; }

  // return a string for aggregate function
  const char *SzId() const override { return "CScalarArray"; }

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

  // conversion function
  static CScalarArray *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarArray == pop->Eopid());

    return dynamic_cast<CScalarArray *>(pop);
  }

  // element type id
  IMDId *PmdidElem() const;

  // array type id
  IMDId *PmdidArray() const;

  // is array multi-dimensional
  bool FMultiDimensional() const;

  // type of expression's result
  IMDId *MdidType() const override;

  // CScalarConst array
  CScalarConstArray *PdrgPconst() const;

  // print
  IOstream &OsPrint(IOstream &os) const override;

};  // class CScalarArray

}  // namespace gpopt

#endif  // !GPOPT_CScalarArray_H

// EOF
