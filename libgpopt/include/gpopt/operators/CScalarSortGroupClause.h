//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarSortGroupClause.h
//
//	@doc:
//		An operator class that wraps a sort group clause
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSortGroupClause_H
#define GPOPT_CScalarSortGroupClause_H

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		CScalarSortGroupClause
//
//	@doc:
//		A wrapper operator for sort group clause
//
//---------------------------------------------------------------------------
class CScalarSortGroupClause : public CScalar {
 private:
  int32_t m_tle_sort_group_ref;
  int32_t m_eqop;
  int32_t m_sortop;
  bool m_nulls_first;
  bool m_hashable;

 public:
  // private copy ctor
  CScalarSortGroupClause(const CScalarSortGroupClause &) = delete;

  // ctor
  CScalarSortGroupClause(CMemoryPool *mp, int32_t tle_sort_group_ref, int32_t eqop, int32_t sortop, bool nulls_first,
                         bool hashable);

  ~CScalarSortGroupClause() override = default;

  int32_t Index() const { return m_tle_sort_group_ref; }
  int32_t EqOp() const { return m_eqop; }
  int32_t SortOp() const { return m_sortop; }
  bool NullsFirst() const { return m_nulls_first; }
  bool IsHashable() const { return m_hashable; }

  // identity accessor
  EOperatorId Eopid() const override { return EopScalarSortGroupClause; }

  // return a string for operator name
  const char *SzId() const override { return "CScalarSortGroupClause"; }

  // match function
  bool Matches(COperator *op) const override;

  // conversion function
  static CScalarSortGroupClause *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarSortGroupClause == pop->Eopid());

    return dynamic_cast<CScalarSortGroupClause *>(pop);
  }

  // the type of the scalar expression
  IMDId *MdidType() const override { return nullptr; }

  int32_t TypeModifier() const override;

  // boolean expression evaluation
  EBoolEvalResult Eber(ULongPtrArray *) const override;

  COperator *PopCopyWithRemappedColumns(CMemoryPool *,       // mp,
                                        UlongToColRefMap *,  // colref_mapping,
                                        bool                 // must_exist
                                        ) override {
    return PopCopyDefault();
  }

  bool FInputOrderSensitive() const override { return false; }

  // print
  IOstream &OsPrint(IOstream &io) const override;

};  // class CScalarSortGroupClause

}  // namespace gpopt

#endif  // !GPOPT_CScalarSortGroupClause_H

// EOF
