//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarWindowFunc.h
//
//	@doc:
//		Class for scalar window function
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarWindowFunc_H
#define GPOPT_CScalarWindowFunc_H

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CScalarWindowFunc
//
//	@doc:
//		Class for scalar window function
//
//---------------------------------------------------------------------------
class CScalarWindowFunc : public CScalarFunc {
 public:
  // window stage
  enum EWinStage {
    EwsImmediate,
    EwsPreliminary,
    EwsRowKey,

    EwsSentinel
  };

 private:
  // window stage
  EWinStage m_ewinstage;

  // distinct window computation
  bool m_is_distinct;

  /* TRUE if argument list was really '*' */
  bool m_is_star_arg;

  /* is function a simple aggregate? */
  bool m_is_simple_agg;

  // aggregate window function, e.g. count(*) over()
  bool m_fAgg;

 public:
  CScalarWindowFunc(const CScalarWindowFunc &) = delete;

  // ctor
  CScalarWindowFunc(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type, const CWStringConst *pstrFunc,
                    EWinStage ewinstage, bool is_distinct, bool is_star_arg, bool is_simple_agg);

  // dtor
  ~CScalarWindowFunc() override = default;

  // ident accessors
  EOperatorId Eopid() const override { return EopScalarWindowFunc; }

  // return a string for window function
  const char *SzId() const override { return "CScalarWindowFunc"; }

  EWinStage Ews() const { return m_ewinstage; }

  // operator specific hash function
  uint32_t HashValue() const override;

  // match function
  bool Matches(COperator *pop) const override;

  // conversion function
  static CScalarWindowFunc *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopScalarWindowFunc == pop->Eopid());

    return dynamic_cast<CScalarWindowFunc *>(pop);
  }

  // does window function definition include Distinct?
  bool IsDistinct() const { return m_is_distinct; }

  bool IsStarArg() const { return m_is_star_arg; }

  bool IsSimpleAgg() const { return m_is_simple_agg; }

  // is window function defined as Aggregate?
  bool FAgg() const { return m_fAgg; }

  // print
  IOstream &OsPrint(IOstream &os) const override;

};  // class CScalarWindowFunc

}  // namespace gpopt

#endif  // !GPOPT_CScalarWindowFunc_H

// EOF
