//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CReqdPropRelational.h
//
//	@doc:
//		Derived required relational properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdPropRelational_H
#define GPOPT_CReqdPropRelational_H

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CReqdProp.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

// forward declaration
class CExpression;
class CExpressionHandle;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CReqdPropRelational
//
//	@doc:
//		Required relational properties container.
//
//---------------------------------------------------------------------------
class CReqdPropRelational : public CReqdProp {
 private:
  // required stat columns
  CColRefSet *m_pcrsStat{nullptr};

 public:
  CReqdPropRelational(const CReqdPropRelational &) = delete;

  // default ctor
  CReqdPropRelational();

  // ctor
  explicit CReqdPropRelational(CColRefSet *pcrs);

  // dtor
  ~CReqdPropRelational() override;

  // type of properties
  bool FRelational() const override {
    GPOS_ASSERT(!FPlan());
    return true;
  }

  // stat columns accessor
  CColRefSet *PcrsStat() const { return m_pcrsStat; }

  // required properties computation function
  void Compute(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, uint32_t child_index,
               CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) override;

  // return difference from given properties
  CReqdPropRelational *PrprelDifference(CMemoryPool *mp, CReqdPropRelational *prprel);

  // return true if property container is empty
  bool IsEmpty() const;

  // shorthand for conversion
  static CReqdPropRelational *GetReqdRelationalProps(CReqdProp *prp);

  // print function
  IOstream &OsPrint(IOstream &os) const override;

};  // class CReqdPropRelational

}  // namespace gpopt

#endif  // !GPOPT_CReqdPropRelational_H

// EOF
