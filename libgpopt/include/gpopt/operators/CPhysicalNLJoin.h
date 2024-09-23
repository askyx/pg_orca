//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalNLJoin.h
//
//	@doc:
//		Base nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalNLJoin_H
#define GPOPT_CPhysicalNLJoin_H

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPhysicalJoin.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalNLJoin
//
//	@doc:
//		Inner nested-loops join operator
//
//---------------------------------------------------------------------------
class CPhysicalNLJoin : public CPhysicalJoin {
 private:
 public:
  CPhysicalNLJoin(const CPhysicalNLJoin &) = delete;

  // ctor
  explicit CPhysicalNLJoin(CMemoryPool *mp);

  // dtor
  ~CPhysicalNLJoin() override;

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required sort order of the n-th child
  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posInput, uint32_t child_index,
                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  // compute required output columns of the n-th child
  CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t child_index,
                           CDrvdPropArray *,  // pdrgpdpCtxt
                           uint32_t           // ulOptReq
                           ) override;

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return order property enforcing type for this operator
  CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // return true if operator is a correlated NL Join
  virtual bool FCorrelated() const { return false; }

  // return required inner columns -- overloaded by correlated join children
  virtual CColRefArray *PdrgPcrInner() const { return nullptr; }

  // conversion function
  static CPhysicalNLJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(CUtils::FNLJoin(pop));

    return dynamic_cast<CPhysicalNLJoin *>(pop);
  }

};  // class CPhysicalNLJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalNLJoin_H

// EOF
