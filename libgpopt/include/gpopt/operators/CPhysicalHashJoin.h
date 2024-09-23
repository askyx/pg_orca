//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalHashJoin.h
//
//	@doc:
//		Base hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalHashJoin_H
#define GPOPT_CPhysicalHashJoin_H

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPhysicalJoin.h"
#include "gpos/base.h"

namespace gpopt {

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalHashJoin
//
//	@doc:
//		Base hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalHashJoin : public CPhysicalJoin {
 private:
  // the array of expressions from the outer relation
  // that are extracted from the hashing condition
  CExpressionArray *m_pdrgpexprOuterKeys;

  // the array of expressions from the inner relation
  // that are extracted from the hashing condition
  CExpressionArray *m_pdrgpexprInnerKeys;

  // Hash op families of the operators used in the join conditions
  IMdIdArray *m_hash_opfamilies;

  // if the join condition is null-aware
  // true by default, and false if the join condition doesn't contain
  // any INDF predicates
  bool m_is_null_aware;

 protected:
  bool FSelfJoinWithMatchingJoinKeys(CMemoryPool *mp, CExpressionHandle &exprhdl) const;

 private:
  // check whether a hash key is nullable
  bool FNullableHashKey(uint32_t ulKey, CColRefSet *pcrsNotNullInner, bool fInner) const;

 protected:
  // check whether the hash keys from one child are nullable
  bool FNullableHashKeys(CColRefSet *pcrsNotNullInner, bool fInner) const;

  // create optimization requests
  virtual void CreateOptRequests();

  CPartitionPropagationSpec *PppsRequiredForJoins(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                  CPartitionPropagationSpec *pppsRequired, uint32_t child_index,
                                                  CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const;

  CExpression *PexprJoinPredOnPartKeys(CMemoryPool *mp, CExpression *pexprScalar, CPartKeysArray *pdrgppartkeys,
                                       CColRefSet *pcrsAllowedRefs) const;

  CPartitionPropagationSpec *PppsDeriveForJoins(CMemoryPool *mp, CExpressionHandle &exprhdl) const;

 public:
  CPhysicalHashJoin(const CPhysicalHashJoin &) = delete;

  // ctor
  CPhysicalHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys, CExpressionArray *pdrgpexprInnerKeys,
                    IMdIdArray *hash_opfamilies, bool is_null_aware = true,
                    CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CPhysicalHashJoin() override;

  // inner keys
  const CExpressionArray *PdrgpexprInnerKeys() const { return m_pdrgpexprInnerKeys; }

  // outer keys
  const CExpressionArray *PdrgpexprOuterKeys() const { return m_pdrgpexprOuterKeys; }

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required sort order of the n-th child
  COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posInput, uint32_t child_index,
                          CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const override;

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  // derive sort order
  COrderSpec *PosDerive(CMemoryPool *mp,
                        CExpressionHandle &  // exprhdl
  ) const override {
    // hash join is not order-preserving
    return GPOS_NEW(mp) COrderSpec(mp);
  }

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  //-------------------------------------------------------------------------------------

  // return order property enforcing type for this operator
  CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // execution order of children
  EChildExecOrder Eceo() const override {
    // TODO - ; 01/06/2014
    // obtain this property by through MD abstraction layer, similar to scalar properties

    // hash join in GPDB executes its inner (right) child first,
    // the optimization order of hash join children follows the execution order
    return EceoRightToLeft;
  }

  // conversion function
  static CPhysicalHashJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(CUtils::FHashJoin(pop));

    return dynamic_cast<CPhysicalHashJoin *>(pop);
  }

};  // class CPhysicalHashJoin

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalHashJoin_H

// EOF
