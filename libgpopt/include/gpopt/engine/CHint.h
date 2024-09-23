//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.
//
//	@filename:
//		CHint.h
//
//	@doc:
//		Hint configurations
//---------------------------------------------------------------------------
#ifndef GPOPT_CHint_H
#define GPOPT_CHint_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/memory/CMemoryPool.h"

#define JOIN_ORDER_DP_THRESHOLD uint32_t(10)
#define BROADCAST_THRESHOLD uint32_t(10000000)
#define PUSH_GROUP_BY_BELOW_SETOP_THRESHOLD uint32_t(10)
#define XFORM_BIND_THRESHOLD uint32_t(0)
#define SKEW_FACTOR uint32_t(0)

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CHint
//
//	@doc:
//		Hint configurations
//
//---------------------------------------------------------------------------
class CHint : public CRefCount {
 private:
  uint32_t m_ulJoinArityForAssociativityCommutativity;

  uint32_t m_ulArrayExpansionThreshold;

  uint32_t m_ulJoinOrderDPLimit;

  uint32_t m_ulBroadcastThreshold;

  uint32_t m_ulPushGroupByBelowSetopThreshold;

  uint32_t m_ulXform_bind_threshold;

  uint32_t m_ulSkewFactor;

 public:
  CHint(const CHint &) = delete;

  // ctor
  CHint(uint32_t join_arity_for_associativity_commutativity, uint32_t array_expansion_threshold,
        uint32_t ulJoinOrderDPLimit, uint32_t broadcast_threshold, uint32_t push_group_by_below_setop_threshold,
        uint32_t xform_bind_threshold, uint32_t skew_factor)
      : m_ulJoinArityForAssociativityCommutativity(join_arity_for_associativity_commutativity),
        m_ulArrayExpansionThreshold(array_expansion_threshold),
        m_ulJoinOrderDPLimit(ulJoinOrderDPLimit),
        m_ulBroadcastThreshold(broadcast_threshold),
        m_ulPushGroupByBelowSetopThreshold(push_group_by_below_setop_threshold),
        m_ulXform_bind_threshold(xform_bind_threshold),
        m_ulSkewFactor(skew_factor) {}

  // Maximum number of relations in an n-ary join operator where ORCA will
  // explore JoinAssociativity and JoinCommutativity transformations.
  // When the number of relations exceed this we'll prune the search space
  // by not pursuing the above mentioned two transformations.
  uint32_t UlJoinArityForAssociativityCommutativity() const { return m_ulJoinArityForAssociativityCommutativity; }

  // Maximum number of elements in the scalar comparison with an array which
  // will be expanded for constraint derivation. The benefits of using a smaller number
  // are avoiding expensive expansion of constraints in terms of memory and optimization
  // time. This is used to restrict constructs of following types when the constant-array
  // size is greater than threshold:
  // "(expression) scalar op ANY/ALL (array of constants)" OR
  // "(expression1, expression2) scalar op ANY/ALL ((const-x1, const-y1), ... (const-xn, const-yn))"
  uint32_t UlArrayExpansionThreshold() const { return m_ulArrayExpansionThreshold; }

  // Maximum number of relations in an n-ary join operator where ORCA will
  // explore join ordering via dynamic programming.
  uint32_t UlJoinOrderDPLimit() const { return m_ulJoinOrderDPLimit; }

  // Maximum number of rows ORCA will broadcast
  uint32_t UlBroadcastThreshold() const { return m_ulBroadcastThreshold; }

  // Skip CXformPushGbBelowSetOp if set op arity is greater than this
  uint32_t UlPushGroupByBelowSetopThreshold() const { return m_ulPushGroupByBelowSetopThreshold; }

  // Stop generating alternatives for group expression if bindings exceed this threshold
  uint32_t UlXformBindThreshold() const { return m_ulXform_bind_threshold; }

  // User defined skew multiplier, multiplied to the skew ratio calculated from 1000 samples
  uint32_t UlSkewFactor() const { return m_ulSkewFactor; }

  // generate default hint configurations, which disables sort during insert on
  // append only row-oriented partitioned tables by default
  static CHint *PhintDefault(CMemoryPool *mp) {
    return GPOS_NEW(mp) CHint(INT32_MAX,                           /* join_arity_for_associativity_commutativity */
                              INT32_MAX,                           /* array_expansion_threshold */
                              JOIN_ORDER_DP_THRESHOLD,             /*ulJoinOrderDPLimit*/
                              BROADCAST_THRESHOLD,                 /*broadcast_threshold*/
                              PUSH_GROUP_BY_BELOW_SETOP_THRESHOLD, /* push_group_by_below_setop_threshold */
                              XFORM_BIND_THRESHOLD,                /* xform_bind_threshold */
                              SKEW_FACTOR                          /* skew_factor */
    );
  }

};  // class CHint
}  // namespace gpopt

#endif  // !GPOPT_CHint_H

// EOF
