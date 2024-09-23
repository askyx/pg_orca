//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysical.h
//
//	@doc:
//		Base class for all physical operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysical_H
#define GPOPT_CPhysical_H

#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/base/CEnfdOrder.h"
#include "gpopt/base/CEnfdPartitionPropagation.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CPartitionPropagationSpec.h"
#include "gpopt/operators/COperator.h"
#include "gpos/base.h"

// number of plan properties requested during optimization, currently, there are 4 properties:
// order, distribution, rewindability and partition propagation
#define GPOPT_PLAN_PROPS 4

namespace gpopt {
using namespace gpos;

// arrays of unsigned integer arrays
using UlongPtrArray = CDynamicPtrArray<uintptr_t, CleanupDeleteArray>;

class CTableDescriptor;
class CCTEMap;

//---------------------------------------------------------------------------
//	@class:
//		CPhysical
//
//	@doc:
//		base class for all physical operators
//
//---------------------------------------------------------------------------
class CPhysical : public COperator {
 public:
  // the order in which operator triggers the execution of its children
  enum EChildExecOrder {
    EceoLeftToRight,  // children execute in left to right order
    EceoRightToLeft,  // children execute in right to left order

    EceoSentinel
  };

  enum EPropogatePartConstraint {
    EppcAllowed,
    EppcProhibited,

    EppcSentinel
  };

 private:
  //---------------------------------------------------------------------------
  //	@class:
  //		CReqdColsRequest
  //
  //	@doc:
  //		Representation of incoming column requests during optimization
  //
  //---------------------------------------------------------------------------
  class CReqdColsRequest : public CRefCount {
   private:
    // incoming required columns
    CColRefSet *m_pcrsRequired;

    // index of target physical child for which required columns need to be computed
    uint32_t m_ulChildIndex;

    // index of scalar child to be used when computing required columns
    uint32_t m_ulScalarChildIndex;

   public:
    CReqdColsRequest(const CReqdColsRequest &) = delete;

    // ctor
    CReqdColsRequest(CColRefSet *pcrsRequired, uint32_t child_index, uint32_t ulScalarChildIndex)
        : m_pcrsRequired(pcrsRequired), m_ulChildIndex(child_index), m_ulScalarChildIndex(ulScalarChildIndex) {
      GPOS_ASSERT(nullptr != pcrsRequired);
    }

    // dtor
    ~CReqdColsRequest() override { m_pcrsRequired->Release(); }

    // required columns
    CColRefSet *GetColRefSet() const { return m_pcrsRequired; }

    // child index to push requirements to
    uint32_t UlChildIndex() const { return m_ulChildIndex; }

    // scalar child index
    uint32_t UlScalarChildIndex() const { return m_ulScalarChildIndex; }

    // hash function
    static uint32_t HashValue(const CReqdColsRequest *prcr);

    // equality function
    static bool Equals(const CReqdColsRequest *prcrFst, const CReqdColsRequest *prcrSnd);

  };  // class CReqdColsRequest

  // map of incoming required columns request to computed column sets
  using ReqdColsReqToColRefSetMap =
      CHashMap<CReqdColsRequest, CColRefSet, CReqdColsRequest::HashValue, CReqdColsRequest::Equals,
               CleanupRelease<CReqdColsRequest>, CleanupRelease<CColRefSet>>;

  // hash map of child columns requests
  ReqdColsReqToColRefSetMap *m_phmrcr;

  // given an optimization context, the elements in this array represent is the
  // number of requests that operator will create for its child,
  // array entries correspond to order, distribution, rewindability and partition
  // propagation, respectively
  uint32_t m_rgulOptReqs[GPOPT_PLAN_PROPS];

  // array of expanded requests
  UlongPtrArray *m_pdrgpulpOptReqsExpanded;

  // total number of optimization requests
  uint32_t m_ulTotalOptRequests;

  // update number of requests of a given property
  void UpdateOptRequests(uint32_t ulPropIndex, uint32_t ulRequests);

  // check whether we can push a part table requirement to a given child, given
  // the knowledge of where the part index id is defined
  static bool FCanPushPartReqToChild(CBitSet *pbsPartConsumer, uint32_t child_index);

 protected:
  // set number of order requests that operator creates for its child
  void SetOrderRequests(uint32_t ulOrderReqs) { UpdateOptRequests(0 /*ulPropIndex*/, ulOrderReqs); }

  // set number of distribution requests that operator creates for its child
  void SetDistrRequests(uint32_t ulDistrReqs) { UpdateOptRequests(1 /*ulPropIndex*/, ulDistrReqs); }

  // set number of rewindability requests that operator creates for its child
  void SetRewindRequests(uint32_t ulRewindReqs) { UpdateOptRequests(2 /*ulPropIndex*/, ulRewindReqs); }

  // set number of partition propagation requests that operator creates for its child
  void SetPartPropagateRequests(uint32_t ulPartPropagationReqs) {
    UpdateOptRequests(3 /*ulPropIndex*/, ulPartPropagationReqs);
  }

  // pass cte requirement to the n-th child
  CCTEReq *PcterNAry(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter, uint32_t child_index,
                     CDrvdPropArray *pdrgpdpCtxt) const;

  // helper for computing required columns of the n-th child by including used
  // columns and excluding defined columns of the scalar child
  CColRefSet *PcrsChildReqd(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput, uint32_t child_index,
                            uint32_t ulScalarIndex);

  // helper for a simple case of computing child's required sort order
  static COrderSpec *PosPassThru(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posInput,
                                 uint32_t child_index);

  // pass cte requirement to the child
  static CCTEReq *PcterPushThru(CCTEReq *pcter);

  // combine the derived CTE maps of the first n children
  // of the given expression handle
  static CCTEMap *PcmCombine(CMemoryPool *mp, CDrvdPropArray *pdrgpdpCtxt);

  // helper for common case of sort order derivation
  static COrderSpec *PosDerivePassThruOuter(CExpressionHandle &exprhdl);

  // helper for checking if output columns of a unary operator
  // that defines no new columns include the required columns
  static bool FUnaryProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired);

  // return true if the given column set includes any of the columns defined by
  // the unary node, as given by the handle
  static bool FUnaryUsesDefinedColumns(CColRefSet *pcrs, CExpressionHandle &exprhdl);

 public:
  CPhysical(const CPhysical &) = delete;

  // ctor
  explicit CPhysical(CMemoryPool *mp);

  // dtor
  ~CPhysical() override {
    CRefCount::SafeRelease(m_phmrcr);
    CRefCount::SafeRelease(m_pdrgpulpOptReqsExpanded);
  }

  // helper to compute skew estimate based on given stats and distribution spec
  static CDouble GetSkew(IStatistics *stats);

  // type of operator
  bool FPhysical() const override {
    GPOS_ASSERT(!FLogical() && !FScalar() && !FPattern());
    return true;
  }

  // create base container of derived properties
  CDrvdProp *PdpCreate(CMemoryPool *mp) const override;

  // create base container of required properties
  CReqdProp *PrpCreate(CMemoryPool *mp) const override;

  //-------------------------------------------------------------------------------------
  // Required Plan Properties
  //-------------------------------------------------------------------------------------

  // compute required output columns of the n-th child
  virtual CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                                   uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) = 0;

  // compute required ctes of the n-th child
  virtual CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CCTEReq *pcter, uint32_t child_index,
                                CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const = 0;

  // compute required sort order of the n-th child
  virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, COrderSpec *posRequired,
                                  uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const = 0;

  // compute required partition propagation spec of the n-th child
  virtual CPartitionPropagationSpec *PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                                  CPartitionPropagationSpec *pppsRequired, uint32_t child_index,
                                                  CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) const;

  // required properties: check if required columns are included in output columns
  virtual bool FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, uint32_t ulOptReq) const = 0;

  // required properties: check if required CTEs are included in derived CTE map
  virtual bool FProvidesReqdCTEs(CExpressionHandle &exprhdl, const CCTEReq *pcter) const;

  //-------------------------------------------------------------------------------------
  // Derived Plan Properties
  //-------------------------------------------------------------------------------------

  // derive sort order
  virtual COrderSpec *PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const = 0;

  // derived properties: derive partition propagation spec
  virtual CPartitionPropagationSpec *PppsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const;

  // derive cte map
  virtual CCTEMap *PcmDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const;

  //-------------------------------------------------------------------------------------
  // Enforced Properties
  // See CEngine::FCheckEnfdProps() for comments on usage.
  //-------------------------------------------------------------------------------------

  // return order property enforcing type for this operator
  virtual CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const = 0;

  // return partition propagation property enforcing type for this operator
  virtual CEnfdProp::EPropEnforcingType EpetPartitionPropagation(CExpressionHandle &exprhdl,
                                                                 const CEnfdPartitionPropagation *per) const;

  // order matching type
  virtual CEnfdOrder::EOrderMatching Eom(CReqdPropPlan *prppInput, uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt,
                                         uint32_t ulOptReq);

  // check if optimization contexts is valid
  virtual bool FValidContext(CMemoryPool *,               // mp
                             COptimizationContext *,      // poc,
                             COptimizationContextArray *  // pdrgpocChild
  ) const {
    return true;
  }

  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------
  //-------------------------------------------------------------------------------------

  // execution order of children
  virtual EChildExecOrder Eceo() const {
    // by default, children execute in left to right order
    return EceoLeftToRight;
  }

  // number of order requests that operator creates for its child
  uint32_t UlOrderRequests() const { return m_rgulOptReqs[0]; }

  // number of distribution requests that operator creates for its child
  uint32_t UlDistrRequests() const { return m_rgulOptReqs[1]; }

  // number of rewindability requests that operator creates for its child
  uint32_t UlRewindRequests() const { return m_rgulOptReqs[2]; }

  // number of partition propagation requests that operator creates for its child
  uint32_t UlPartPropagateRequests() const { return m_rgulOptReqs[3]; }

  // return total number of optimization requests
  uint32_t UlOptRequests() const { return m_ulTotalOptRequests; }

  // map request number to order, distribution, rewindability and partition propagation requests
  void LookupRequest(uint32_t ulReqNo,              // input: request number
                     uint32_t *pulOrderReq,         // output: order request number
                     uint32_t *pulDistrReq,         // output: distribution request number
                     uint32_t *pulRewindReq,        // output: rewindability request number
                     uint32_t *pulPartPropagateReq  // output: partition propagation request number
  );

  // return true if operator passes through stats obtained from children,
  // this is used when computing stats during costing
  virtual bool FPassThruStats() const = 0;

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, bool must_exist) override;

  // conversion function
  static CPhysical *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(pop->FPhysical());

    return dynamic_cast<CPhysical *>(pop);
  }

};  // class CPhysical

}  // namespace gpopt

#endif  // !GPOPT_CPhysical_H

// EOF
