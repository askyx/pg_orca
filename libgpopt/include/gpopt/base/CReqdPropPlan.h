//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CReqdPropPlan.h
//
//	@doc:
//		Derived required relational properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdPropPlan_H
#define GPOPT_CReqdPropPlan_H

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CReqdProp.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

// forward declaration
class CColRefSet;
class CDrvdPropRelational;
class CDrvdPropPlan;
class CEnfdOrder;
class CEnfdPartitionPropagation;
class CExpressionHandle;
class CCTEReq;
class CPartInfo;
class CPhysical;
class CPropSpec;

//---------------------------------------------------------------------------
//	@class:
//		CReqdPropPlan
//
//	@doc:
//		Required plan properties container.
//
//---------------------------------------------------------------------------
class CReqdPropPlan : public CReqdProp {
 private:
  // required columns
  CColRefSet *m_pcrs{nullptr};

  // required sort order
  CEnfdOrder *m_peo{nullptr};

  // required partition propagation
  CEnfdPartitionPropagation *m_pepp{nullptr};

  // required ctes
  CCTEReq *m_pcter{nullptr};

 public:
  CReqdPropPlan(const CReqdPropPlan &) = delete;

  // default ctor
  CReqdPropPlan() = default;

  // ctor
  CReqdPropPlan(CColRefSet *pcrs, CEnfdOrder *peo, CEnfdPartitionPropagation *pepp, CCTEReq *pcter);

  // dtor
  ~CReqdPropPlan() override;

  // type of properties
  bool FPlan() const override {
    GPOS_ASSERT(!FRelational());
    return true;
  }

  // required properties computation function
  void Compute(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, uint32_t child_index,
               CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) override;

  // required columns computation function
  void ComputeReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, uint32_t child_index,
                       CDrvdPropArray *pdrgpdpCtxt);

  // required ctes computation function
  void ComputeReqdCTEs(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, uint32_t child_index,
                       CDrvdPropArray *pdrgpdpCtxt);

  // required columns accessor
  CColRefSet *PcrsRequired() const { return m_pcrs; }

  // required order accessor
  CEnfdOrder *Peo() const { return m_peo; }

  // required partition propagation accessor
  CEnfdPartitionPropagation *Pepp() const { return m_pepp; }

  // required cte accessor
  CCTEReq *Pcter() const { return m_pcter; }

  // given a property spec type, return the corresponding property spec member
  CPropSpec *Pps(uint32_t ul) const;

  // equality function
  bool Equals(const CReqdPropPlan *prpp) const;

  // hash function
  uint32_t HashValue() const;

  // check if plan properties are satisfied by the given derived properties
  bool FSatisfied(const CDrvdPropRelational *pdprel, const CDrvdPropPlan *pdpplan) const;

  // check if plan properties are compatible with the given derived properties
  bool FCompatible(CExpressionHandle &exprhdl, CPhysical *popPhysical, const CDrvdPropRelational *pdprel,
                   const CDrvdPropPlan *pdpplan) const;

  // check if expression attached to handle provides required columns by all plan properties
  bool FProvidesReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl, uint32_t ulOptReq) const;

  // shorthand for conversion
  static CReqdPropPlan *Prpp(CReqdProp *prp) {
    GPOS_ASSERT(nullptr != prp);

    return dynamic_cast<CReqdPropPlan *>(prp);
  }

  // generate empty required properties
  static CReqdPropPlan *PrppEmpty(CMemoryPool *mp);

  // hash function used for cost bounding
  static uint32_t UlHashForCostBounding(const CReqdPropPlan *prpp);

  // equality function used for cost bounding
  static bool FEqualForCostBounding(const CReqdPropPlan *prppFst, const CReqdPropPlan *prppSnd);

  // map input required and derived plan properties into new required plan properties
  static CReqdPropPlan *PrppRemapForCTE(CMemoryPool *mp, CReqdPropPlan *prppProducer, CDrvdPropPlan *pdpplanProducer,
                                        CDrvdPropPlan *pdpplanConsumer, UlongToColRefMap *colref_mapping);

  // print function
  IOstream &OsPrint(IOstream &os) const override;

};  // class CReqdPropPlan

}  // namespace gpopt

#endif  // !GPOPT_CReqdPropPlan_H

// EOF
