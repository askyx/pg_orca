//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalJoin.h
//
//	@doc:
//		Base class of all logical join operators
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalJoin_H
#define GPOS_CLogicalJoin_H

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpos/base.h"

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CLogicalJoin
//
//	@doc:
//		join operator
//
//---------------------------------------------------------------------------
class CLogicalJoin : public CLogical {
 private:
  // xform that join originated from
  CXform::EXformId m_origin_xform;

 protected:
  // ctor
  explicit CLogicalJoin(CMemoryPool *mp, CXform::EXformId origin_xform = CXform::ExfSentinel);

  // dtor
  ~CLogicalJoin() override = default;

 public:
  CLogicalJoin(const CLogicalJoin &) = delete;

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
  static CLogicalJoin *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);

    return dynamic_cast<CLogicalJoin *>(pop);
  }

  //-------------------------------------------------------------------------------------
  // Derived Relational Properties
  //-------------------------------------------------------------------------------------

  // derive output columns
  CColRefSet *DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl) override {
    return PcrsDeriveOutputCombineLogical(mp, exprhdl);
  }

  // derive partition consumer info
  CPartInfo *DerivePartitionInfo(CMemoryPool *mp, CExpressionHandle &exprhdl) const override {
    return PpartinfoDeriveCombine(mp, exprhdl);
  }

  // derive keys
  CKeyCollection *DeriveKeyCollection(CMemoryPool *mp, CExpressionHandle &exprhdl) const override {
    return PkcCombineKeys(mp, exprhdl);
  }

  // derive function properties
  CFunctionProp *DeriveFunctionProperties(CMemoryPool *mp, CExpressionHandle &exprhdl) const override {
    return PfpDeriveFromScalar(mp, exprhdl);
  }

  //-------------------------------------------------------------------------------------
  // Derived Stats
  //-------------------------------------------------------------------------------------

  // promise level for stat derivation
  EStatPromise Esp(CExpressionHandle &exprhdl) const override {
    // no stat derivation on Join trees with subqueries
    if (exprhdl.DeriveHasSubquery(exprhdl.Arity() - 1)) {
      return EspLow;
    }

    if (nullptr != exprhdl.Pgexpr() && exprhdl.Pgexpr()->ExfidOrigin() == CXform::ExfExpandNAryJoin) {
      return EspMedium;
    }

    return EspHigh;
  }

  // derive statistics
  IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl, IStatisticsArray *stats_ctxt) const override;

  //-------------------------------------------------------------------------------------
  // Required Relational Properties
  //-------------------------------------------------------------------------------------

  // compute required stat columns of the n-th child
  CColRefSet *PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput,
                       uint32_t child_index) const override {
    const uint32_t arity = exprhdl.Arity();

    return PcrsReqdChildStats(mp, exprhdl, pcrsInput, exprhdl.DeriveUsedColumns(arity - 1), child_index);
  }

  // return true if operator can select a subset of input tuples based on some predicate
  bool FSelectionOp() const override { return true; }

  CXform::EXformId OriginXform() { return m_origin_xform; }

};  // class CLogicalJoin

}  // namespace gpopt

#endif  // !GPOS_CLogicalJoin_H

// EOF
