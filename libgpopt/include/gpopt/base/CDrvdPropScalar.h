//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdPropScalar.h
//
//	@doc:
//		Derived scalar properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropScalar_H
#define GPOPT_CDrvdPropScalar_H

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CFunctionProp.h"
#include "gpopt/base/CPartInfo.h"
#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

namespace gpopt {
using namespace gpos;

// forward declaration
class CExpressionHandle;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropScalar
//
//	@doc:
//		Derived scalar properties container.
//
//		These are properties specific to scalar expressions like predicates and
//		project list. This includes used and defined columns.
//
//---------------------------------------------------------------------------
class CDrvdPropScalar : public CDrvdProp {
  friend class CExpression;
  enum EDrvdPropType {
    EdptPcrsDefined = 0,
    EdptPcrsUsed,
    EdptPcrsSetReturningFunction,
    EdptFHasSubquery,
    EdptPPartInfo,
    EdptPfp,
    EdptFHasNonScalarFunction,
    EdptUlDistinctAggs,
    EdptUlOrderedAggs,
    EdptFHasMultipleDistinctAggs,
    EdptFHasScalarArrayCmp,
    EdptFHasScalarFuncProject,
    EdptFContainsOnlyReplicationSafeAggFuncs,
    EdptSentinel
  };

 private:
  CMemoryPool *m_mp;

  CBitSet *m_is_prop_derived;

  // defined columns
  CColRefSet *m_pcrsDefined;

  // columns generated by set-returning-function like 'unnest'
  CColRefSet *m_pcrsSetReturningFunction;

  // used columns
  CColRefSet *m_pcrsUsed;

  // do subqueries appear in the operator's tree?
  bool m_fHasSubquery;

  // partition table consumers in subqueries
  CPartInfo *m_ppartinfo;

  // function properties
  CFunctionProp *m_pfp;

  // scalar expression contains non-scalar function?
  bool m_fHasNonScalarFunction;

  // total number of Distinct Aggs (e.g., {count(distinct a), sum(distinct a), count(distinct b)}, the value is 3),
  // only applicable to project lists
  uint32_t m_ulDistinctAggs;

  // only applicable to project lists
  bool m_fHasScalarFunc;

  // does operator define Distinct Aggs on different arguments (e.g., count(distinct a), sum(distinct b)),
  // only applicable to project lists
  bool m_fHasMultipleDistinctAggs;
  uint32_t m_ulOrderedAggs;

  // does expression contain ScalarArrayCmp generated for "scalar op ANY/ALL (array)" construct
  bool m_fHasScalarArrayCmp;

  // does expression contain only replication safe agg funcs
  bool m_fContainsOnlyReplicationSafeAggFuncs;

  // Have all the properties been derived?
  //
  // NOTE1: This is set ONLY when Derive() is called. If all the properties
  // are independently derived, m_is_complete will remain false. In that
  // case, even though Derive() would attempt to derive all the properties
  // once again, it should be quick, since each individual member has been
  // cached.
  // NOTE2: Once these properties are detached from the
  // corresponding expression used to derive it, this MUST be set to true,
  // since after the detachment, there will be no way to derive the
  // properties once again.
  bool m_is_complete;

 protected:
  CColRefSet *DeriveDefinedColumns(CExpressionHandle &);

  CColRefSet *DeriveUsedColumns(CExpressionHandle &);

  CColRefSet *DeriveSetReturningFunctionColumns(CExpressionHandle &);

  bool DeriveHasSubquery(CExpressionHandle &);

  CPartInfo *DerivePartitionInfo(CExpressionHandle &);

  CFunctionProp *DeriveFunctionProperties(CExpressionHandle &);

  bool DeriveHasNonScalarFunction(CExpressionHandle &);

  uint32_t DeriveTotalDistinctAggs(CExpressionHandle &);

  bool DeriveHasScalarFuncProject(CExpressionHandle &);

  bool DeriveHasMultipleDistinctAggs(CExpressionHandle &);

  bool DeriveContainsOnlyReplicationSafeAggFuncs(CExpressionHandle &);

  bool DeriveHasScalarArrayCmp(CExpressionHandle &);
  uint32_t DeriveTotalOrderedAggs(CExpressionHandle &);

 public:
  CDrvdPropScalar(const CDrvdPropScalar &) = delete;

  // ctor
  CDrvdPropScalar(CMemoryPool *mp);

  // dtor
  ~CDrvdPropScalar() override;

  // type of properties
  EPropType Ept() override { return EptScalar; }

  bool IsComplete() const override { return m_is_complete; }

  // derivation function
  void Derive(CMemoryPool *mp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt) override;

  // check for satisfying required plan properties
  bool FSatisfies(const CReqdPropPlan *prpp) const override;

  // defined columns
  CColRefSet *GetDefinedColumns() const;

  // used columns
  CColRefSet *GetUsedColumns() const;

  // columns containing set-returning function
  CColRefSet *GetSetReturningFunctionColumns() const;

  // do subqueries appear in the operator's tree?
  bool HasSubquery() const;

  // derived partition consumers
  CPartInfo *GetPartitionInfo() const;

  // function properties
  CFunctionProp *GetFunctionProperties() const;

  // scalar expression contains non-scalar function?
  virtual bool HasNonScalarFunction() const;

  // return total number of Distinct Aggs, only applicable to project list
  uint32_t GetTotalDistinctAggs() const;

  bool HasScalarFuncProject() const;

  // does operator define Distinct Aggs on different arguments, only applicable to project lists
  bool HasMultipleDistinctAggs() const;

  bool HasScalarArrayCmp() const;

  bool ContainsOnlyReplicationSafeAggFuncs() const;

  // short hand for conversion
  static CDrvdPropScalar *GetDrvdScalarProps(CDrvdProp *pdp);

  // print function
  IOstream &OsPrint(IOstream &os) const override;

};  // class CDrvdPropScalar

}  // namespace gpopt

#endif  // !GPOPT_CDrvdPropScalar_H

// EOF
