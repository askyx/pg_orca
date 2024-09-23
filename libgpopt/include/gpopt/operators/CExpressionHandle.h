//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CExpressionHandle.h
//
//	@doc:
//		Handle to convey context wherever an expression is used in a shallow
//		context, i.e. operator and the properties of its children but no
//		access to the children is needed.
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionHandle_H
#define GPOPT_CExpressionHandle_H

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CReqdProp.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpos/base.h"
#include "naucrates/statistics/IStatistics.h"

namespace gpopt {
// fwd declaration
class CExpression;
class COperator;
class CDrvdPropPlan;
class CDrvdPropScalar;
class CColRefSet;
class CPropConstraint;
class CCostContext;

using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CExpressionHandle
//
//	@doc:
//		Context for expression; abstraction for group expressions and
//		stand-alone expressions/DAGs;
//		a handle is attached to either an expression or a group expression
//
//---------------------------------------------------------------------------
class CExpressionHandle {
  friend class CExpression;

 private:
  // memory pool
  CMemoryPool *m_mp;

  // attached expression
  CExpression *m_pexpr;

  // attached group expression
  CGroupExpression *m_pgexpr;

  // attached cost context
  CCostContext *m_pcc;

  // derived plan properties of the gexpr attached by a CostContext under
  // the default CDrvdPropCtxtPlan. See DerivePlanPropsForCostContext()
  // NB: does NOT support on-demand property derivation
  CDrvdProp *m_pdpplan;

  // statistics of attached expr/gexpr;
  // set during derived stats computation
  IStatistics *m_pstats;

  // required properties of attached expr/gexpr;
  // set during required property computation
  CReqdProp *m_prp;

  // array of children's derived stats
  IStatisticsArray *m_pdrgpstat;

  // array of children's required properties
  CReqdPropArray *m_pdrgprp;

  // return an array of stats objects starting from the first stats object referenced by child
  IStatisticsArray *PdrgpstatOuterRefs(IStatisticsArray *statistics_array, uint32_t child_index);

  // check if stats are derived for attached expression and its children
  bool FStatsDerived() const;

  // copy stats from attached expression/group expression to local stats members
  void CopyStats();

  // return True if handle is attached to a leaf pattern
  bool FAttachedToLeafPattern() const;

  // stat derivation at root operator where handle is attached
  void DeriveRootStats(IStatisticsArray *stats_ctxt);

 public:
  CExpressionHandle(const CExpressionHandle &) = delete;

  // ctor
  explicit CExpressionHandle(CMemoryPool *mp);

  // dtor
  ~CExpressionHandle();

  // attach handle to a given expression
  void Attach(CExpression *pexpr);

  // attach handle to a given group expression
  void Attach(CGroupExpression *pgexpr);

  // attach handle to a given cost context
  void Attach(CCostContext *pcc);

  // recursive property derivation,
  void DeriveProps(CDrvdPropCtxt *pdpctxt);

  // recursive stats derivation
  void DeriveStats(IStatisticsArray *stats_ctxt, bool fComputeRootStats = true);

  // stats derivation for attached cost context
  void DeriveCostContextStats();

  // stats derivation using given properties and context
  void DeriveStats(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal, CReqdPropRelational *prprel,
                   IStatisticsArray *stats_ctxt) const;

  // derive the properties of the plan carried by attached cost context,
  // using default CDrvdPropCtxtPlan
  void DerivePlanPropsForCostContext();

  // initialize required properties container
  void InitReqdProps(CReqdProp *prpInput);

  // compute required properties of the n-th child
  void ComputeChildReqdProps(uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq);

  // copy required properties of the n-th child
  void CopyChildReqdProps(uint32_t child_index, CReqdProp *prp);

  // compute required columns of the n-th child
  void ComputeChildReqdCols(uint32_t child_index, CDrvdPropArray *pdrgpdpCtxt);

  // required properties computation of all children
  void ComputeReqdProps(CReqdProp *prpInput, uint32_t ulOptReq);

  // derived relational props of n-th child
  CDrvdPropRelational *GetRelationalProperties(uint32_t child_index) const;

  // derived stats of n-th child
  IStatistics *Pstats(uint32_t child_index) const;

  // derived plan props of n-th child
  CDrvdPropPlan *Pdpplan(uint32_t child_index) const;

  // derived scalar props of n-th child
  CDrvdPropScalar *GetDrvdScalarProps(uint32_t child_index) const;

  // derived properties of attached expr/gexpr
  CDrvdProp *Pdp() const;

  // derived relational properties of attached expr/gexpr
  CDrvdPropRelational *GetRelationalProperties() const;

  // stats of attached expr/gexpr
  IStatistics *Pstats();

  // required properties of attached expr/gexpr
  CReqdProp *Prp() const { return m_prp; }

  // check if given child is a scalar
  bool FScalarChild(uint32_t child_index) const;

  // required relational props of n-th child
  CReqdPropRelational *GetReqdRelationalProps(uint32_t child_index) const;

  // required plan props of n-th child
  CReqdPropPlan *Prpp(uint32_t child_index) const;

  // arity function
  uint32_t Arity() const;

  // index of the last non-scalar child
  uint32_t UlLastNonScalarChild() const;

  // index of the first non-scalar child
  uint32_t UlFirstNonScalarChild() const;

  // number of non-scalar children
  uint32_t UlNonScalarChildren() const;

  // accessor for operator
  COperator *Pop() const;

  // accessor for child operator
  COperator *Pop(uint32_t child_index) const;

  // accessor for grandchild operator
  COperator *PopGrandchild(uint32_t child_index, uint32_t grandchild_index, CCostContext **grandchildContext) const;

  // accessor for expression
  CExpression *Pexpr() const { return m_pexpr; }

  // accessor for group expression
  CGroupExpression *Pgexpr() const { return m_pgexpr; }

  // check for outer references
  bool HasOuterRefs() const { return (0 < DeriveOuterReferences()->Size()); }

  // check if attached expression must execute on a single host
  bool NeedsSingletonExecution() const { return DeriveFunctionProperties()->NeedsSingletonExecution(); }

  // check for outer references in the given child
  bool HasOuterRefs(uint32_t child_index) const { return (0 < DeriveOuterReferences(child_index)->Size()); }

  // get next child index based on child optimization order, return true if such index could be found
  bool FNextChildIndex(uint32_t *pulChildIndex  // output: index to be changed
  ) const;

  // return the index of first child to be optimized
  uint32_t UlFirstOptimizedChildIndex() const;

  // return the index of last child to be optimized
  uint32_t UlLastOptimizedChildIndex() const;

  // return the index of child to be optimized next to the given child
  uint32_t UlNextOptimizedChildIndex(uint32_t child_index) const;

  // return the index of child optimized before the given child
  uint32_t UlPreviousOptimizedChildIndex(uint32_t child_index) const;

  // get the function properties of a child
  CFunctionProp *PfpChild(uint32_t child_index) const;

  // check whether an expression's children have a volatile function scan
  bool FChildrenHaveVolatileFuncScan() const;

  // check whether an expression's children have a volatile function
  bool FChildrenHaveVolatileFunc() const;

  // return a representative (inexact) scalar child at given index
  CExpression *PexprScalarRepChild(uint32_t child_index) const;

  // return a representative (inexact) scalar expression attached to handle
  CExpression *PexprScalarRep() const;

  // return an exact scalar child at given index or return null if not possible
  CExpression *PexprScalarExactChild(uint32_t child_index, bool error_on_null_return = false) const;

  // return an exact scalar expression attached to handle or null if not possible
  CExpression *PexprScalarExact() const;

  void DeriveProducerStats(uint32_t child_index, CColRefSet *pcrsStat) const;

  // return the columns used by a logical operator internally as well
  // as columns used by all its scalar children
  CColRefSet *PcrsUsedColumns(CMemoryPool *mp) const;

  CColRefSet *DeriveOuterReferences() const;
  CColRefSet *DeriveOuterReferences(uint32_t child_index) const;

  CColRefSet *DeriveOutputColumns() const;
  CColRefSet *DeriveOutputColumns(uint32_t child_index) const;

  CColRefSet *DeriveNotNullColumns() const;
  CColRefSet *DeriveNotNullColumns(uint32_t child_index) const;

  CColRefSet *DeriveCorrelatedApplyColumns() const;
  CColRefSet *DeriveCorrelatedApplyColumns(uint32_t child_index) const;

  CMaxCard DeriveMaxCard() const;
  CMaxCard DeriveMaxCard(uint32_t child_index) const;

  CKeyCollection *DeriveKeyCollection() const;
  CKeyCollection *DeriveKeyCollection(uint32_t child_index) const;

  CPropConstraint *DerivePropertyConstraint() const;
  CPropConstraint *DerivePropertyConstraint(uint32_t child_index) const;

  uint32_t DeriveJoinDepth() const;
  uint32_t DeriveJoinDepth(uint32_t child_index) const;

  CFunctionProp *DeriveFunctionProperties() const;
  CFunctionProp *DeriveFunctionProperties(uint32_t child_index) const;

  CFunctionalDependencyArray *Pdrgpfd() const;
  CFunctionalDependencyArray *Pdrgpfd(uint32_t child_index) const;

  CPartInfo *DerivePartitionInfo() const;
  CPartInfo *DerivePartitionInfo(uint32_t child_index) const;

  CTableDescriptorHashSet *DeriveTableDescriptor() const;
  CTableDescriptorHashSet *DeriveTableDescriptor(uint32_t child_index) const;

  // Scalar property accessors
  CColRefSet *DeriveDefinedColumns(uint32_t child_index) const;
  CColRefSet *DeriveUsedColumns(uint32_t child_index) const;
  CColRefSet *DeriveSetReturningFunctionColumns(uint32_t child_index) const;
  bool DeriveHasSubquery(uint32_t child_index) const;
  CPartInfo *DeriveScalarPartitionInfo(uint32_t child_index) const;
  CFunctionProp *DeriveScalarFunctionProperties(uint32_t child_index) const;
  bool DeriveHasNonScalarFunction(uint32_t child_index) const;
  uint32_t DeriveTotalDistinctAggs(uint32_t child_index) const;
  bool DeriveHasMultipleDistinctAggs(uint32_t child_index) const;
  bool DeriveHasScalarArrayCmp(uint32_t child_index) const;
  bool DeriveHasScalarFuncProject(uint32_t child_index) const;
  bool DeriveContainsOnlyReplicationSafeAggFuncs(uint32_t child_index) const;

};  // class CExpressionHandle

}  // namespace gpopt

#endif  // !GPOPT_CExpressionHandle_H

// EOF
