//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CUtils.h
//
//	@doc:
//		Optimizer utility functions
//---------------------------------------------------------------------------
#ifndef GPOPT_CUtils_H
#define GPOPT_CUtils_H

#include "gpopt/base/CColRef.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarArrayCmp.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/xforms/CXform.h"
#include "gpos/common/CHashSet.h"

// fwd declarations
namespace gpmd {
class IMDId;
}

namespace gpopt {
using namespace gpos;

#define SORT_ASC 0
#define SORT_DESC 1

// fwd declaration
class CMemo;
class CLogicalCTEConsumer;
class CLogicalCTEProducer;
class IConstExprEvaluator;
class CLogical;
class CLogicalGbAgg;

//---------------------------------------------------------------------------
//	@class:
//		CUtils
//
//	@doc:
//		General utility functions
//
//---------------------------------------------------------------------------
class CUtils {
 private:
  // check if the expression is a scalar boolean const
  static bool FScalarConstBool(CExpression *pexpr, bool value);

  // check if two expressions have the same children in any order
  static bool FMatchChildrenUnordered(const CExpression *pexprLeft, const CExpression *pexprRight);

  // check if two expressions have the same children in the same order
  static bool FMatchChildrenOrdered(const CExpression *pexprLeft, const CExpression *pexprRight);

  // checks that the given type has all the comparisons: Eq, NEq, L, LEq, G, GEq
  static bool FHasAllDefaultComparisons(const IMDType *pmdtype);

  //	append the expressions in the source array to destination array
  static void AppendArrayExpr(CExpressionArray *pdrgpexprSrc, CExpressionArray *pdrgpexprDest);

 public:
#ifdef GPOS_DEBUG

  // print given expression to debug trace
  static void PrintExpression(CExpression *pexpr);

  // print memo to debug trace
  static void PrintMemo(CMemo *pmemo);

#endif  // GPOS_DEBUG

  static IOstream &OsPrintDrgPcoldesc(IOstream &os, CColumnDescriptorArray *pdrgpcoldescIncludedCols, uint32_t length);

  //-------------------------------------------------------------------
  // Helpers for generating expressions
  //-------------------------------------------------------------------

  // generate a comparison expression for two column references
  static CExpression *PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft, const CColRef *pcrRight,
                                     CWStringConst strOp, IMDId *mdid_op);

  // generate a comparison expression for a column reference and an expression
  static CExpression *PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft, CExpression *pexprRight,
                                     CWStringConst strOp, IMDId *mdid_op);

  // generate a comparison expression for an expression and a column reference
  static CExpression *PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft, const CColRef *pcrRight,
                                     CWStringConst strOp, IMDId *mdid_op);

  // generate a comparison expression for two expressions
  static CExpression *PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                     CWStringConst strOp, IMDId *mdid_op);

  // generate a comparison expression for two expressions
  static CExpression *PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                     IMDId *mdid_scop);

  // generate a comparison expression for a column reference and an expression
  static CExpression *PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft, CExpression *pexprRight,
                                     IMDType::ECmpType cmp_type);

  // generate a comparison expression between two column references
  static CExpression *PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft, const CColRef *pcrRight,
                                     IMDType::ECmpType cmp_type);

  // generate a comparison expression between an expression and a column reference
  static CExpression *PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft, const CColRef *pcrRight,
                                     IMDType::ECmpType cmp_type);

  // generate a comparison expression for two expressions
  static CExpression *PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                     IMDType::ECmpType cmp_type);

  // generate a comparison against Zero
  static CExpression *PexprCmpWithZero(CMemoryPool *mp, CExpression *pexprLeft, IMDId *mdid_type_left,
                                       IMDType::ECmpType ecmptype);

  // generate an equality comparison expression for column references
  static CExpression *PexprScalarEqCmp(CMemoryPool *mp, const CColRef *pcrLeft, const CColRef *pcrRight);

  // generate an equality comparison expression for two expressions
  static CExpression *PexprScalarEqCmp(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight);

  // generate an equality comparison expression for a column reference and an expression
  static CExpression *PexprScalarEqCmp(CMemoryPool *mp, const CColRef *pcrLeft, CExpression *pexprRight);

  // generate an equality comparison expression for an expression and a column reference
  static CExpression *PexprScalarEqCmp(CMemoryPool *mp, CExpression *pexprLeft, const CColRef *pcrRight);

  // generate an array comparison expression for a column reference and an expression
  static CExpression *PexprScalarArrayCmp(CMemoryPool *mp, CScalarArrayCmp::EArrCmpType earrcmptype,
                                          IMDType::ECmpType ecmptype, CExpressionArray *pexprScalarChildren,
                                          const CColRef *colref);

  // generate an Is Distinct From expression
  static CExpression *PexprIDF(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight);

  static CExpression *PexprIDF(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight, IMDId *mdid_scop);

  // generate an Is NOT Distinct From expression for two column references
  static CExpression *PexprINDF(CMemoryPool *mp, const CColRef *pcrLeft, const CColRef *pcrRight);

  // generate an Is NOT Distinct From expression for scalar expressions
  static CExpression *PexprINDF(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight);

  static CExpression *PexprINDF(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight, IMDId *mdid_scop);

  // generate an Is NULL expression
  static CExpression *PexprIsNull(CMemoryPool *mp, CExpression *pexpr);

  // generate an Is NOT NULL expression
  static CExpression *PexprIsNotNull(CMemoryPool *mp, CExpression *pexpr);

  // generate an Is NOT FALSE expression
  static CExpression *PexprIsNotFalse(CMemoryPool *mp, CExpression *pexpr);

  // find if a scalar expression uses a nullable columns from the output of a logical expression
  static bool FUsesNullableCol(CMemoryPool *mp, CExpression *pexprScalar, CExpression *pexprLogical);

  // generate a scalar op expression for a column reference and an expression
  static CExpression *PexprScalarOp(CMemoryPool *mp, const CColRef *pcrLeft, CExpression *pexpr, CWStringConst strOp,
                                    IMDId *mdid_op, IMDId *return_type_mdid = nullptr);

  // generate a scalar bool op expression
  static CExpression *PexprScalarBoolOp(CMemoryPool *mp, CScalarBoolOp::EBoolOperator eboolop,
                                        CExpressionArray *pdrgpexpr);

  // negate the given expression
  static CExpression *PexprNegate(CMemoryPool *mp, CExpression *pexpr);

  // generate a scalar ident expression
  static CExpression *PexprScalarIdent(CMemoryPool *mp, const CColRef *colref);

  // generate a scalar project element expression
  static CExpression *PexprScalarProjectElement(CMemoryPool *mp, CColRef *colref, CExpression *pexpr);

  // generate an aggregate function operator
  static CScalarAggFunc *PopAggFunc(
      CMemoryPool *mp, IMDId *pmdidAggFunc, const CWStringConst *pstrAggFunc, bool is_distinct,
      EAggfuncStage eaggfuncstage, bool fSplit,
      IMDId *pmdidResolvedReturnType,  // return type to be used if original return type is ambiguous
      EAggfuncKind aggkind, ULongPtrArray *argtypes, bool fRepSafe);

  // generate an aggregate function
  static CExpression *PexprAggFunc(CMemoryPool *mp, IMDId *pmdidAggFunc, const CWStringConst *pstrAggFunc,
                                   const CColRef *colref, bool is_distinct, EAggfuncStage eaggfuncstage, bool fSplit);

  // generate a count(*) expression
  static CExpression *PexprCountStar(CMemoryPool *mp);

  // generate a GbAgg with count(*) function over the given expression
  static CExpression *PexprCountStar(CMemoryPool *mp, CExpression *pexprLogical);

  // return True if passed expression is a Project Element defined on count(*)/count(any) agg
  static bool FCountAggProjElem(CExpression *pexprPrjElem, CColRef **ppcrCount);

  // check if given expression has a count(*)/count(Any) agg
  static bool FHasCountAgg(CExpression *pexpr, CColRef **ppcrCount);

  // check if given expression has count matching the given column, returns the Logical GroupBy Agg above
  static bool FHasCountAggMatchingColumn(const CExpression *pexpr, const CColRef *colref,
                                         const CLogicalGbAgg **ppgbAgg);

  // generate a GbAgg with count(*) and sum(col) over the given expression
  static CExpression *PexprCountStarAndSum(CMemoryPool *mp, const CColRef *colref, CExpression *pexprLogical);

  // generate a sum(col) expression
  static CExpression *PexprSum(CMemoryPool *mp, const CColRef *colref);

  // generate a GbAgg with sum(col) expressions for all columns in the given array
  static CExpression *PexprGbAggSum(CMemoryPool *mp, CExpression *pexprLogical, CColRefArray *pdrgpcrSum);

  // generate a count(col) expression
  static CExpression *PexprCount(CMemoryPool *mp, const CColRef *colref, bool is_distinct);

  // generate a min(col) expression
  static CExpression *PexprMin(CMemoryPool *mp, CMDAccessor *md_accessor, const CColRef *colref);

  // generate an aggregate expression
  static CExpression *PexprAgg(CMemoryPool *mp, CMDAccessor *md_accessor, IMDType::EAggType agg_type,
                               const CColRef *colref, bool is_distinct);

  // generate a select expression
  static CExpression *PexprLogicalSelect(CMemoryPool *mp, CExpression *pexpr, CExpression *pexprPredicate);

  // if predicate is True return logical expression, otherwise return a new select node
  static CExpression *PexprSafeSelect(CMemoryPool *mp, CExpression *pexprLogical, CExpression *pexprPredicate);

  // generate a select expression, if child is another Select expression collapse both Selects into one expression
  static CExpression *PexprCollapseSelect(CMemoryPool *mp, CExpression *pexpr, CExpression *pexprPredicate);

  // generate a project expression
  static CExpression *PexprLogicalProject(CMemoryPool *mp, CExpression *pexpr, CExpression *pexprPrjList,
                                          bool fNewComputedCol);

  // generate a sequence project expression
  static CExpression *PexprLogicalSequenceProject(CMemoryPool *mp, COrderSpecArray *pdrgpos, CWindowFrameArray *pdrgpwf,
                                                  CExpression *pexpr, CExpression *pexprPrjList);

  // generate a projection of NULL constants
  // to the map 'colref_mapping', and add the mappings to the colref_mapping map if not NULL
  static CExpression *PexprLogicalProjectNulls(CMemoryPool *mp, CColRefArray *colref_array, CExpression *pexpr,
                                               UlongToColRefMap *colref_mapping = nullptr);

  // construct a project list using the given columns and datums
  // store the mapping in the colref_mapping map if not NULL
  static CExpression *PexprScalarProjListConst(CMemoryPool *mp, CColRefArray *colref_array, IDatumArray *pdrgpdatum,
                                               UlongToColRefMap *colref_mapping);

  // generate a project expression
  static CExpression *PexprAddProjection(CMemoryPool *mp, CExpression *pexpr, CExpression *pexprProjected);

  // generate a project expression with one or more additional project elements
  static CExpression *PexprAddProjection(CMemoryPool *mp, CExpression *pexpr, CExpressionArray *pdrgpexprProjected,
                                         bool fNewComputedCol = true);

  // generate an aggregate expression
  static CExpression *PexprLogicalGbAggGlobal(CMemoryPool *mp, CColRefArray *colref_array, CExpression *pexpr,
                                              CExpression *pexprPrL);

  // generate an aggregate expression
  static CExpression *PexprLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array, CExpression *pexpr,
                                        CExpression *pexprPrL, COperator::EGbAggType egbaggtype);

  // check if the aggregate is local or global
  static bool FHasGlobalAggFunc(const CExpression *pexprProjList);

  // check if given project list has only aggregate functions
  // that can be safely executed on replicated slices
  static bool FContainsOnlyReplicationSafeAggFuncs(const CExpression *pexprProjList);

  // generate a bool expression
  static CExpression *PexprScalarConstBool(CMemoryPool *mp, bool value, bool is_null = false);

  // generate an int4 expression
  static CExpression *PexprScalarConstInt4(CMemoryPool *mp, int32_t val);

  // generate an int8 expression
  static CExpression *PexprScalarConstInt8(CMemoryPool *mp, int64_t val, bool is_null = false);

  // generate an oid constant expression
  static CExpression *PexprScalarConstOid(CMemoryPool *mp, OID oid_val);

  // generate a NULL constant of a given type
  static CExpression *PexprScalarConstNull(CMemoryPool *mp, const IMDType *typ, int32_t type_modifier);

  // comparison operator type
  static IMDType::ECmpType ParseCmpType(IMDId *mdid);

  // comparison operator type
  static IMDType::ECmpType ParseCmpType(CMDAccessor *md_accessor, IMDId *mdid);

  // generate a binary join expression
  template <class T>
  static CExpression *PexprLogicalJoin(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                       CExpression *pexprPredicate,
                                       CXform::EXformId origin_xform = CXform::ExfSentinel);

  // generate an apply expression
  template <class T>
  static CExpression *PexprLogicalApply(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                        CExpression *pexprPred = nullptr);

  // generate an apply expression with a known inner column
  template <class T>
  static CExpression *PexprLogicalApply(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                        const CColRef *pcrInner, COperator::EOperatorId eopidOriginSubq,
                                        CExpression *pexprPred = nullptr);

  // generate an apply expression with a known array of inner columns
  template <class T>
  static CExpression *PexprLogicalApply(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                        CColRefArray *pdrgpcrInner, COperator::EOperatorId eopidOriginSubq,
                                        CExpression *pexprPred = nullptr);

  // generate a correlated apply for quantified subquery with a known array of inner columns
  template <class T>
  static CExpression *PexprLogicalCorrelatedQuantifiedApply(CMemoryPool *mp, CExpression *pexprLeft,
                                                            CExpression *pexprRight, CColRefArray *pdrgpcrInner,
                                                            COperator::EOperatorId eopidOriginSubq,
                                                            CExpression *pexprPred = nullptr);

  //-------------------------------------------------------------------
  // Helpers for partitioning
  //-------------------------------------------------------------------

  // extract the nth partition key from the given array of partition keys
  static CColRef *PcrExtractPartKey(CColRef2dArray *pdrgpdrgpcr, uint32_t ulLevel);

  //-------------------------------------------------------------------
  // Helpers for comparisons
  //-------------------------------------------------------------------

  // deduplicate array of expressions
  static CExpressionArray *PdrgpexprDedup(CMemoryPool *mp, CExpressionArray *pdrgpexpr);

  // deep equality of expression trees
  static bool Equals(const CExpression *pexprLeft, const CExpression *pexprRight);

  // compare expression against an array of expressions
  static bool FEqualAny(const CExpression *pexpr, const CExpressionArray *pdrgpexpr);

  // deep equality of expression arrays
  static bool Equals(const CExpressionArray *pdrgpexprLeft, const CExpressionArray *pdrgpexprRight);

  // check if first expression array contains all expressions in second array
  static bool Contains(const CExpressionArray *pdrgpexprFst, const CExpressionArray *pdrgpexprSnd);

  // return the number of occurrences of the given expression in the given
  // array of expressions
  static uint32_t UlOccurrences(const CExpression *pexpr, CExpressionArray *pdrgpexpr);

  //-------------------------------------------------------------------
  // Helpers for datums
  //-------------------------------------------------------------------

  // check to see if the expression is a scalar const TRUE
  static bool FScalarConstTrue(CExpression *pexpr);

  // check to see if the expression is a scalar const FALSE
  static bool FScalarConstFalse(CExpression *pexpr);

  // check if the given expression is an int32_t, the template parameter is an int32_t type
  template <class T>
  static bool FScalarConstInt(CExpression *pexpr);

  //-------------------------------------------------------------------
  // Helpers for printing
  //-------------------------------------------------------------------

  // column reference array print helper
  static IOstream &OsPrintDrgPcr(IOstream &os, const CColRefArray *colref_array, uint32_t = UINT32_MAX);

  //-------------------------------------------------------------------
  // Helpers for column reference sets
  //-------------------------------------------------------------------

  // create an array of output columns including a key for grouping
  static CColRefArray *PdrgpcrGroupingKey(CMemoryPool *mp, CExpression *pexpr, CColRefArray **ppdrgpcrKey);

  // add an equivalence class (col ref set) to the array. If the new equiv
  // class contains columns from existing equiv classes, then these are merged
  static CColRefSetArray *AddEquivClassToArray(CMemoryPool *mp, const CColRefSet *pcrsNew,
                                               const CColRefSetArray *pdrgpcrs);

  // merge 2 arrays of equivalence classes
  static CColRefSetArray *PdrgpcrsMergeEquivClasses(CMemoryPool *mp, CColRefSetArray *pdrgpcrsFst,
                                                    CColRefSetArray *pdrgpcrsSnd);

  // intersect 2 arrays of equivalence classes
  static CColRefSetArray *PdrgpcrsIntersectEquivClasses(CMemoryPool *mp, CColRefSetArray *pdrgpcrsFst,
                                                        CColRefSetArray *pdrgpcrsSnd);

  // return a copy of equivalence classes from all children
  static CColRefSetArray *PdrgpcrsCopyChildEquivClasses(CMemoryPool *mp, CExpressionHandle &exprhdl);

  // return a copy of the given array of columns, excluding the columns
  // in the given colrefset
  static CColRefArray *PdrgpcrExcludeColumns(CMemoryPool *mp, CColRefArray *pdrgpcrOriginal, CColRefSet *pcrsExcluded);

  //-------------------------------------------------------------------
  // General helpers
  //-------------------------------------------------------------------

  // append elements from input array to output array, starting from given index, after add-refing them
  template <class T, void (*CleanupFn)(T *)>
  static void AddRefAppend(CDynamicPtrArray<T, CleanupFn> *pdrgptOutput, CDynamicPtrArray<T, CleanupFn> *pdrgptInput,
                           uint32_t ulStart = 0);

  // check for existence of subqueries
  static bool FHasSubquery(CExpression *pexpr);

  // check existence of subqueries or Apply operators in deep expression tree
  static bool FHasSubqueryOrApply(CExpression *pexpr, bool fCheckRoot = true);

  // check existence of correlated apply operators in deep expression tree
  static bool FHasCorrelatedApply(CExpression *pexpr, bool fCheckRoot = true);

  // check for existence of CTE anchor
  static bool FHasCTEAnchor(CExpression *pexpr);

  // check for existence of outer references
  static bool HasOuterRefs(CExpression *pexpr);

  // check if a given operator is a logical join
  static bool FLogicalJoin(COperator *pop);

  // check if a given operator is a logical set operation
  static bool FLogicalSetOp(COperator *pop);

  // check if a given operator is a logical unary operator
  static bool FLogicalUnary(COperator *pop);

  // check if a given operator is a physical join
  static bool FPhysicalJoin(COperator *pop);

  // check if a given operator is a physical scan
  static bool FPhysicalScan(COperator *pop);

  // check if a given operator is a physical agg
  static bool FPhysicalAgg(COperator *pop);

  // check if given expression has any one stage agg nodes
  static bool FHasOneStagePhysicalAgg(const CExpression *pexpr);

  // check if a given operator is an enforcer
  static bool FEnforcer(COperator *pop);

  // check if a given operator is a hash join
  static bool FHashJoin(COperator *pop);

  // check if a given operator is a correlated nested loops join
  static bool FCorrelatedNLJoin(COperator *pop);

  // check if a given operator is a nested loops join
  static bool FNLJoin(COperator *pop);

  // check if a given operator is an Apply
  static bool FApply(COperator *pop);

  // check if a given operator is a correlated Apply
  static bool FCorrelatedApply(COperator *pop);

  // check if a given operator is left semi apply
  static bool FLeftSemiApply(COperator *pop);

  // check if a given operator is left anti semi apply
  static bool FLeftAntiSemiApply(COperator *pop);

  // check if a given operator is a subquery
  static bool FSubquery(COperator *pop);

  // check if a given operator is existential subquery
  static bool FExistentialSubquery(COperator *pop);

  // check if a given operator is quantified subquery
  static bool FQuantifiedSubquery(COperator *pop);

  // check if given expression is a Project Element with scalar subquery
  static bool FProjElemWithScalarSubq(CExpression *pexpr);

  // check if given expression is a scalar subquery with a ConstTableGet as the only child
  static bool FScalarSubqWithConstTblGet(CExpression *pexpr);

  // check if given expression is a Project on ConstTable with one scalar subquery in Project List
  static bool FProjectConstTableWithOneScalarSubq(CExpression *pexpr);

  // check if an expression is a 0 offset
  static bool FScalarConstIntZero(CExpression *pexprOffset);

  // check if a limit expression has 0 offset
  static bool FHasZeroOffset(CExpression *pexpr);

  // check if expression is scalar comparison
  static bool FScalarCmp(CExpression *pexpr);

  // check if expression is scalar array comparison
  static bool FScalarArrayCmp(CExpression *pexpr);

  // check if given operator exists in the given list
  static bool FOpExists(const COperator *pop, const COperator::EOperatorId *peopid, uint32_t ulOps);

  // check if given expression has any operator in the given list
  static bool FHasOp(const CExpression *pexpr, const COperator::EOperatorId *peopid, uint32_t ulOps);

  // return number of inlinable CTEs in the given expression
  static uint32_t UlInlinableCTEs(CExpression *pexpr, uint32_t ulDepth = 1);

  // return number of joins in the given expression
  static uint32_t UlJoins(CExpression *pexpr);

  // return number of subqueries in the given expression
  static uint32_t UlSubqueries(CExpression *pexpr);

  // check if expression is scalar bool op
  static bool FScalarBoolOp(CExpression *pexpr);

  // is the given expression a scalar bool op of the passed type?
  static bool FScalarBoolOp(CExpression *pexpr, CScalarBoolOp::EBoolOperator eboolop);

  // check if expression is scalar bool test op
  static bool FScalarBooleanTest(CExpression *pexpr);

  // check if expression is scalar null test
  static bool FScalarNullTest(CExpression *pexpr);

  // check if given expression is a NOT NULL predicate
  static bool FScalarNotNull(CExpression *pexpr);

  // check if expression is scalar identifier
  static bool FScalarIdent(CExpression *pexpr);

  // check if expression is scalar identifier (with or without a cast)
  static bool FScalarIdentIgnoreCast(CExpression *pexpr);

  static bool FScalarConstAndScalarIdentArray(CExpression *pexprArray);

  // check if expression is scalar identifier of boolean type
  static bool FScalarIdentBoolType(CExpression *pexpr);

  // check if expression is scalar array
  static bool FScalarArray(CExpression *pexpr);

  // returns number of children or constants of it is all constants
  static uint32_t UlScalarArrayArity(CExpression *pexpr);

  // returns constant operator of a scalar array expression
  static CScalarConst *PScalarArrayConstChildAt(CExpression *pexprArray, uint32_t ul);

  // returns constant expression of a scalar array expression
  static CExpression *PScalarArrayExprChildAt(CMemoryPool *mp, CExpression *pexprArray, uint32_t ul);

  // returns the scalar array expression child of CScalarArrayComp
  static CExpression *PexprScalarArrayChild(CExpression *pexpr);

  // returns if the scalar array has all constant elements or children
  static bool FScalarConstArray(CExpression *pexpr);

  // returns if the scalar constant is an array
  static bool FIsConstArray(CExpression *pexpr);

  // returns MDId for gp_percentile based on return type
  static CMDIdGPDB *GetPercentileAggMDId(CMemoryPool *mp, CExpression *pexprAggFn);

  // returns if the scalar constant array has already been collapased
  static bool FScalarArrayCollapsed(CExpression *pexprArray);

  // returns true if the subquery is a ScalarSubqueryAny
  static bool FAnySubquery(COperator *pop);

  // returns true if the subquery is a ScalarSubqueryExists
  static bool FExistsSubquery(COperator *pop);

  // returns true if the expression is a correlated EXISTS/ANY subquery
  static bool FCorrelatedExistsAnySubquery(CExpression *pexpr);

  static CScalarProjectElement *PNthProjectElement(CExpression *pexpr, uint32_t ul);

  // returns the expression under the Nth project element of a CLogicalProject
  static CExpression *PNthProjectElementExpr(CExpression *pexpr, uint32_t ul);

  // check if the Project list has an inner reference assuming project list has one projecet element
  static bool FInnerRefInProjectList(CExpression *pexpr);

  // Check if expression tree has a col being referenced in the CColRefSet passed as input
  static bool FExprHasAnyCrFromCrs(CExpression *pexpr, CColRefSet *pcrs);

  // If it's a scalar array of all CScalarConst, collapse it into a single
  // expression but keep the constants in the operator.
  static CExpression *PexprCollapseConstArray(CMemoryPool *mp, CExpression *pexprArray);

  // check if expression is scalar array coerce
  static bool FScalarArrayCoerce(CExpression *pexpr);

  // is the given expression a scalar identifier with the given column reference
  static bool FScalarIdent(CExpression *pexpr, CColRef *colref);

  // check if expression is scalar const
  static bool FScalarConst(CExpression *pexpr);

  // check if this is a variable-free expression
  static bool FVarFreeExpr(CExpression *pexpr);

  // check if expression is a predicate
  static bool FPredicate(CExpression *pexpr);

  // is this type supported in contradiction detection using stats logic
  static bool FIntType(IMDId *mdid_type);

  // is this type supported in contradiction detection
  static bool FConstrainableType(IMDId *mdid_type);

  // check if a binary operator uses only columns produced by its children
  static bool FUsesChildColsOnly(CExpressionHandle &exprhdl);

  // check if inner child of a binary operator uses columns not produced by outer child
  static bool FInnerUsesExternalCols(CExpressionHandle &exprhdl);

  // check if inner child of a binary operator uses only columns not produced by outer child
  static bool FInnerUsesExternalColsOnly(CExpressionHandle &exprhdl);

  // check if comparison operators are available for the given columns
  static bool FComparisonPossible(CColRefArray *colref_array, IMDType::ECmpType cmp_type);

  static uint32_t UlCountOperator(const CExpression *pexpr, COperator::EOperatorId op_id);

  // check if hashing is possible for the given columns
  static bool IsHashable(CColRefArray *colref_array);

  // check if the given operator is a logical DML operator
  static bool FLogicalDML(COperator *pop);

  // return regular string from wide-character string
  static char *CreateMultiByteCharStringFromWCString(CMemoryPool *mp, wchar_t *wsz);

  // return column reference defined by project element
  static CColRef *PcrFromProjElem(CExpression *pexprPrEl);

  // construct an array of colids from the given array of column references
  static ULongPtrArray *Pdrgpul(CMemoryPool *mp, const CColRefArray *colref_array);

  // generate a timestamp-based file name
  static void GenerateFileName(char *buf, const char *szPrefix, const char *szExt, uint32_t length,
                               uint32_t ulSessionId, uint32_t ulCmdId);

  // return the mapping of the given colref based on the given hashmap
  static CColRef *PcrRemap(const CColRef *colref, UlongToColRefMap *colref_mapping, bool must_exist);

  // create a new colrefset corresponding to the given colrefset
  // and based on the given mapping
  static CColRefSet *PcrsRemap(CMemoryPool *mp, CColRefSet *pcrs, UlongToColRefMap *colref_mapping, bool must_exist);

  // create an array of column references corresponding to the given array
  // and based on the given mapping
  static CColRefArray *PdrgpcrRemap(CMemoryPool *mp, CColRefArray *colref_array, UlongToColRefMap *colref_mapping,
                                    bool must_exist);

  // create an array of column references corresponding to the given array
  // and based on the given mapping and create new colrefs if necessary
  static CColRefArray *PdrgpcrRemapAndCreate(CMemoryPool *mp, CColRefArray *colref_array,
                                             UlongToColRefMap *colref_mapping);

  // create an array of column arrays corresponding to the given array
  // and based on the given mapping
  static CColRef2dArray *PdrgpdrgpcrRemap(CMemoryPool *mp, CColRef2dArray *pdrgpdrgpcr,
                                          UlongToColRefMap *colref_mapping, bool must_exist);

  // remap given array of expressions with provided column mappings
  static CExpressionArray *PdrgpexprRemap(CMemoryPool *mp, CExpressionArray *pdrgpexpr,
                                          UlongToColRefMap *colref_mapping);

  // create ColRef->ColRef mapping using the given ColRef arrays
  static UlongToColRefMap *PhmulcrMapping(CMemoryPool *mp, CColRefArray *pdrgpcrFrom, CColRefArray *pdrgpcrTo);

  // add col ID->ColRef mappings to the given hashmap based on the
  // given ColRef arrays
  static void AddColumnMapping(CMemoryPool *mp, UlongToColRefMap *colref_mapping, CColRefArray *pdrgpcrFrom,
                               CColRefArray *pdrgpcrTo);

  // create a copy of the array of column references
  static CColRefArray *PdrgpcrExactCopy(CMemoryPool *mp, CColRefArray *colref_array);

  // create an array of new column references with the same names and
  // types as the given column references.
  // if the passed map is not null, mappings from old to copied variables are added to it
  static CColRefArray *PdrgpcrCopy(CMemoryPool *mp, CColRefArray *colref_array, bool fAllComputed = false,
                                   UlongToColRefMap *colref_mapping = nullptr);

  // equality check between two arrays of column refs. Inputs can be NULL
  static bool Equals(CColRefArray *pdrgpcrFst, CColRefArray *pdrgpcrSnd);

  // compute hash value for an array of column references
  static uint32_t UlHashColArray(const CColRefArray *colref_array, const uint32_t ulMaxCols = 5);

  // return the set of column reference from the CTE Producer corresponding to the
  // subset of input columns from the CTE Consumer
  static CColRefSet *PcrsCTEProducerColumns(CMemoryPool *mp, CColRefSet *pcrsInput,
                                            CLogicalCTEConsumer *popCTEConsumer);

  // construct the join condition (AND-tree of INDF operators)
  // from the array of input columns reference arrays (aligned)
  static CExpression *PexprConjINDFCond(CMemoryPool *mp, CColRef2dArray *pdrgpdrgpcrInput);

  // check whether a colref array contains repeated items
  static bool FHasDuplicates(const CColRefArray *colref_array);

  // cast the input expression to the destination mdid
  static CExpression *PexprCast(CMemoryPool *mp, CMDAccessor *md_accessor, CExpression *pexpr, IMDId *mdid_dest);

  // construct a func element expr for array coerce
  static CExpression *PexprFuncElemExpr(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid_func,
                                        IMDId *mdid_elem_type, int32_t typmod);

  // construct a logical join expression of the given type, with the given children
  static CExpression *PexprLogicalJoin(CMemoryPool *mp, EdxlJoinType edxljointype, CExpressionArray *pdrgpexpr);

  // construct an array of scalar ident expressions from the given array
  // of column references
  static CExpressionArray *PdrgpexprScalarIdents(CMemoryPool *mp, CColRefArray *colref_array);

  // return the columns from the scalar ident expressions in the given array
  static CColRefSet *PcrsExtractColumns(CMemoryPool *mp, const CExpressionArray *pdrgpexpr);

  // create a new bitset of the given length, where all the bits are set
  static CBitSet *PbsAllSet(CMemoryPool *mp, uint32_t size);

  // return a new bitset, setting the bits in the given array
  static CBitSet *Pbs(CMemoryPool *mp, ULongPtrArray *pdrgpul);

  // helper to create a dummy constant table expression
  static CExpression *PexprLogicalCTGDummy(CMemoryPool *mp);

  // map a column from source array to destination array based on position
  static CColRef *PcrMap(CColRef *pcrSource, CColRefArray *pdrgpcrSource, CColRefArray *pdrgpcrTarget);

  //	return index of the set containing given column
  static uint32_t UlPcrIndexContainingSet(CColRefSetArray *pdrgpcrs, const CColRef *colref);

  // collapse the top two project nodes, if unable return NULL
  static CExpression *PexprCollapseProjects(CMemoryPool *mp, CExpression *pexpr);

  // match function between index get/scan operators
  template <class T>
  static bool FMatchIndex(T *pop1, COperator *pop2);

  // match function between dynamic index get/scan operators
  template <class T>
  static bool FMatchDynamicIndex(T *pop1, COperator *pop2);

  // match function between dynamic get/scan operators
  template <class T>
  static bool FMatchDynamicScan(T *pop1, COperator *pop2);

  // match function between dynamic bitmap get/scan operators
  template <class T>
  static bool FMatchDynamicBitmapScan(T *pop1, COperator *pop2);

  // match function between bitmap get/scan operators
  template <class T>
  static bool FMatchBitmapScan(T *pop1, COperator *pop2);

  // compares two Idatums, useful for sorting functions
  static int32_t IDatumCmp(const void *val1, const void *val2);

  // compares two CPoints, useful for sorting functions
  static int32_t CPointCmp(const void *val1, const void *val2);

  // check if the equivalance classes are disjoint
  static bool FEquivalanceClassesDisjoint(CMemoryPool *mp, const CColRefSetArray *pdrgpcrs);

  // check if the equivalance classes are same
  static bool FEquivalanceClassesEqual(CMemoryPool *mp, CColRefSetArray *pdrgpcrsFst, CColRefSetArray *pdrgpcrsSnd);

  // generate a limit expression on top of the given relational child with the given offset and limit count
  static CExpression *PexprLimit(CMemoryPool *mp, CExpression *pexpr, uint32_t ulOffSet, uint32_t count);

  // generate a limit expression on top of the given relational child with given offset, limit count and OrderSpec
  static CExpression *BuildLimitExprWithOrderSpec(CMemoryPool *mp, CExpression *pexpr, COrderSpec *pos,
                                                  uint32_t ulOffSet, uint32_t count);

  // return true if given expression contains window aggregate function
  static bool FHasAggWindowFunc(CExpression *pexpr);

  // return true if given expression contains ordered aggregate function
  static bool FHasOrderedAggToSplit(CExpression *pexpr);

  // return true if the given expression is a cross join
  static bool FCrossJoin(CExpression *pexpr);

  // return true if can create hash join for the expression
  static bool IsHashJoinPossible(CMemoryPool *mp, CExpression *pexpr);

  // is this scalar expression an NDV-preserving function (used for join stats derivation)
  static bool IsExprNDVPreserving(CExpression *pexpr, const CColRef **underlying_colref);

  // search the given array of predicates for predicates with equality or IS NOT
  // DISTINCT FROM operators that has one side equal to the given expression
  static CExpression *PexprMatchEqualityOrINDF(CExpression *pexprToMatch,
                                               CExpressionArray *pdrgpexpr  // array of predicates to inspect
  );

  static CExpression *MakeJoinWithoutInferredPreds(CMemoryPool *mp, CExpression *join_expr);

  static bool Contains(const CExpressionArray *exprs, CExpression *expr_to_match);

  static bool Equals(const CExpressionArrays *exprs_arr, const CExpressionArrays *other_exprs_arr);

  static bool Equals(const IMdIdArray *mdids, const IMdIdArray *other_mdids);

  static bool Equals(const IMDId *mdid, const IMDId *other_mdid);

  static bool CanRemoveInferredPredicates(COperator::EOperatorId op_id);

  static CExpressionArrays *GetCombinedExpressionArrays(CMemoryPool *mp, CExpressionArrays *exprs_array,
                                                        CExpressionArrays *exprs_array_other);

  static void AddExprs(CExpressionArrays *results_exprs, CExpressionArrays *input_exprs);

  static bool FScalarConstBoolNull(CExpression *pexpr);

  static bool FScalarConstOrBinaryCoercible(CExpression *pexpr);

  static bool FScalarIdentNullTest(CExpression *pexpr);

  static bool FContainsScalarIdentNullTest(CExpression *pexpr);

  static CTableDescriptorHashSet *RemoveDuplicateMdids(CMemoryPool *mp, CTableDescriptorHashSet *tabdescs);

  static CExpression *ReplaceColrefWithProjectExpr(CMemoryPool *mp, CExpression *pexpr, CColRef *pcolref,
                                                   CExpression *pprojExpr);

};  // class CUtils

// hash set from expressions
using ExprHashSet = CHashSet<CExpression, CExpression::UlHashDedup, CUtils::Equals, CleanupRelease<CExpression>>;

//---------------------------------------------------------------------------
//	@function:
//		CUtils::PexprLogicalJoin
//
//	@doc:
//		Generate a join expression from given expressions
//
//---------------------------------------------------------------------------
template <class T>
CExpression *CUtils::PexprLogicalJoin(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                      CExpression *pexprPredicate, CXform::EXformId origin_xform) {
  GPOS_ASSERT(nullptr != pexprLeft);
  GPOS_ASSERT(nullptr != pexprRight);
  GPOS_ASSERT(nullptr != pexprPredicate);

  return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) T(mp, origin_xform), pexprLeft, pexprRight, pexprPredicate);
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::PexprLogicalApply
//
//	@doc:
//		Generate an apply expression from given expressions
//
//---------------------------------------------------------------------------
template <class T>
CExpression *CUtils::PexprLogicalApply(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                       CExpression *pexprPred) {
  GPOS_ASSERT(nullptr != pexprLeft);
  GPOS_ASSERT(nullptr != pexprRight);

  CExpression *pexprScalar = pexprPred;
  if (nullptr == pexprPred) {
    pexprScalar = PexprScalarConstBool(mp, true /*value*/);
  }

  return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) T(mp), pexprLeft, pexprRight, pexprScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::PexprLogicalApply
//
//	@doc:
//		Generate an apply expression with a known inner column
//
//---------------------------------------------------------------------------
template <class T>
CExpression *CUtils::PexprLogicalApply(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                       const CColRef *pcrInner, COperator::EOperatorId eopidOriginSubq,
                                       CExpression *pexprPred) {
  GPOS_ASSERT(nullptr != pexprLeft);
  GPOS_ASSERT(nullptr != pexprRight);
  GPOS_ASSERT(nullptr != pcrInner);

  CExpression *pexprScalar = pexprPred;
  if (nullptr == pexprPred) {
    pexprScalar = PexprScalarConstBool(mp, true /*value*/);
  }

  CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
  colref_array->Append(const_cast<CColRef *>(pcrInner));
  return GPOS_NEW(mp)
      CExpression(mp, GPOS_NEW(mp) T(mp, colref_array, eopidOriginSubq), pexprLeft, pexprRight, pexprScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::PexprLogicalApply
//
//	@doc:
//		Generate an apply expression with known array of inner columns
//
//---------------------------------------------------------------------------
template <class T>
CExpression *CUtils::PexprLogicalApply(CMemoryPool *mp, CExpression *pexprLeft, CExpression *pexprRight,
                                       CColRefArray *pdrgpcrInner, COperator::EOperatorId eopidOriginSubq,
                                       CExpression *pexprPred) {
  GPOS_ASSERT(nullptr != pexprLeft);
  GPOS_ASSERT(nullptr != pexprRight);
  GPOS_ASSERT(nullptr != pdrgpcrInner);
  GPOS_ASSERT(0 < pdrgpcrInner->Size());

  CExpression *pexprScalar = pexprPred;
  if (nullptr == pexprPred) {
    pexprScalar = PexprScalarConstBool(mp, true /*value*/);
  }

  return GPOS_NEW(mp)
      CExpression(mp, GPOS_NEW(mp) T(mp, pdrgpcrInner, eopidOriginSubq), pexprLeft, pexprRight, pexprScalar);
}

//---------------------------------------------------------------------------
//	@class:
//		CUtils::PexprLogicalCorrelatedQuantifiedApply
//
//	@doc:
//		Helper to create a left semi correlated apply from left semi apply
//
//---------------------------------------------------------------------------
template <class T>
CExpression *CUtils::PexprLogicalCorrelatedQuantifiedApply(CMemoryPool *mp, CExpression *pexprLeft,
                                                           CExpression *pexprRight, CColRefArray *pdrgpcrInner,
                                                           COperator::EOperatorId eopidOriginSubq,
                                                           CExpression *pexprPred) {
  GPOS_ASSERT(nullptr != pexprLeft);
  GPOS_ASSERT(nullptr != pexprRight);
  GPOS_ASSERT(nullptr != pdrgpcrInner);
  GPOS_ASSERT(0 < pdrgpcrInner->Size());

  CExpression *pexprScalar = pexprPred;
  if (nullptr == pexprPred) {
    pexprScalar = PexprScalarConstBool(mp, true /*value*/);
  }

  if (COperator::EopLogicalSelect != pexprRight->Pop()->Eopid()) {
    // quantified comparison was pushed down, we create a dummy comparison here
    GPOS_ASSERT(!CUtils::HasOuterRefs(pexprRight) &&
                "unexpected outer references in inner child of Semi Apply expression ");
    pexprScalar->Release();
    pexprScalar = PexprScalarConstBool(mp, true /*value*/);
  } else {
    // quantified comparison is now on top of inner expression, skip to child
    (*pexprRight)[1]->AddRef();
    CExpression *pexprNewPredicate = (*pexprRight)[1];
    pexprScalar->Release();
    pexprScalar = pexprNewPredicate;

    (*pexprRight)[0]->AddRef();
    CExpression *pexprChild = (*pexprRight)[0];
    pexprRight->Release();
    pexprRight = pexprChild;
  }

  return GPOS_NEW(mp)
      CExpression(mp, GPOS_NEW(mp) T(mp, pdrgpcrInner, eopidOriginSubq), pexprLeft, pexprRight, pexprScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::AddRefAppend
//
//	@doc:
//		Append elements from input array to output array, starting from
//		given index, after add-refing them
//
//---------------------------------------------------------------------------
template <class T, void (*CleanupFn)(T *)>
void CUtils::AddRefAppend(CDynamicPtrArray<T, CleanupFn> *pdrgptOutput, CDynamicPtrArray<T, CleanupFn> *pdrgptInput,
                          uint32_t ulStart) {
  GPOS_ASSERT(nullptr != pdrgptOutput);
  GPOS_ASSERT(nullptr != pdrgptInput);

  const uint32_t size = pdrgptInput->Size();
  GPOS_ASSERT_IMP(0 < size, ulStart < size);

  for (uint32_t ul = ulStart; ul < size; ul++) {
    T *pt = (*pdrgptInput)[ul];
    CRefCount *prc = dynamic_cast<CRefCount *>(pt);
    prc->AddRef();
    pdrgptOutput->Append(pt);
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::FScalarConstInt
//
//	@doc:
//		Check if the given expression is an int32_t,
//		the template parameter is an int32_t type
//
//---------------------------------------------------------------------------
template <class T>
bool CUtils::FScalarConstInt(CExpression *pexpr) {
  GPOS_ASSERT(nullptr != pexpr);

  IMDType::ETypeInfo type_info = T::GetTypeInfo();
  GPOS_ASSERT(IMDType::EtiInt2 == type_info || IMDType::EtiInt4 == type_info || IMDType::EtiInt8 == type_info);

  COperator *pop = pexpr->Pop();
  if (COperator::EopScalarConst == pop->Eopid()) {
    CScalarConst *popScalarConst = CScalarConst::PopConvert(pop);
    if (type_info == popScalarConst->GetDatum()->GetDatumType()) {
      return true;
    }
  }

  return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::FMatchIndex
//
//	@doc:
//		Match function between index get/scan operators
//
//---------------------------------------------------------------------------
template <class T>
bool CUtils::FMatchIndex(T *pop1, COperator *pop2) {
  if (pop1->Eopid() != pop2->Eopid()) {
    return false;
  }

  T *popIndex = T::PopConvert(pop2);

  return pop1->UlOriginOpId() == popIndex->UlOriginOpId() &&
         pop1->Ptabdesc()->MDId()->Equals(popIndex->Ptabdesc()->MDId()) &&
         pop1->Pindexdesc()->MDId()->Equals(popIndex->Pindexdesc()->MDId()) &&
         pop1->PdrgpcrOutput()->Equals(popIndex->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::FMatchDynamicIndex
//
//	@doc:
//		Match function between dynamic index get/scan operators
//
//---------------------------------------------------------------------------
template <class T>
bool CUtils::FMatchDynamicIndex(T *pop1, COperator *pop2) {
  if (pop1->Eopid() != pop2->Eopid()) {
    return false;
  }

  T *popIndex2 = T::PopConvert(pop2);

  // match if the index descriptors are identical
  // we will compare MDIds, so both indexes should be partial or non-partial.
  // Possible future improvement:
  // For heterogeneous indexes, we use pointer comparison of part constraints.
  // That was to avoid memory allocation because matching function was used while
  // holding spin locks. This is no longer an issue, as we don't use spin locks
  // anymore. Using a match function would mean improved matching for heterogeneous
  // indexes.
  return pop1->UlOriginOpId() == popIndex2->UlOriginOpId() && pop1->ScanId() == popIndex2->ScanId() &&
         pop1->Ptabdesc()->MDId()->Equals(popIndex2->Ptabdesc()->MDId()) &&
         pop1->Pindexdesc()->MDId()->Equals(popIndex2->Pindexdesc()->MDId()) &&
         pop1->PdrgpcrOutput()->Equals(popIndex2->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::FMatchDynamicScan
//
//	@doc:
//		Match function between dynamic get/scan operators
//
//---------------------------------------------------------------------------
template <class T>
bool CUtils::FMatchDynamicScan(T *pop1, COperator *pop2) {
  if (pop1->Eopid() != pop2->Eopid()) {
    return false;
  }

  T *popScan2 = T::PopConvert(pop2);

  // match if the table descriptors are identical
  return pop1->ScanId() == popScan2->ScanId() && pop1->Ptabdesc()->MDId()->Equals(popScan2->Ptabdesc()->MDId()) &&
         pop1->PdrgpcrOutput()->Equals(popScan2->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::FMatchDynamicBitmapScan
//
//	@doc:
//		Match function between dynamic bitmap get/scan operators
//
//---------------------------------------------------------------------------
template <class T>
bool CUtils::FMatchDynamicBitmapScan(T *pop1, COperator *pop2) {
  if (pop1->Eopid() != pop2->Eopid()) {
    return false;
  }

  T *popDynamicBitmapScan2 = T::PopConvert(pop2);

  return pop1->UlOriginOpId() == popDynamicBitmapScan2->UlOriginOpId() &&
         FMatchDynamicScan(pop1,
                           pop2);  // call match dynamic scan to compare other member vars
}

//---------------------------------------------------------------------------
//	@function:
//		CUtils::FMatchBitmapScan
//
//	@doc:
//		Match function between bitmap get/scan operators
//
//---------------------------------------------------------------------------
template <class T>
bool CUtils::FMatchBitmapScan(T *pop1, COperator *pop2) {
  if (pop1->Eopid() != pop2->Eopid()) {
    return false;
  }

  T *popScan2 = T::PopConvert(pop2);

  return pop1->UlOriginOpId() == popScan2->UlOriginOpId() &&
         pop1->Ptabdesc()->MDId()->Equals(popScan2->Ptabdesc()->MDId()) &&
         pop1->PdrgpcrOutput()->Equals(popScan2->PdrgpcrOutput());
}
}  // namespace gpopt

#ifdef GPOS_DEBUG

// helper to print given expression
// outside of namespace to make sure gdb can resolve the symbol easily
void PrintExpr(void *pv);

// helper to print memo structure
void PrintMemo(void *pv);

#endif  // GPOS_DEBUG

#endif  // !GPOPT_CUtils_H

// EOF
