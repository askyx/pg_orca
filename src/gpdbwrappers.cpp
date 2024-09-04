//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		gpdbwrappers.cpp
//
//	@doc:
//		Implementation of GPDB function wrappers. Note that we should never
// 		return directly from inside the PG_TRY() block, in order to restore
//		the long jump stack. That is why we save the return value of the GPDB
//		function to a local variable and return it after the PG_END_TRY().
//		./README file contains the sources (caches and catalog tables) of metadata
//		requested by the optimizer and retrieved using GPDB function wrappers. Any
//		change to optimizer's requested metadata should also be recorded in ./README file.
//
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/gpdbwrappers.h"

#include <limits>  // std::numeric_limits

#include "catalog/pg_collation.h"
#include "gpopt/utils/gpdbdefs.h"
#include "gpos/base.h"
#include "gpos/error/CAutoExceptionStack.h"
#include "gpos/error/CException.h"
#include "naucrates/exception.h"
extern "C" {
#include <postgres.h>

#include <access/amapi.h>
#include <access/genam.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_inherits.h>
#include <foreign/fdwapi.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/plancat.h>
#include <optimizer/subselect.h>
#include <parser/parse_agg.h>
#include <partitioning/partdesc.h>
#include <storage/lmgr.h>
#include <utils/fmgroids.h>
#include <utils/memutils.h>
#include <utils/partcache.h>
}

using namespace gpos;

bool gpdb::BoolFromDatum(Datum d) {
  { return DatumGetBool(d); }

  return false;
}

Datum gpdb::DatumFromBool(bool b) {
  { return BoolGetDatum(b); }

  return 0;
}

char gpdb::CharFromDatum(Datum d) {
  { return DatumGetChar(d); }

  return '\0';
}

Datum gpdb::DatumFromChar(char c) {
  { return CharGetDatum(c); }

  return 0;
}

int8 gpdb::Int8FromDatum(Datum d) {
  { return DatumGetUInt8(d); }

  return 0;
}

Datum gpdb::DatumFromInt8(int8 i8) {
  { return Int8GetDatum(i8); }

  return 0;
}

uint8 gpdb::Uint8FromDatum(Datum d) {
  { return DatumGetUInt8(d); }

  return 0;
}

Datum gpdb::DatumFromUint8(uint8 ui8) {
  { return UInt8GetDatum(ui8); }

  return 0;
}

int16 gpdb::Int16FromDatum(Datum d) {
  { return DatumGetInt16(d); }

  return 0;
}

Datum gpdb::DatumFromInt16(int16 i16) {
  { return Int16GetDatum(i16); }

  return 0;
}

uint16 gpdb::Uint16FromDatum(Datum d) {
  { return DatumGetUInt16(d); }

  return 0;
}

Datum gpdb::DatumFromUint16(uint16 ui16) {
  { return UInt16GetDatum(ui16); }

  return 0;
}

int32 gpdb::Int32FromDatum(Datum d) {
  { return DatumGetInt32(d); }

  return 0;
}

Datum gpdb::DatumFromInt32(int32 i32) {
  { return Int32GetDatum(i32); }

  return 0;
}

uint32 gpdb::lUint32FromDatum(Datum d) {
  { return DatumGetUInt32(d); }

  return 0;
}

Datum gpdb::DatumFromUint32(uint32 ui32) {
  { return UInt32GetDatum(ui32); }

  return 0;
}

int64 gpdb::Int64FromDatum(Datum d) {
  Datum d2 = d;

  { return DatumGetInt64(d2); }

  return 0;
}

Datum gpdb::DatumFromInt64(int64 i64) {
  int64 ii64 = i64;

  { return Int64GetDatum(ii64); }

  return 0;
}

uint64 gpdb::Uint64FromDatum(Datum d) {
  { return DatumGetUInt64(d); }

  return 0;
}

Datum gpdb::DatumFromUint64(uint64 ui64) {
  { return UInt64GetDatum(ui64); }

  return 0;
}

Oid gpdb::OidFromDatum(Datum d) {
  { return DatumGetObjectId(d); }

  return 0;
}

void *gpdb::PointerFromDatum(Datum d) {
  { return DatumGetPointer(d); }

  return nullptr;
}

float4 gpdb::Float4FromDatum(Datum d) {
  { return DatumGetFloat4(d); }

  return 0;
}

float8 gpdb::Float8FromDatum(Datum d) {
  { return DatumGetFloat8(d); }

  return 0;
}

Datum gpdb::DatumFromPointer(const void *p) {
  { return PointerGetDatum(p); }

  return 0;
}

bool gpdb::AggregateExists(Oid oid) {
  { return SearchSysCacheExists1(AGGFNOID, oid); }

  return false;
}

Bitmapset *gpdb::BmsAddMember(Bitmapset *a, int x) {
  { return bms_add_member(a, x); }

  return nullptr;
}

void *gpdb::CopyObject(void *from) {
  { return copyObjectImpl(from); }

  return nullptr;
}

Size gpdb::DatumSize(Datum value, bool type_by_val, int iTypLen) {
  { return datumGetSize(value, type_by_val, iTypLen); }

  return 0;
}

Node *gpdb::MutateExpressionTree(Node *node, Node *(*mutator)(Node *node, void *context), void *context) {
  { return expression_tree_mutator(node, mutator, context); }

  return nullptr;
}

bool gpdb::WalkExpressionTree(Node *node, bool (*walker)(Node *node, void *context), void *context) {
  { return expression_tree_walker(node, walker, context); }

  return false;
}

gpos::BOOL gpdb::WalkQueryTree(Query *query, bool (*walker)(Node *node, void *context), void *context, int flags) {
  { return query_tree_walker(query, walker, context, flags); }

  return false;
}

Oid gpdb::ExprType(Node *expr) {
  { return exprType(expr); }

  return 0;
}

int32 gpdb::ExprTypeMod(Node *expr) {
  { return exprTypmod(expr); }

  return 0;
}

Oid gpdb::ExprCollation(Node *expr) {
  {
    if (expr && IsA(expr, List)) {
      // GPDB_91_MERGE_FIXME: collation
      List *exprlist = (List *)expr;
      ListCell *lc;

      Oid collation = InvalidOid;
      foreach (lc, exprlist) {
        Node *expr = (Node *)lfirst(lc);
        if ((collation = exprCollation(expr)) != InvalidOid) {
          break;
        }
      }
      return collation;
    } else {
      return exprCollation(expr);
    }
  }

  return 0;
}

Oid gpdb::TypeCollation(Oid type) {
  {
    Oid collation = InvalidOid;
    Oid typcollation = get_typcollation(type);
    if (OidIsValid(typcollation)) {
      if (type == NAMEOID) {
        return typcollation;  // As of v12, this is C_COLLATION_OID
      }
      return DEFAULT_COLLATION_OID;
    }
    return collation;
  }

  return 0;
}

List *gpdb::ExtractNodesPlan(Plan *pl, int node_tag, bool descend_into_subqueries) {
  { return nullptr; }

  return NIL;
}

List *gpdb::ExtractNodesExpression(Node *node, int node_tag, bool descend_into_subqueries) {
  { return nullptr; }

  return NIL;
}

void gpdb::FreeAttrStatsSlot(AttStatsSlot *sslot) {
  {
    free_attstatsslot(sslot);
    return;
  }
}

bool gpdb::IsFuncAllowedForPartitionSelection(Oid funcid) {
  switch (funcid) {
      // These are the functions we have allowed as lossy casts for Partition selection.
      // For range partition selection, the logic in ORCA checks on bounds of the partition ranges.
      // Hence these must be increasing functions.
    case F_TIMESTAMP_DATE:  // date(timestamp) -> date
    case F_FLOAT4_NUMERIC:  // numeric(float4) -> numeric
    case F_FLOAT8_NUMERIC:  // numeric(float8) -> numeric
    case F_NUMERIC_INT8:    // int8(numeric) -> int8
    case F_NUMERIC_INT2:    // int2(numeric) -> int2
    case F_NUMERIC_INT4:    // int4(numeric) -> int4
      return true;
    default:
      return false;
  }
}

bool gpdb::FuncStrict(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return func_strict(funcid);
  }

  return false;
}

bool gpdb::IsFuncNDVPreserving(Oid funcid) {
  // Given a function oid, return whether it's one of a list of NDV-preserving
  // functions (estimated NDV of output is similar to that of the input)

  return false;
}

char gpdb::FuncStability(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return func_volatile(funcid);
  }

  return '\0';
}

char gpdb::FuncExecLocation(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return '\0';
  }

  return '\0';
}

bool gpdb::FunctionExists(Oid oid) {
  {
    /* catalog tables: pg_proc */
    return false;
  }

  return false;
}

Oid gpdb::GetAggIntermediateResultType(Oid aggid) {
  HeapTuple tp;
  Oid result;

  tp = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggid));
  if (!HeapTupleIsValid(tp))
    elog(ERROR, "cache lookup failed for aggregate %u", aggid);

  result = ((Form_pg_aggregate)GETSTRUCT(tp))->aggtranstype;
  ReleaseSysCache(tp);
  return result;
}

int gpdb::GetAggregateArgTypes(Aggref *aggref, Oid *inputTypes) {
  { return get_aggregate_argtypes(aggref, inputTypes); }

  return 0;
}

Oid gpdb::ResolveAggregateTransType(Oid aggfnoid, Oid aggtranstype, Oid *inputTypes, int numArguments) {
  { return resolve_aggregate_transtype(aggfnoid, aggtranstype, inputTypes, numArguments); }

  return 0;
}

Query *gpdb::FlattenJoinAliasVar(Query *query, gpos::ULONG query_level) {
  { return nullptr; }

  return nullptr;
}

bool gpdb::IsOrderedAgg(Oid aggid) {
  {
    /* catalog tables: pg_aggregate */
    return false;
  }

  return false;
}

bool gpdb::IsRepSafeAgg(Oid aggid) {
  {
    /* catalog tables: pg_aggregate */
    return false;
  }

  return false;
}

bool gpdb::IsAggPartialCapable(Oid aggid) {
  {
    /* catalog tables: pg_aggregate */
    return false;
  }

  return false;
}

Oid gpdb::GetAggregate(const char *agg, Oid type_oid) {
  {
    /* catalog tables: pg_aggregate */
    return type_oid;
  }

  return 0;
}

Oid gpdb::GetArrayType(Oid typid) {
  {
    /* catalog tables: pg_type */
    return get_array_type(typid);
  }

  return 0;
}

bool gpdb::GetAttrStatsSlot(AttStatsSlot *sslot, HeapTuple statstuple, int reqkind, Oid reqop, int flags) {
  { return get_attstatsslot(sslot, statstuple, reqkind, reqop, flags); }

  return false;
}

HeapTuple gpdb::GetAttStats(Oid relid, AttrNumber attnum) {
  {
    /* catalog tables: pg_statistic */
    return nullptr;
  }

  return nullptr;
}

List *gpdb::GetExtStats(Relation rel) {
  {
    /* catalog tables: pg_statistic_ext */
    return nullptr;
  }

  return nullptr;
}

char *gpdb::GetExtStatsName(Oid statOid) {
  { return nullptr; }

  return nullptr;
}

List *gpdb::GetExtStatsKinds(Oid statOid) {
  { return nullptr; }

  return nullptr;
}

Oid gpdb::GetCommutatorOp(Oid opno) {
  {
    /* catalog tables: pg_operator */
    return get_commutator(opno);
  }

  return 0;
}

char *gpdb::GetCheckConstraintName(Oid check_constraint_oid) {
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }

  return nullptr;
}

Oid gpdb::GetCheckConstraintRelid(Oid check_constraint_oid) {
  {
    /* catalog tables: pg_constraint */
    return (check_constraint_oid);
  }

  return 0;
}

Node *gpdb::PnodeCheckConstraint(Oid check_constraint_oid) {
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }

  return nullptr;
}

List *gpdb::GetCheckConstraintOids(Oid rel_oid) {
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }

  return nullptr;
}

Node *gpdb::GetRelationPartConstraints(Relation rel) {
  {
    /* catalog tables: pg_partition, pg_partition_rule, pg_constraint */
    List *part_quals = RelationGetPartitionQual(rel);
    if (part_quals) {
      return (Node *)make_ands_explicit(part_quals);
    }
  }

  return nullptr;
}

bool gpdb::GetCastFunc(Oid src_oid, Oid dest_oid, bool *is_binary_coercible, Oid *cast_fn_oid,
                       CoercionPathType *pathtype) {
  if (IsBinaryCoercible(src_oid, dest_oid)) {
    *is_binary_coercible = true;
    *cast_fn_oid = 0;
    return true;
  }

  *is_binary_coercible = false;

  *pathtype = find_coercion_pathway(dest_oid, src_oid, COERCION_IMPLICIT, cast_fn_oid);
  if (*pathtype == COERCION_PATH_RELABELTYPE)
    *is_binary_coercible = true;
  if (*pathtype != COERCION_PATH_NONE)
    return true;
  return false;
}

unsigned int gpdb::GetComparisonType(Oid op_oid) {
  {
    /* catalog tables: pg_amop */
    return (op_oid);
  }

  return InvalidOid;
}

Oid gpdb::GetComparisonOperator(Oid left_oid, Oid right_oid, unsigned int cmpt) {
  {
#ifdef FAULT_INJECTOR
    SIMPLE_FAULT_INJECTOR("gpdbwrappers_get_comparison_operator");
#endif
    /* catalog tables: pg_amop */
    return right_oid;
  }

  return InvalidOid;
}

Oid gpdb::GetEqualityOp(Oid type_oid) {
  {
    /* catalog tables: pg_type */
    Oid eq_opr;

    get_sort_group_operators(type_oid, false, true, false, nullptr, &eq_opr, nullptr, nullptr);

    return eq_opr;
  }

  return InvalidOid;
}

Oid gpdb::GetEqualityOpForOrderingOp(Oid opno, bool *reverse) {
  {
    /* catalog tables: pg_amop */
    return get_equality_op_for_ordering_op(opno, reverse);
  }

  return InvalidOid;
}

Oid gpdb::GetOrderingOpForEqualityOp(Oid opno, bool *reverse) {
  {
    /* catalog tables: pg_amop */
    return get_ordering_op_for_equality_op(opno, reverse);
  }

  return InvalidOid;
}

char *gpdb::GetFuncName(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return get_func_name(funcid);
  }

  return nullptr;
}

List *gpdb::GetFuncOutputArgTypes(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return nullptr;
  }

  return NIL;
}

List *gpdb::GetFuncArgTypes(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return nullptr;
  }

  return NIL;
}

bool gpdb::GetFuncRetset(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return get_func_retset(funcid);
  }

  return false;
}

Oid gpdb::GetFuncRetType(Oid funcid) {
  {
    /* catalog tables: pg_proc */
    return get_func_rettype(funcid);
  }

  return 0;
}

Oid gpdb::GetInverseOp(Oid opno) {
  {
    /* catalog tables: pg_operator */
    return get_negator(opno);
  }

  return 0;
}

RegProcedure gpdb::GetOpFunc(Oid opno) {
  {
    /* catalog tables: pg_operator */
    return get_opcode(opno);
  }

  return 0;
}

char *gpdb::GetOpName(Oid opno) {
  {
    /* catalog tables: pg_operator */
    return get_opname(opno);
  }

  return nullptr;
}

List *gpdb::GetRelationKeys(Oid relid) {
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }

  return NIL;
}

Oid gpdb::GetTypeRelid(Oid typid) {
  {
    /* catalog tables: pg_type */
    return get_typ_typrelid(typid);
  }

  return 0;
}

char *gpdb::GetTypeName(Oid typid) {
  HeapTuple tp;

  tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
  if (HeapTupleIsValid(tp)) {
    Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
    char *result;

    result = pstrdup(NameStr(typtup->typname));
    ReleaseSysCache(tp);
    return result;
  } else
    return NULL;
}

int gpdb::GetGPSegmentCount(void) {
  return 0;
}

bool gpdb::HeapAttIsNull(HeapTuple tup, int attno) {
  { return heap_attisnull(tup, attno, nullptr); }

  return false;
}

void gpdb::FreeHeapTuple(HeapTuple htup) {
  {
    heap_freetuple(htup);
    return;
  }
}

Oid gpdb::GetDefaultDistributionOpclassForType(Oid typid) {
  {
    /* catalog tables: pg_type, pg_opclass */
    return (typid);
  }

  return false;
}

Oid gpdb::GetColumnDefOpclassForType(List *opclassName, Oid typid) {
  {
    /* catalog tables: pg_type, pg_opclass */
    return typid;
  }

  return false;
}

Oid gpdb::GetDefaultDistributionOpfamilyForType(Oid typid) {
  {
    /* catalog tables: pg_type, pg_opclass */
    return (typid);
  }

  return false;
}

Oid gpdb::GetDefaultPartitionOpfamilyForType(Oid typid) {
  TypeCacheEntry *tcache;

  // flags required for or applicable to btree opfamily
  // required: TYPECACHE_CMP_PROC, TYPECACHE_CMP_PROC_FINFO, TYPECACHE_BTREE_OPFAMILY
  // applicable: TYPECACHE_EQ_OPR, TYPECACHE_LT_OPR, TYPECACHE_GT_OPR, TYPECACHE_EQ_OPR_FINFO
  // Note we don't need all the flags to obtain the btree opfamily
  // But applying all the flags allows us to abstract away the lookup_type_cache call
  tcache = lookup_type_cache(typid, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR | TYPECACHE_CMP_PROC |
                                        TYPECACHE_EQ_OPR_FINFO | TYPECACHE_CMP_PROC_FINFO | TYPECACHE_BTREE_OPFAMILY);

  if (!tcache->btree_opf)
    return InvalidOid;
  if (!tcache->cmp_proc)
    return InvalidOid;
  if (!tcache->eq_opr && !tcache->lt_opr && !tcache->gt_opr)
    return InvalidOid;

  return tcache->btree_opf;
}

Oid gpdb::GetHashProcInOpfamily(Oid opfamily, Oid typid) {
  {
    /* catalog tables: pg_amproc, pg_type, pg_opclass */
    return typid;
  }

  return false;
}

Oid gpdb::IsLegacyCdbHashFunction(Oid funcid) {
  { return (funcid); }

  return false;
}

Oid gpdb::GetLegacyCdbHashOpclassForBaseType(Oid typid) {
  { return (typid); }

  return false;
}

Oid gpdb::GetOpclassFamily(Oid opclass) {
  { return get_opclass_family(opclass); }

  return false;
}

List *gpdb::LAppend(List *list, void *datum) {
  { return lappend(list, datum); }

  return NIL;
}

List *gpdb::LAppendInt(List *list, int iDatum) {
  { return lappend_int(list, iDatum); }

  return NIL;
}

List *gpdb::LAppendOid(List *list, Oid datum) {
  { return lappend_oid(list, datum); }

  return NIL;
}

List *gpdb::LPrepend(void *datum, List *list) {
  { return lcons(datum, list); }

  return NIL;
}

List *gpdb::LPrependInt(int datum, List *list) {
  { return lcons_int(datum, list); }

  return NIL;
}

List *gpdb::LPrependOid(Oid datum, List *list) {
  { return lcons_oid(datum, list); }

  return NIL;
}

List *gpdb::ListConcat(List *list1, List *list2) {
  { return list_concat(list1, list2); }

  return NIL;
}

List *gpdb::ListCopy(List *list) {
  { return list_copy(list); }

  return NIL;
}

ListCell *gpdb::ListHead(List *l) {
  { return list_head(l); }

  return nullptr;
}

ListCell *gpdb::ListTail(List *l) {
  { return list_tail(l); }

  return nullptr;
}

uint32 gpdb::ListLength(List *l) {
  { return list_length(l); }

  return 0;
}

void *gpdb::ListNth(List *list, int n) {
  { return list_nth(list, n); }

  return nullptr;
}

int gpdb::ListNthInt(List *list, int n) {
  { return list_nth_int(list, n); }

  return 0;
}

Oid gpdb::ListNthOid(List *list, int n) {
  { return list_nth_oid(list, n); }

  return 0;
}

bool gpdb::ListMemberOid(List *list, Oid oid) {
  { return list_member_oid(list, oid); }

  return false;
}

void gpdb::ListFree(List *list) {
  {
    list_free(list);
    return;
  }
}

void gpdb::ListFreeDeep(List *list) {
  {
    list_free_deep(list);
    return;
  }
}

TypeCacheEntry *gpdb::LookupTypeCache(Oid type_id, int flags) {
  {
    /* catalog tables: pg_type, pg_operator, pg_opclass, pg_opfamily, pg_amop */
    return lookup_type_cache(type_id, flags);
  }

  return nullptr;
}

Node *gpdb::MakeStringValue(char *str) {
  { return (Node *)makeString(str); }

  return nullptr;
}

Node *gpdb::MakeIntegerValue(long i) {
  { return (Node *)makeInteger(i); }

  return nullptr;
}

Node *gpdb::MakeIntConst(int32 intValue) {
  { return (Node *)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(intValue), false, true); }
}

Node *gpdb::MakeBoolConst(bool value, bool isnull) {
  { return makeBoolConst(value, isnull); }

  return nullptr;
}

Node *gpdb::MakeNULLConst(Oid type_oid) {
  { return (Node *)makeNullConst(type_oid, -1 /*consttypmod*/, InvalidOid); }

  return nullptr;
}

Node *gpdb::MakeSegmentFilterExpr(int segid) {
  {
    return nullptr;
    ;
  }
}

TargetEntry *gpdb::MakeTargetEntry(Expr *expr, AttrNumber resno, char *resname, bool resjunk) {
  { return makeTargetEntry(expr, resno, resname, resjunk); }

  return nullptr;
}

Var *gpdb::MakeVar(Index varno, AttrNumber varattno, Oid vartype, int32 vartypmod, Index varlevelsup) {
  {
    // GPDB_91_MERGE_FIXME: collation
    Oid collation = TypeCollation(vartype);
    return makeVar(varno, varattno, vartype, vartypmod, collation, varlevelsup);
  }

  return nullptr;
}

void *gpdb::MemCtxtAllocZeroAligned(MemoryContext context, Size size) {
  { return nullptr; }

  return nullptr;
}

void *gpdb::MemCtxtAllocZero(MemoryContext context, Size size) {
  { return MemoryContextAllocZero(context, size); }

  return nullptr;
}

void *gpdb::MemCtxtRealloc(void *pointer, Size size) {
  { return repalloc(pointer, size); }

  return nullptr;
}

char *gpdb::MemCtxtStrdup(MemoryContext context, const char *string) {
  { return MemoryContextStrdup(context, string); }

  return nullptr;
}

// Helper function to throw an error with errcode, message and hint, like you
// would with ereport(...) in the backend. This could be extended for other
// fields, but this is all we need at the moment.
void gpdb::GpdbEreportImpl(int xerrcode, int severitylevel, const char *xerrmsg, const char *xerrhint,
                           const char *filename, int lineno, const char *funcname) {
  {
    // We cannot use the ereport() macro here, because we want to pass on
    // the caller's filename and line number. This is essentially an
    // expanded version of ereport(). It will be caught by the
    // GP_WRAP_END, and propagated up as a C++ exception, to be
    // re-thrown as a Postgres error once we leave the C++ land.
    if (errstart(severitylevel, TEXTDOMAIN)) {
      errcode(xerrcode);
      errmsg("%s", xerrmsg);
      if (xerrhint) {
        errhint("%s", xerrhint);
      }
      errfinish(filename, lineno, funcname);
    }
  }
}

char *gpdb::NodeToString(void *obj) {
  { return nodeToString(obj); }

  return nullptr;
}

Node *gpdb::GetTypeDefault(Oid typid) {
  {
    /* catalog tables: pg_type */
    return get_typdefault(typid);
  }

  return nullptr;
}

double gpdb::NumericToDoubleNoOverflow(Numeric num) {
  { return 0; }

  return 0.0;
}

bool gpdb::NumericIsNan(Numeric num) {
  { return numeric_is_nan(num); }

  return false;
}

double gpdb::ConvertTimeValueToScalar(Datum datum, Oid typid) {
  { return 0; }

  return 0.0;
}

double gpdb::ConvertNetworkToScalar(Datum datum, Oid typid) {
  bool failure = false;

  { return convert_network_to_scalar(datum, typid, &failure); }

  return 0.0;
}

bool gpdb::IsOpHashJoinable(Oid opno, Oid inputtype) {
  {
    /* catalog tables: pg_operator */
    return op_hashjoinable(opno, inputtype);
  }

  return false;
}

bool gpdb::IsOpMergeJoinable(Oid opno, Oid inputtype) {
  {
    /* catalog tables: pg_operator */
    return op_mergejoinable(opno, inputtype);
  }

  return false;
}

bool gpdb::IsOpStrict(Oid opno) {
  {
    /* catalog tables: pg_operator, pg_proc */
    return op_strict(opno);
  }

  return false;
}

bool gpdb::IsOpNDVPreserving(Oid opno) {
  return false;
}

void gpdb::GetOpInputTypes(Oid opno, Oid *lefttype, Oid *righttype) {
  {
    /* catalog tables: pg_operator */
    op_input_types(opno, lefttype, righttype);
    return;
  }
}

void *gpdb::GPDBAlloc(Size size) {
  { return palloc(size); }

  return nullptr;
}

void gpdb::GPDBFree(void *ptr) {
  {
    pfree(ptr);
    return;
  }
}

bool gpdb::WalkQueryOrExpressionTree(Node *node, bool (*walker)(Node *node, void *context), void *context, int flags) {
  { return query_or_expression_tree_walker(node, walker, context, flags); }

  return false;
}

Node *gpdb::MutateQueryOrExpressionTree(Node *node, Node *(*mutator)(Node *node, void *context), void *context,
                                        int flags) {
  { return query_or_expression_tree_mutator(node, mutator, context, flags); }

  return nullptr;
}

Query *gpdb::MutateQueryTree(Query *query, Node *(*mutator)(Node *node, void *context), void *context, int flags) {
  { return query_tree_mutator(query, mutator, context, flags); }

  return nullptr;
}

bool gpdb::HasSubclassSlow(Oid rel_oid) {
  {
    /* catalog tables: pg_inherits */
    return false;
  }

  return false;
}

gpos::BOOL gpdb::IsChildPartDistributionMismatched(Relation rel) {
  {
    /* catalog tables: pg_class, pg_inherits */
    return false;
  }

  return false;
}

double gpdb::CdbEstimatePartitionedNumTuples(Relation rel) {
  { return 0; }
}

void gpdb::CloseRelation(Relation rel) {
  {
    RelationClose(rel);
    return;
  }
}

List *gpdb::GetRelationIndexes(Relation relation) {
  {
    if (relation->rd_rel->relhasindex) {
      /* catalog tables: from relcache */
      return RelationGetIndexList(relation);
    }
  }

  return NIL;
}

MVNDistinct *gpdb::GetMVNDistinct(Oid stat_oid) {
  { return nullptr; }
}

MVDependencies *gpdb::GetMVDependencies(Oid stat_oid) {
  { return nullptr; }
}

gpdb::RelationWrapper gpdb::GetRelation(Oid rel_oid) {
  {
    /* catalog tables: relcache */
    return RelationWrapper{RelationIdGetRelation(rel_oid)};
  }
}

ForeignScan *gpdb::CreateForeignScan(Oid rel_oid, Index scanrelid, List *qual, List *targetlist, Query *query,
                                     RangeTblEntry *rte) {
  { return nullptr; }

  return nullptr;
}

TargetEntry *gpdb::FindFirstMatchingMemberInTargetList(Node *node, List *targetlist) {
  { return tlist_member((Expr *)node, targetlist); }

  return nullptr;
}

List *gpdb::FindMatchingMembersInTargetList(Node *node, List *targetlist) {
  List *tlist = NIL;
  ListCell *temp = NULL;

  foreach (temp, targetlist) {
    TargetEntry *tlentry = (TargetEntry *)lfirst(temp);

    Assert(IsA(tlentry, TargetEntry));

    if (equal(node, tlentry->expr)) {
      tlist = lappend(tlist, tlentry);
    }
  }

  return tlist;
}

bool gpdb::Equals(void *p1, void *p2) {
  { return equal(p1, p2); }

  return false;
}

bool gpdb::IsCompositeType(Oid typid) {
  {
    /* catalog tables: pg_type */
    return type_is_rowtype(typid);
  }

  return false;
}

bool gpdb::IsTextRelatedType(Oid typid) {
  {
    /* catalog tables: pg_type */
    char typcategory;
    bool typispreferred;
    get_type_category_preferred(typid, &typcategory, &typispreferred);

    return typcategory == TYPCATEGORY_STRING;
  }

  return false;
}

StringInfo gpdb::MakeStringInfo(void) {
  { return makeStringInfo(); }

  return nullptr;
}

void gpdb::AppendStringInfo(StringInfo str, const char *str1, const char *str2) {
  {
    appendStringInfo(str, "%s%s", str1, str2);
    return;
  }
}

int gpdb::FindNodes(Node *node, List *nodeTags) {
  { return 0; }

  return -1;
}

int gpdb::CheckCollation(Node *node) {
  { return 0; }

  return -1;
}

Node *gpdb::CoerceToCommonType(ParseState *pstate, Node *node, Oid target_type, const char *context) {
  {
    /* catalog tables: pg_type, pg_cast */
    return coerce_to_common_type(pstate, node, target_type, context);
  }

  return nullptr;
}

bool gpdb::ResolvePolymorphicArgType(int numargs, Oid *argtypes, char *argmodes, FuncExpr *call_expr) {
  {
    /* catalog tables: pg_proc */
    return resolve_polymorphic_argtypes(numargs, argtypes, argmodes, (Node *)call_expr);
  }

  return false;
}

// hash a list of const values with GPDB's hash function
int32 gpdb::CdbHashConstList(List *constants, int num_segments, Oid *hashfuncs) {
  { return num_segments; }

  return 0;
}

unsigned int gpdb::CdbHashRandomSeg(int num_segments) {
  { return num_segments; }

  return 0;
}

// check permissions on range table
void gpdb::CheckRTPermissions(List *rtable) {
  { return; }
}

// check that a table doesn't have UPDATE triggers.
bool gpdb::HasUpdateTriggers(Oid relid) {
  { return (false); }

  return false;
}

// get index op family properties
void gpdb::IndexOpProperties(Oid opno, Oid opfamily, StrategyNumber *strategynumber, Oid *righttype) {
  {
    /* catalog tables: pg_amop */

    // Only the right type is returned to the caller, the left
    // type is simply ignored.
    Oid lefttype;
    INT strategy;

    get_op_opfamily_properties(opno, opfamily, false, &strategy, &lefttype, righttype);

    // Ensure the value of strategy doesn't get truncated when converted to StrategyNumber
    GPOS_ASSERT(strategy >= 0 && strategy <= std::numeric_limits<StrategyNumber>::max());
    *strategynumber = static_cast<StrategyNumber>(strategy);
    return;
  }
}

// check whether index column is returnable (for index-only scans)
gpos::BOOL gpdb::IndexCanReturn(Relation index, int attno) {
  { return index_can_return(index, attno); }
}

// get oids of opfamilies for the index keys
List *gpdb::GetIndexOpFamilies(Oid index_oid) {
  {
    /* catalog tables: pg_index */

    // We return the operator families of the index keys.
    return (nullptr);
  }

  return NIL;
}

// get oids of families this operator belongs to
List *gpdb::GetOpFamiliesForScOp(Oid opno) {
  {
    /* catalog tables: pg_amop */

    // We return the operator families this operator
    // belongs to.
    return (nullptr);
  }

  return NIL;
}

// get the OID of hash equality operator(s) compatible with the given op
Oid gpdb::GetCompatibleHashOpFamily(Oid opno) {
  { return opno; }

  return InvalidOid;
}

// get the OID of hash equality operator(s) compatible with the given op
Oid gpdb::GetCompatibleLegacyHashOpFamily(Oid opno) {
  { return opno; }

  return InvalidOid;
}

List *gpdb::GetMergeJoinOpFamilies(Oid opno) {
  {
    /* catalog tables: pg_amop */

    return get_mergejoin_opfamilies(opno);
  }

  return NIL;
}

// get the OID of base elementtype for a given typid
// eg.: CREATE DOMAIN text_domain as text;
// SELECT oid, typbasetype from pg_type where typname = 'text_domain';
// oid         | XXXXX  --> Oid for text_domain
// typbasetype | 25     --> Oid for base element ie, TEXT
Oid gpdb::GetBaseType(Oid typid) {
  { return getBaseType(typid); }

  return InvalidOid;
}

// Evaluates 'expr' and returns the result as an Expr.
// Caller keeps ownership of 'expr' and takes ownership of the result
Expr *gpdb::EvaluateExpr(Expr *expr, Oid result_type, int32 typmod) {
  {
    // GPDB_91_MERGE_FIXME: collation
    return evaluate_expr(expr, result_type, typmod, InvalidOid);
  }

  return nullptr;
}

char *gpdb::DefGetString(DefElem *defelem) {
  { return defGetString(defelem); }

  return nullptr;
}

Expr *gpdb::TransformArrayConstToArrayExpr(Const *c) {
  { return nullptr; }

  return nullptr;
}

Node *gpdb::EvalConstExpressions(Node *node) {
  { return eval_const_expressions(nullptr, node); }

  return nullptr;
}

#ifdef FAULT_INJECTOR
FaultInjectorType_e gpdb::InjectFaultInOptTasks(const char *fault_name) {
  { return FaultInjector_InjectFaultIfSet(fault_name, DDLNotSpecified, "", ""); }

  return FaultInjectorTypeNotSpecified;
}
#endif

/*
 * To detect changes to catalog tables that require resetting the Metadata
 * Cache, we use the normal PostgreSQL catalog cache invalidation mechanism.
 * We register a callback to a cache on all the catalog tables that contain
 * information that's contained in the ORCA metadata cache.

 * There is no fine-grained mechanism in the metadata cache for invalidating
 * individual entries ATM, so we just blow the whole cache whenever anything
 * changes. The callback simply increments a counter. Whenever we start
 * planning a query, we check the counter to see if it has changed since the
 * last planned query, and reset the whole cache if it has.
 *
 * To make sure we've covered all catalog tables that contain information
 * that's stored in the metadata cache, there are "catalog tables: xxx"
 * comments in all the calls to backend functions in this file. They indicate
 * which catalog tables each function uses. We conservatively assume that
 * anything fetched via the wrapper functions in this file can end up in the
 * metadata cache and hence need to have an invalidation callback registered.
 */
static bool mdcache_invalidation_counter_registered = false;
static int64 mdcache_invalidation_counter = 0;
static int64 last_mdcache_invalidation_counter = 0;

static void mdsyscache_invalidation_counter_callback(Datum arg, int cacheid, uint32 hashvalue) {
  mdcache_invalidation_counter++;
}

static void mdrelcache_invalidation_counter_callback(Datum arg, Oid relid) {
  mdcache_invalidation_counter++;
}

static void register_mdcache_invalidation_callbacks(void) {
  /* These are all the catalog tables that we care about. */
  int metadata_caches[] = {
      AGGFNOID,         /* pg_aggregate */
      AMOPOPID,         /* pg_amop */
      CASTSOURCETARGET, /* pg_cast */
      CONSTROID,        /* pg_constraint */
      OPEROID,          /* pg_operator */
      OPFAMILYOID,      /* pg_opfamily */
      STATRELATTINH,    /* pg_statistics */
      TYPEOID,          /* pg_type */
      PROCOID,          /* pg_proc */

      /*
       * lookup_type_cache() will also access pg_opclass, via GetDefaultOpClass(),
       * but there is no syscache for it. Postgres doesn't seem to worry about
       * invalidating the type cache on updates to pg_opclass, so we don't
       * worry about that either.
       */
      /* pg_opclass */

      /*
       * Information from the following catalogs are included in the
       * relcache, and any updates will generate relcache invalidation
       * event. We'll catch the relcache invalidation event and don't need
       * to register a catcache callback for them.
       */
      /* pg_class */
      /* pg_index */

      /*
       * pg_foreign_table is updated when a new external table is dropped/created,
       * which will trigger a relcache invalidation event.
       */
      /* pg_foreign_table */

      /*
       * XXX: no syscache on pg_inherits. Is that OK? For any partitioning
       * changes, I think there will also be updates on pg_partition and/or
       * pg_partition_rules.
       */
      /* pg_inherits */

      /*
       * We assume that gp_segment_config will not change on the fly in a way that
       * would affect ORCA
       */
      /* gp_segment_config */
  };
  unsigned int i;

  for (i = 0; i < lengthof(metadata_caches); i++) {
    CacheRegisterSyscacheCallback(metadata_caches[i], &mdsyscache_invalidation_counter_callback, (Datum)0);
  }

  /* also register the relcache callback */
  CacheRegisterRelcacheCallback(&mdrelcache_invalidation_counter_callback, (Datum)0);
}

// Has there been any catalog changes since last call?
bool gpdb::MDCacheNeedsReset(void) {
  {
    if (!mdcache_invalidation_counter_registered) {
      register_mdcache_invalidation_callbacks();
      mdcache_invalidation_counter_registered = true;
    }
    if (last_mdcache_invalidation_counter == mdcache_invalidation_counter) {
      return false;
    } else {
      last_mdcache_invalidation_counter = mdcache_invalidation_counter;
      return true;
    }
  }

  return true;
}

// returns true if a query cancel is requested in GPDB
bool gpdb::IsAbortRequested(void) {
  // No GP_WRAP_START/END needed here. We just check these global flags,
  // it cannot throw an ereport().
  return (false);
}

// Given the type OID, get the typelem (InvalidOid if not an array type).
Oid gpdb::GetElementType(Oid array_type_oid) {
  { return get_element_type(array_type_oid); }
}

uint32 gpdb::HashChar(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1(hashchar, d)); }
}

uint32 gpdb::HashBpChar(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1Coll(hashbpchar, C_COLLATION_OID, d)); }
}

uint32 gpdb::HashText(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1Coll(hashtext, C_COLLATION_OID, d)); }
}

uint32 gpdb::HashName(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1(hashname, d)); }
}

uint32 gpdb::UUIDHash(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1(uuid_hash, d)); }
}

void *gpdb::GPDBMemoryContextAlloc(MemoryContext context, Size size) {
  { return MemoryContextAlloc(context, size); }

  return nullptr;
}

void gpdb::GPDBMemoryContextDelete(MemoryContext context) {
  { MemoryContextDelete(context); }
}

MemoryContext gpdb::GPDBAllocSetContextCreate() {
  { return nullptr; }

  return nullptr;
}

bool gpdb::ExpressionReturnsSet(Node *clause) {
  { return expression_returns_set(clause); }
}

List *gpdb::GetRelChildIndexes(Oid reloid) {
  List *partoids = NIL;

  {
    if (InvalidOid == reloid) {
      return NIL;
    }
    partoids = find_inheritance_children(reloid, NoLock);
  }

  return partoids;
}

Oid gpdb::GetForeignServerId(Oid reloid) {
  { return GetForeignServerIdByRelId(reloid); }

  return 0;
}

// Locks on partition leafs and indexes are held during optimizer (after
// parse-analyze stage). ORCA need this function to lock relation. Here
// we do not need to consider lock-upgrade issue, reasons are:
//   1. Only UPDATE|DELETE statement may upgrade lock level
//   2. ORCA currently does not support DML on partition tables
//   3. If not partition table, then parser should have already locked
//   4. Even later ORCA support DML on partition tables, the lock mode
//      of leafs should be the same as the mode in root's RTE's rellockmode
//   5. Index does not have lock-upgrade problem.
void gpdb::GPDBLockRelationOid(Oid reloid, LOCKMODE lockmode) {
  { LockRelationOid(reloid, lockmode); }
}

char *gpdb::GetRelFdwName(Oid reloid) {
  { return nullptr; }

  return nullptr;
}

PathTarget *gpdb::MakePathtargetFromTlist(List *tlist) {
  { return make_pathtarget_from_tlist(tlist); }
}

void gpdb::SplitPathtargetAtSrfs(PlannerInfo *root, PathTarget *target, PathTarget *input_target, List **targets,
                                 List **targets_contain_srfs) {
  { split_pathtarget_at_srfs(root, target, input_target, targets, targets_contain_srfs); }
}

List *gpdb::MakeTlistFromPathtarget(PathTarget *target) {
  { return make_tlist_from_pathtarget(target); }

  return NIL;
}

Node *gpdb::Expression_tree_mutator(Node *node, Node *(*mutator)(Node *node, void *context), void *context) {
  { return expression_tree_mutator(node, mutator, context); }

  return nullptr;
}

TargetEntry *gpdb::TlistMember(Expr *node, List *targetlist) {
  { return tlist_member(node, targetlist); }

  return nullptr;
}

Var *gpdb::MakeVarFromTargetEntry(Index varno, TargetEntry *tle) {
  { return makeVarFromTargetEntry(varno, tle); }
}

TargetEntry *gpdb::FlatCopyTargetEntry(TargetEntry *src_tle) {
  { return flatCopyTargetEntry(src_tle); }
}

// Returns true if type is a RANGE
// pg_type (typtype = 'r')
bool gpdb::IsTypeRange(Oid typid) {
  { return type_is_range(typid); }

  return false;
}

char *gpdb::GetRelAmName(Oid reloid) {
  { return nullptr; }

  return nullptr;
}

// Get IndexAmRoutine struct for the given access method handler.
IndexAmRoutine *gpdb::GetIndexAmRoutineFromAmHandler(Oid am_handler) {
  { return GetIndexAmRoutine(am_handler); }
}

PartitionDesc gpdb::GPDBRelationRetrievePartitionDesc(Relation rel) {
  return RelationGetPartitionDesc(rel, true);
}

PartitionKey gpdb::GPDBRelationRetrievePartitionKey(Relation rel) {
  return RelationGetPartitionKey(rel);
}

bool gpdb::TestexprIsHashable(Node *testexpr, List *param_ids) {
  { return false; }

  return false;
}

// EOF
