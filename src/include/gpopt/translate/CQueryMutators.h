//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CQueryMutators.h
//
//	@doc:
//		Class providing methods for translating a GPDB Query object into a
//		DXL Tree
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CWalkerUtils_H
#define GPDXL_CWalkerUtils_H

#include "gpopt/translate/CMappingVarColId.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/md/IMDType.h"

// fwd declarations
namespace gpopt {
class CMDAccessor;
}

struct Query;
struct RangeTblEntry;
struct Const;
struct List;

namespace gpdxl {
//---------------------------------------------------------------------------
//	@class:
//		CQueryMutators
//
//	@doc:
//		Class providing methods for translating a GPDB Query object into a
//      DXL Tree.
//
//---------------------------------------------------------------------------
class CQueryMutators {
  using MutatorWalkerFn = Node *(*)(Node *node, void *context);
  using ExprWalkerFn = bool (*)(Node *node, void *context);

  using CContextGrpbyPlMutator = struct SContextGrpbyPlMutator {
   public:
    // memory pool
    CMemoryPool *m_mp;

    // MD accessor to get the function name
    CMDAccessor *m_mda;

    // original query
    Query *m_query;

    // the new target list of the group by or window (lower) query
    List *m_lower_table_tlist;

    // the current query level
    uint32_t m_current_query_level;

    // indicate the levels up of the aggregate we are mutating
    uint32_t m_agg_levels_up;

    // indicate whether we are mutating the argument of an aggregate
    bool m_is_mutating_agg_arg;

    // ctor
    SContextGrpbyPlMutator(CMemoryPool *mp, CMDAccessor *mda, Query *query, List *derived_table_tlist)
        : m_mp(mp),
          m_mda(mda),
          m_query(query),
          m_lower_table_tlist(derived_table_tlist),
          m_current_query_level(0),
          m_agg_levels_up(UINT32_MAX),
          m_is_mutating_agg_arg(false) {}

    // dtor
    ~SContextGrpbyPlMutator() = default;
  };

  using CContextIncLevelsupMutator = struct SContextIncLevelsupMutator {
   public:
    // the current query level
    uint32_t m_current_query_level;

    // fix target list entry of the top level
    bool m_should_fix_top_level_target_list;

    // ctor
    SContextIncLevelsupMutator(uint32_t current_query_level, bool should_fix_top_level_target_list)
        : m_current_query_level(current_query_level),
          m_should_fix_top_level_target_list(should_fix_top_level_target_list) {}

    // dtor
    ~SContextIncLevelsupMutator() = default;
  };

  // context for walker that iterates over the expression in the target entry
  using CContextTLWalker = struct SContextTLWalker {
   public:
    // list of target list entries in the query
    List *m_target_entries;

    // list of grouping clauses
    List *m_group_clause;

    // ctor
    SContextTLWalker(List *target_entries, List *group_clause)
        : m_target_entries(target_entries), m_group_clause(group_clause) {}

    // dtor
    ~SContextTLWalker() = default;
  };

 public:
  // fall back during since the target list refers to a attribute which algebrizer at this point cannot resolve
  static bool ShouldFallback(Node *node, SContextTLWalker *context);

  // check if the project list contains expressions on aggregates thereby needing normalization
  static bool NeedsProjListNormalization(const Query *query);

  // normalize query
  static Query *NormalizeQuery(CMemoryPool *mp, CMDAccessor *md_accessor, const Query *query, uint32_t query_level);

  // check if the project list contains expressions on window operators thereby needing normalization
  static bool NeedsProjListWindowNormalization(const Query *query);

  // flatten expressions in window operation project list
  static Query *NormalizeWindowProjList(CMemoryPool *mp, CMDAccessor *md_accessor, const Query *original_query);

  // traverse the project list to extract all window functions in an arbitrarily complex project element
  static Node *RunWindowProjListMutator(Node *node, SContextGrpbyPlMutator *context);

  // flatten expressions in project list
  static Query *NormalizeGroupByProjList(CMemoryPool *mp, CMDAccessor *md_accessor, const Query *query);

  // make a copy of the aggref (minus the arguments)
  static Aggref *FlatCopyAggref(Aggref *aggref);

  // create a new entry in the derived table and return its corresponding var
  static Var *MakeVarInDerivedTable(Node *node, SContextGrpbyPlMutator *context);

  // check if a matching node exists in the list of target entries
  static Node *FindNodeInGroupByTargetList(Node *node, SContextGrpbyPlMutator *context);

  // increment the levels up of outer references
  static Var *IncrLevelsUpIfOuterRef(Var *var);

  // pull up having clause into a select
  static Query *NormalizeHaving(CMemoryPool *mp, CMDAccessor *md_accessor, const Query *query);

  // traverse the expression and fix the levels up of any outer reference
  static Node *RunIncrLevelsUpMutator(Node *node, SContextIncLevelsupMutator *context);

  // traverse the expression and fix the levels up of any CTE
  static bool RunFixCTELevelsUpWalker(Node *node, SContextIncLevelsupMutator *context);

  // mutate the grouping columns, fix levels up when necessary
  static Node *RunGroupingColMutator(Node *node, SContextGrpbyPlMutator *context);

  // fix the level up of grouping columns when necessary
  static Node *FixGroupingCols(Node *node, TargetEntry *original, SContextGrpbyPlMutator *context);

  // return a target entry for the aggregate expression
  static TargetEntry *GetTargetEntryForAggExpr(CMemoryPool *mp, CMDAccessor *md_accessor, Node *node, uint32_t attno);

  // traverse the having qual to extract all aggregate functions,
  // fix correlated vars and return the modified having qual
  static Node *RunExtractAggregatesMutator(Node *node, SContextGrpbyPlMutator *context);

  // for a given an TE in the derived table, create a new TE to be added to the top level query
  static TargetEntry *MakeTopLevelTargetEntry(TargetEntry *target_entry, uint32_t attno);

  // return the column name of the target entry
  static char *GetTargetEntryColName(TargetEntry *target_entry, Query *query);

  // make the input query into a derived table and return a new root query
  static Query *ConvertToDerivedTable(const Query *original_query, bool should_fix_target_list,
                                      bool should_fix_having_qual);

  // eliminate distinct clause
  static Query *EliminateDistinctClause(const Query *query);

  // reassign the sorting clause from the derived table to the new top-level query
  static void ReassignSortClause(Query *top_level_query, Query *derive_table_query);
};
}  // namespace gpdxl
#endif  // GPDXL_CWalkerUtils_H

// EOF
