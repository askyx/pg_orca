//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTranslatorDXLToPlStmt.h
//
//	@doc:
//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorDxlToPlStmt_H
#define GPDXL_CTranslatorDxlToPlStmt_H

extern "C" {
#include <postgres.h>
}

#include "access/attnum.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CDXLTranslateContext.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"
#include "gpopt/translate/CMappingColIdVarPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToScalar.h"
#include "gpos/base.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexScan.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"

// fwd declarations
namespace gpopt {
class CMDAccessor;
}

namespace gpmd {
class IMDRelation;
class IMDIndex;
}  // namespace gpmd

struct PlannedStmt;
struct Scan;
struct HashJoin;
struct NestLoop;
struct MergeJoin;
struct Hash;
struct RangeTblEntry;
struct Motion;
struct Limit;
struct Agg;
struct Append;
struct Sort;
struct SubqueryScan;
struct SubPlan;
struct Result;
struct Material;
// struct Const;
// struct List;

namespace gpdxl {
using namespace gpopt;

// fwd decl
class CDXLNode;
class CDXLDirectDispatchInfo;

//---------------------------------------------------------------------------
//	@class:
//		CTranslatorDXLToPlStmt
//
//	@doc:
//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
//
//---------------------------------------------------------------------------
class CTranslatorDXLToPlStmt {
 private:
  // context for fixing index var attno
  struct SContextIndexVarAttno {
    // MD relation
    const IMDRelation *m_md_rel;

    // MD index
    const IMDIndex *m_md_index;

    // ctor
    SContextIndexVarAttno(const IMDRelation *md_rel, const IMDIndex *md_index)
        : m_md_rel(md_rel), m_md_index(md_index) {
      GPOS_ASSERT(nullptr != md_rel);
      GPOS_ASSERT(nullptr != md_index);
    }
  };  // SContextIndexVarAttno

  // context for finding security quals in an RTE
  struct SContextSecurityQuals {
    // relid of the RTE to search in the rewritten parse tree
    const OID m_relId;

    // List to hold the security quals present in an RTE
    List *m_security_quals{NIL};

    // ctor
    SContextSecurityQuals(const OID relId) : m_relId(relId) {}
  };  // SContextSecurityQuals

  // memory pool
  CMemoryPool *m_mp;

  // meta data accessor
  CMDAccessor *m_md_accessor;

  CContextDXLToPlStmt *m_dxl_to_plstmt_context;

  CTranslatorDXLToScalar *m_translator_dxl_to_scalar;

  // command type
  CmdType m_cmd_type;

  // list of result relations range table indexes for DML statements,
  // or NULL for select queries
  List *m_result_rel_list;

  // private copy ctor
  CTranslatorDXLToPlStmt(const CTranslatorDXLToPlStmt &);

  // walker to set index var attno's
  static bool SetIndexVarAttnoWalker(Node *node, SContextIndexVarAttno *ctxt_index_var_attno_walker);

  // walker to set inner var to outer
  static bool SetHashKeysVarnoWalker(Node *node, void *context);

  static bool FetchSecurityQualsWalker(Node *node, SContextSecurityQuals *ctxt_security_quals);

  static bool FetchSecurityQuals(Query *parsetree, SContextSecurityQuals *ctxt_security_quals);

  static bool SetSecurityQualsVarnoWalker(Node *node, Index *index);

 public:
  // ctor
  CTranslatorDXLToPlStmt(CMemoryPool *mp, CMDAccessor *md_accessor, CContextDXLToPlStmt *dxl_to_plstmt_context);

  // dtor
  ~CTranslatorDXLToPlStmt();

  // translate DXL operator node into a Plan node
  Plan *TranslateDXLOperatorToPlan(
      const CDXLNode *dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // main translation routine for DXL tree -> PlannedStmt
  PlannedStmt *GetPlannedStmtFromDXL(const CDXLNode *dxlnode, const Query *orig_query, bool can_set_tag);

  // translate the join types from its DXL representation to the GPDB one
  static JoinType GetGPDBJoinTypeFromDXLJoinType(EdxlJoinType join_type);

 private:
  // Set the bitmapset of a plan to the list of param_ids defined by the plan
  static void SetParamIds(Plan *);

  static List *TranslatePartOids(IMdIdArray *parts, INT lockmode);

  void TranslatePlan(Plan *plan, const CDXLNode *dxlnode, CDXLTranslateContext *output_context,
                     CContextDXLToPlStmt *dxl_to_plstmt_context, TranslateContextBaseTable *base_table_context,
                     CDXLTranslationContextArray *ctxt_translation_prev_siblings);

  // translate DXL table scan node into a SeqScan node
  Plan *TranslateDXLTblScan(
      const CDXLNode *tbl_scan_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL index scan node into a IndexScan node
  Plan *TranslateDXLIndexScan(
      const CDXLNode *index_scan_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translates a DXL index scan node into a IndexScan node
  Plan *TranslateDXLIndexScan(
      const CDXLNode *index_scan_dxlnode, gpdxl::CDXLPhysicalIndexScan *dxl_physical_idx_scan_op,
      CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL index scan node into a IndexOnlyScan node
  Plan *TranslateDXLIndexOnlyScan(
      const CDXLNode *index_scan_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL hash join into a HashJoin node
  Plan *TranslateDXLHashJoin(
      const CDXLNode *TranslateDXLHashJoin, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL nested loop join into a NestLoop node
  Plan *TranslateDXLNLJoin(
      const CDXLNode *nl_join_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL merge join into a MergeJoin node
  Plan *TranslateDXLMergeJoin(
      const CDXLNode *merge_join_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL aggregate node into GPDB Agg plan node
  Plan *TranslateDXLAgg(
      const CDXLNode *motion_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL window node into GPDB window node
  Plan *TranslateDXLWindow(
      const CDXLNode *motion_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL sort node into GPDB Sort plan node
  Plan *TranslateDXLSort(
      const CDXLNode *sort_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate a DXL node into a Hash node
  Plan *TranslateDXLHash(
      const CDXLNode *dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL Limit node into a Limit node
  Plan *TranslateDXLLimit(
      const CDXLNode *limit_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate DXL TVF into a GPDB Function Scan node
  Plan *TranslateDXLTvf(
      const CDXLNode *tvf_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  Plan *TranslateDXLProjectSet(const CDXLNode *result_dxlnode);

  Plan *CreateProjectSetNodeTree(const CDXLNode *result_dxlnode, Plan *result_node_plan, Plan *child_plan,
                                 Plan *&project_set_child_plan, bool &will_require_result_node);

  void MutateFuncExprToVarProjectSet(Plan *final_plan);

  Plan *TranslateDXLResult(
      const CDXLNode *result_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  Plan *TranslateDXLAppend(
      const CDXLNode *append_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  Plan *TranslateDXLMaterialize(
      const CDXLNode *materialize_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  Plan *TranslateDXLSharedScan(
      const CDXLNode *shared_scan_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate a DML operator
  Plan *TranslateDXLDml(
      const CDXLNode *dml_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate a (dynamic) bitmap table scan operator
  Plan *TranslateDXLBitmapTblScan(
      const CDXLNode *bitmapscan_dxlnode, CDXLTranslateContext *output_context,
      CDXLTranslationContextArray *ctxt_translation_prev_siblings  // translation contexts of previous siblings
  );

  // translate a DXL Value Scan into GPDB Value Scan
  Plan *TranslateDXLValueScan(const CDXLNode *value_scan_dxlnode, CDXLTranslateContext *output_context,
                              CDXLTranslationContextArray *ctxt_translation_prev_siblings);

  // translate DXL filter list into GPDB filter list
  List *TranslateDXLFilterList(const CDXLNode *filter_list_dxlnode, const TranslateContextBaseTable *base_table_context,
                               CDXLTranslationContextArray *child_contexts, CDXLTranslateContext *output_context);

  // create range table entry from a CDXLPhysicalTVF node
  RangeTblEntry *TranslateDXLTvfToRangeTblEntry(const CDXLNode *tvf_dxlnode, CDXLTranslateContext *output_context,
                                                TranslateContextBaseTable *base_table_context);

  // create range table entry from a CDXLPhysicalValueScan node
  RangeTblEntry *TranslateDXLValueScanToRangeTblEntry(const CDXLNode *value_scan_dxlnode,
                                                      CDXLTranslateContext *output_context,
                                                      TranslateContextBaseTable *base_table_context);

  // create range table entry from a table descriptor
  Index ProcessDXLTblDescr(const CDXLTableDescr *table_descr, TranslateContextBaseTable *base_table_context);

  // translate DXL projection list into a target list
  List *TranslateDXLProjList(const CDXLNode *project_list_dxlnode, const TranslateContextBaseTable *base_table_context,
                             CDXLTranslationContextArray *child_contexts, CDXLTranslateContext *output_context);

  // insert NULL values for dropped attributes to construct the target list for a DML statement
  List *CreateTargetListWithNullsForDroppedCols(List *target_list, const IMDRelation *md_rel);

  // create a target list containing column references for a hash node from the
  // project list of its child node
  static List *TranslateDXLProjectListToHashTargetList(const CDXLNode *project_list_dxlnode,
                                                       CDXLTranslateContext *child_context,
                                                       CDXLTranslateContext *output_context);

  List *TranslateDXLFilterToQual(const CDXLNode *filter_dxlnode, const TranslateContextBaseTable *base_table_context,
                                 CDXLTranslationContextArray *child_contexts, CDXLTranslateContext *output_context);

  // translate operator costs from the DXL cost structure into a Plan
  // struct used by GPDB
  void TranslatePlanCosts(const CDXLNode *dxlnode, Plan *plan);

  // shortcut for translating both the projection list and the filter
  void TranslateProjListAndFilter(const CDXLNode *project_list_dxlnode, const CDXLNode *filter_dxlnode,
                                  const TranslateContextBaseTable *base_table_context,
                                  CDXLTranslationContextArray *child_contexts, List **targetlist_out, List **qual_out,
                                  CDXLTranslateContext *output_context);

  void AddSecurityQuals(OID relId, List **qual, Index *index);

  // translate the hash expr list of a redistribute motion node
  void TranslateHashExprList(const CDXLNode *hash_expr_list_dxlnode, const CDXLTranslateContext *child_context,
                             List **hash_expr_out_list, List **hash_expr_types_out_list,
                             CDXLTranslateContext *output_context);

  // translate the tree of bitmap index operators that are under a (dynamic) bitmap table scan
  Plan *TranslateDXLBitmapAccessPath(const CDXLNode *bitmap_access_path_dxlnode, CDXLTranslateContext *output_context,
                                     const IMDRelation *md_rel, const CDXLTableDescr *table_descr,
                                     TranslateContextBaseTable *base_table_context,
                                     CDXLTranslationContextArray *ctxt_translation_prev_siblings,
                                     BitmapHeapScan *bitmap_tbl_scan);

  // translate a bitmap bool op expression
  Plan *TranslateDXLBitmapBoolOp(const CDXLNode *bitmap_boolop_dxlnode, CDXLTranslateContext *output_context,
                                 const IMDRelation *md_rel, const CDXLTableDescr *table_descr,
                                 TranslateContextBaseTable *base_table_context,
                                 CDXLTranslationContextArray *ctxt_translation_prev_siblings,
                                 BitmapHeapScan *bitmap_tbl_scan);

  // translate CDXLScalarBitmapIndexProbe into BitmapIndexScan or DynamicBitmapIndexScan
  Plan *TranslateDXLBitmapIndexProbe(const CDXLNode *bitmap_index_probe_dxlnode, CDXLTranslateContext *output_context,
                                     const IMDRelation *md_rel, const CDXLTableDescr *table_descr,
                                     TranslateContextBaseTable *base_table_context,
                                     CDXLTranslationContextArray *ctxt_translation_prev_siblings,
                                     BitmapHeapScan *bitmap_tbl_scan);

  static void TranslateSortCols(const CDXLNode *sort_col_list_dxl, const CDXLTranslateContext *child_context,
                                AttrNumber *att_no_sort_colids, Oid *sort_op_oids, Oid *sort_collations_oids,
                                bool *is_nulls_first);

  List *TranslateDXLScCondToQual(const CDXLNode *filter_dxlnode, const TranslateContextBaseTable *base_table_context,
                                 CDXLTranslationContextArray *child_contexts, CDXLTranslateContext *output_context);

  // parse string value into a Const
  static Cost CostFromStr(const CWStringBase *str);

  // check if the given operator is a DML operator on a distributed table
  bool IsTgtTblDistributed(CDXLOperator *dxlop);

  // add a target entry for a junk column with given colid to the target list
  static void AddJunkTargetEntryForColId(List **target_list, CDXLTranslateContext *dxl_translate_ctxt, ULONG colid,
                                         const char *resname);

  // translate the index condition list in an Index scan
  void TranslateIndexConditions(CDXLNode *index_cond_list_dxlnode, const CDXLTableDescr *dxl_tbl_descr,
                                bool is_bitmap_index_probe, const IMDIndex *index, const IMDRelation *md_rel,
                                CDXLTranslateContext *output_context, TranslateContextBaseTable *base_table_context,
                                CDXLTranslationContextArray *ctxt_translation_prev_siblings, List **index_cond,
                                List **index_orig_cond);

  // translate the index filters
  List *TranslateDXLIndexFilter(CDXLNode *filter_dxlnode, CDXLTranslateContext *output_context,
                                TranslateContextBaseTable *base_table_context,
                                CDXLTranslationContextArray *ctxt_translation_prev_siblings);

  // translate the assert constraints
  List *TranslateDXLAssertConstraints(CDXLNode *filter_dxlnode, CDXLTranslateContext *output_context,
                                      CDXLTranslationContextArray *child_contexts);

  // translate nest loop colrefs to GPDB nestparams
  static List *TranslateNestLoopParamList(CDXLColRefArray *pdrgdxlcrOuterRefs, CDXLTranslateContext *dxltrctxLeft,
                                          CDXLTranslateContext *dxltrctxRight);

  static Node *FixUpperExprMutatorProjectSet(Node *node, List *context);

  // checks if index is used for Order by.
  bool IsIndexForOrderBy(TranslateContextBaseTable *base_table_context,
                         CDXLTranslationContextArray *ctxt_translation_prev_siblings,
                         CDXLTranslateContext *output_context, CDXLNode *index_cond_list_dxlnode);
};
}  // namespace gpdxl

#endif  // !GPDXL_CTranslatorDxlToPlStmt_H

// EOF
