//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC Corp.
//
//	@filename:
//		CMemo.h
//
//	@doc:
//		Memo lookup table for dynamic programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CMemo_H
#define GPOPT_CMemo_H

#include "gpopt/search/CGroupExpression.h"
#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncList.h"

namespace gpopt {
class CGroup;
class CDrvdProp;
class CDrvdPropCtxtPlan;
class CMemoProxy;
class COptimizationContext;

// memo tree map definition
using MemoTreeMap =
    CTreeMap<CCostContext, CExpression, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals>;

using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMemo
//
//	@doc:
//		Dynamic programming table
//
//---------------------------------------------------------------------------
class CMemo {
 private:
  // definition of hash table key accessor
  using ShtAcc = CSyncHashtableAccessByKey<CGroupExpression, CGroupExpression>;

  // definition of hash table iterator
  using ShtIter = CSyncHashtableIter<CGroupExpression, CGroupExpression>;

  // definition of hash table iterator accessor
  using ShtAccIter = CSyncHashtableAccessByIter<CGroupExpression, CGroupExpression>;

  // memory pool
  CMemoryPool *m_mp;

  // id counter for groups
  uint32_t m_aul;

  // root group
  CGroup *m_pgroupRoot;

  // number of groups
  uintptr_t m_ulpGrps;

  // tree map of member group expressions
  MemoTreeMap *m_pmemotmap;

  // list of groups
  CSyncList<CGroup> m_listGroups;

  // hashtable of all group expressions
  CSyncHashtable<CGroupExpression,  // entry
                 CGroupExpression>
      m_sht;

  // add new group
  void Add(CGroup *pgroup, CExpression *pexprOrigin);

  // rehash all group expressions after group merge - not thread-safe
  bool FRehash();

  // helper for inserting group expression in target group
  CGroup *PgroupInsert(CGroup *pgroupTarget, CGroupExpression *pgexpr, CExpression *pexprOrigin, bool fNewGroup);

  // helper to check if a new group needs to be created
  bool FNewGroup(CGroup **ppgroupTarget, CGroupExpression *pgexpr, bool fScalar);

 public:
  CMemo(const CMemo &) = delete;

  // ctor
  explicit CMemo(CMemoryPool *mp);

  // dtor
  ~CMemo();

  // return root group
  CGroup *PgroupRoot() const { return m_pgroupRoot; }

  // return number of groups
  uintptr_t UlpGroups() const { return m_ulpGrps; }

  // return total number of group expressions
  uint32_t UlGrpExprs();

  // return number of duplicate groups
  uint32_t UlDuplicateGroups();

  // mark groups as duplicates
  static void MarkDuplicates(CGroup *pgroupFst, CGroup *pgroupSnd);

  // return tree map
  MemoTreeMap *Pmemotmap() const { return m_pmemotmap; }

  // set root group
  void SetRoot(CGroup *pgroup);

  // insert group expression into hash table
  CGroup *PgroupInsert(CGroup *pgroupTarget, CExpression *pexprOrigin, CGroupExpression *pgexpr);

  // extract a plan that delivers the given required properties
  CExpression *PexprExtractPlan(CMemoryPool *mp, CGroup *pgroupRoot, CReqdPropPlan *prppInput, uint32_t ulSearchStages);

  // merge duplicate groups
  void GroupMerge();

  // reset states of all memo groups
  void ResetGroupStates();

  // reset statistics of memo groups
  void ResetStats();

  // print driver
  IOstream &OsPrint(IOstream &os) const;

  // derive stats when no stats not present for the group
  void DeriveStatsIfAbsent(CMemoryPool *mp);

  // build tree map
  void BuildTreeMap(COptimizationContext *poc);

  // reset tree map
  void ResetTreeMap();

  // print memo to output logger
  void Trace();

  // get group by id
  CGroup *Pgroup(uint32_t id);

};  // class CMemo

}  // namespace gpopt

#endif  // !GPOPT_CMemo_H

// EOF
