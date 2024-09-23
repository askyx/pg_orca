//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CTableDescriptor.h
//
//	@doc:
//		Abstraction of metadata for tables; represents metadata as stored
//		in the catalog -- not as used in queries, e.g. no aliasing etc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CTableDescriptor_H
#define GPOPT_CTableDescriptor_H

#include "gpopt/base/CColRef.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/IMDId.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

// dynamic array of columns -- array owns columns
using CColumnDescriptorArray = CDynamicPtrArray<CColumnDescriptor, CleanupRelease>;

// dynamic array of bitsets
using CBitSetArray = CDynamicPtrArray<CBitSet, CleanupRelease>;

//---------------------------------------------------------------------------
//	@class:
//		CTableDescriptor
//
//	@doc:
//		metadata abstraction for tables
//
//---------------------------------------------------------------------------
class CTableDescriptor : public CRefCount {
 private:
  // memory pool
  CMemoryPool *m_mp;

  // mdid of the table
  IMDId *m_mdid;

  // name of table
  CName m_name;

  // array of columns
  CColumnDescriptorArray *m_pdrgpcoldesc;

  // storage type
  IMDRelation::Erelstoragetype m_erelstoragetype;

  // distribution columns for hash distribution
  CColumnDescriptorArray *m_pdrgpcoldescDist;

  // if true, we need to consider a hash distributed table as random
  // there are two possible scenarios:
  // 1. in hawq 2.0, some hash distributed tables need to be considered as random,
  //	  depending on its bucket number
  // 2. for a partitioned table, it may contain a part with a different distribution
  bool m_convert_hash_to_random;

  // indexes of partition columns for partitioned tables
  ULongPtrArray *m_pdrgpulPart;

  // key sets
  CBitSetArray *m_pdrgpbsKeys;

  // id of user the table needs to be accessed with
  uint32_t m_execute_as_user_id;

  // lockmode from the parser
  int32_t m_lockmode;

  // acl mode from the parser
  uint32_t m_acl_mode;

  // identifier of query to which current table belongs.
  // This field is used for assigning current table entry with
  // target one within DML operation. If descriptor doesn't point
  // to the target (result) relation it has value UNASSIGNED_QUERYID
  uint32_t m_assigned_query_id_for_target_rel;

 public:
  CTableDescriptor(const CTableDescriptor &) = delete;

  // ctor
  CTableDescriptor(CMemoryPool *, IMDId *mdid, const CName &, bool convert_hash_to_random,
                   IMDRelation::Erelstoragetype erelstoragetype, uint32_t ulExecuteAsUser, int32_t lockmode,
                   uint32_t acl_mode, uint32_t assigned_query_id_for_target_rel);

  // dtor
  ~CTableDescriptor() override;

  // add a column to the table descriptor
  void AddColumn(CColumnDescriptor *);

  // add the column at the specified position to the list of distribution columns
  void AddDistributionColumn(uint32_t ulPos, IMDId *opfamily);

  // add the column at the specified position to the list of partition columns
  void AddPartitionColumn(uint32_t ulPos);

  // add a keyset
  bool FAddKeySet(CBitSet *pbs);

  // accessors
  uint32_t ColumnCount() const;
  const CColumnDescriptor *Pcoldesc(uint32_t) const;

  // mdid accessor
  IMDId *MDId() const { return m_mdid; }

  // name accessor
  const CName &Name() const { return m_name; }

  // execute as user accessor
  uint32_t GetExecuteAsUserId() const { return m_execute_as_user_id; }

  int32_t LockMode() const { return m_lockmode; }

  uint32_t GetAclMode() const { return m_acl_mode; }

  // return the position of a particular attribute (identified by attno)
  uint32_t GetAttributePosition(int32_t attno) const;

  // column descriptor accessor
  CColumnDescriptorArray *Pdrgpcoldesc() const { return m_pdrgpcoldesc; }

  // distribution column descriptors accessor
  const CColumnDescriptorArray *PdrgpcoldescDist() const { return m_pdrgpcoldescDist; }

  // partition column indexes accessor
  const ULongPtrArray *PdrgpulPart() const { return m_pdrgpulPart; }

  // array of key sets
  const CBitSetArray *PdrgpbsKeys() const { return m_pdrgpbsKeys; }

  // storage type
  IMDRelation::Erelstoragetype RetrieveRelStorageType() const { return m_erelstoragetype; }

  bool IsPartitioned() const { return 0 < m_pdrgpulPart->Size(); }

  // true iff a hash distributed table needs to be considered as random;
  // this happens for when we are in phase 1 of a gpexpand or (for GPDB 5X)
  // when we have a mix of hash-distributed and random distributed partitions
  bool ConvertHashToRandom() const { return m_convert_hash_to_random; }

  // helper function for finding the index of a column descriptor in
  // an array of column descriptors
  static uint32_t UlPos(const CColumnDescriptor *, const CColumnDescriptorArray *);

  IOstream &OsPrint(IOstream &os) const;

  // returns number of indices
  uint32_t IndexCount();

  bool IsAORowOrColTable() const {
    return m_erelstoragetype == IMDRelation::ErelstorageAppendOnlyCols ||
           m_erelstoragetype == IMDRelation::ErelstorageAppendOnlyRows;
  }

  uint32_t GetAssignedQueryIdForTargetRel() const { return m_assigned_query_id_for_target_rel; }

  static uint32_t HashValue(const CTableDescriptor *ptabdesc);

  static bool Equals(const CTableDescriptor *ptabdescLeft, const CTableDescriptor *ptabdescRight);

};  // class CTableDescriptor

using CTableDescriptorHashSet =
    CHashSet<CTableDescriptor, CTableDescriptor::HashValue, CTableDescriptor::Equals, CleanupRelease<CTableDescriptor>>;
using CTableDescriptorHashSetIter = CHashSetIter<CTableDescriptor, CTableDescriptor::HashValue,
                                                 CTableDescriptor::Equals, CleanupRelease<CTableDescriptor>>;

}  // namespace gpopt

#endif  // !GPOPT_CTableDescriptor_H

// EOF
