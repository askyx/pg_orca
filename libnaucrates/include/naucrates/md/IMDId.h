//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDId.h
//
//	@doc:
//		Abstract class for representing metadata object ids
//---------------------------------------------------------------------------

#ifndef GPMD_IMDId_H
#define GPMD_IMDId_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashSet.h"
#include "gpos/common/CHashSetIter.h"
#include "gpos/string/CWStringConst.h"
#include "naucrates/dxl/gpdb_types.h"

#define GPDXL_MDID_LENGTH 100

// fwd decl
namespace gpdxl {}

namespace gpmd {
using namespace gpos;
using namespace gpdxl;

// fwd decl
class CSystemId;

static const int32_t default_type_modifier = -1;

//---------------------------------------------------------------------------
//	@class:
//		IMDId
//
//	@doc:
//		Abstract class for representing metadata objects ids
//
//---------------------------------------------------------------------------
class IMDId : public CRefCount {
 private:
  // number of deletion locks -- each MDAccessor adds a new deletion lock if it uses
  // an MDId object in its internal hash-table, the deletion lock is released when
  // MDAccessor is destroyed
  uintptr_t m_deletion_locks{0};

 public:
  //------------------------------------------------------------------
  //	@doc:
  //		Type of md id.
  //		The exact values are important when parsing mdids from DXL and
  //		should not be modified without modifying the parser
  //
  //------------------------------------------------------------------
  enum EMDIdType {
    EmdidGeneral = 0,
    EmdidColStats = 1,
    EmdidRelStats = 2,
    EmdidCastFunc = 3,
    EmdidScCmp = 4,
    EmdidRel = 6,
    EmdidInd = 7,
    EmdidCheckConstraint = 8,
    EmdidExtStats = 9,
    EmdidExtStatsInfo = 10,
    EmdidSentinel
  };

  // ctor
  IMDId() = default;

  // dtor
  ~IMDId() override = default;

  // type of mdid
  virtual EMDIdType MdidType() const = 0;

  // string representation of mdid
  virtual const wchar_t *GetBuffer() const = 0;

  // system id
  virtual CSystemId Sysid() const = 0;

  // equality check
  virtual bool Equals(const IMDId *mdid) const = 0;

  // computes the hash value for the metadata id
  virtual uint32_t HashValue() const = 0;

  // return true if calling object's destructor is allowed
  bool Deletable() const override { return (0 == m_deletion_locks); }

  // increase number of deletion locks
  void AddDeletionLock() { m_deletion_locks++; }

  // decrease number of deletion locks
  void RemoveDeletionLock() {
    GPOS_ASSERT(0 < m_deletion_locks);

    m_deletion_locks--;
  }

  // return number of deletion locks
  uintptr_t DeletionLocks() const { return m_deletion_locks; }

  // static hash functions for use in different indexing structures,
  // e.g. hashmaps, MD cache, etc.
  static uint32_t MDIdHash(const IMDId *mdid) {
    GPOS_ASSERT(nullptr != mdid);
    return mdid->HashValue();
  }

  // static equality functions for use in different structures,
  // e.g. hashmaps, MD cache, etc.
  static bool MDIdCompare(const IMDId *left_mdid, const IMDId *right_mdid) {
    GPOS_ASSERT(nullptr != left_mdid && nullptr != right_mdid);
    return left_mdid->Equals(right_mdid);
  }

  // Compare function used by CDynamicPtrArray::Sort
  static int32_t CompareHashVal(const void *left, const void *right) {
    if ((*((IMDId **)left))->HashValue() < (*((IMDId **)right))->HashValue()) {
      return -1;
    } else if ((*((IMDId **)left))->HashValue() > ((*(IMDId **)right))->HashValue()) {
      return 1;
    }

    GPOS_ASSERT((*((IMDId **)left))->HashValue() == ((*(IMDId **)right))->HashValue());
    return 0;
  }

  // is the mdid valid
  virtual bool IsValid() const = 0;

  // safe validity function
  static bool IsValid(const IMDId *mdid) { return nullptr != mdid && mdid->IsValid(); }

  virtual gpos::IOstream &OsPrint(gpos::IOstream &os) const = 0;

  // make a copy in the given memory pool
  virtual IMDId *Copy(CMemoryPool *mp) const = 0;
};

// common structures over metadata id elements
using IMdIdArray = CDynamicPtrArray<IMDId, CleanupRelease>;

// hash set for mdid
using MdidHashSet = CHashSet<IMDId, IMDId::MDIdHash, IMDId::MDIdCompare, CleanupRelease<IMDId>>;

// iterator over the hash set for column id information for missing statistics
using MdidHashSetIter = CHashSetIter<IMDId, IMDId::MDIdHash, IMDId::MDIdCompare, CleanupRelease<IMDId>>;

}  // namespace gpmd

#endif  // !GPMD_IMDId_H

// EOF
