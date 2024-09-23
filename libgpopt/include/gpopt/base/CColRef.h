//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CColRef.h
//
//	@doc:
//		Column reference implementation
//---------------------------------------------------------------------------
#ifndef GPOS_CColRef_H
#define GPOS_CColRef_H

#include "gpopt/metadata/CName.h"
#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CList.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/traceflags/traceflags.h"

namespace gpopt {
using namespace gpos;
using namespace gpmd;

class CColRef;

// colref array
using CColRefArray = CDynamicPtrArray<CColRef, CleanupNULL>;
using CColRef2dArray = CDynamicPtrArray<CColRefArray, CleanupRelease>;

// hash map mapping uint32_t -> CColRef
using UlongToColRefMap = CHashMap<uint32_t, CColRef, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                  CleanupDelete<uint32_t>, CleanupNULL<CColRef>>;
// hash map mapping uint32_t -> const CColRef
using UlongToConstColRefMap = CHashMap<uint32_t, const CColRef, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                       CleanupDelete<uint32_t>, CleanupNULL<const CColRef>>;
// iterator
using UlongToColRefMapIter = CHashMapIter<uint32_t, CColRef, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                          CleanupDelete<uint32_t>, CleanupNULL<CColRef>>;

//---------------------------------------------------------------------------
//	@class:
//		CColRef
//
//	@doc:
//		Column Reference base class
//		Non-refcounted objects; passed by reference; managed by separate
//		factory object
//
//---------------------------------------------------------------------------
class CColRef {
 public:
  enum EUsedStatus { EUsed, EUnused, EUnknown, ESentinel };

 private:
  // type information
  const IMDType *m_pmdtype;

  // type modifier
  const int32_t m_type_modifier;

  // name: SQL alias or artificial name
  const CName *m_pname;

  // track the usage of colref (used/unused/unknown)
  EUsedStatus m_used;

  // table info
  IMDId *m_mdid_table;

 public:
  CColRef(const CColRef &) = delete;

  enum Ecolreftype {
    EcrtTable,
    EcrtComputed,

    EcrtSentinel
  };

  // ctor
  CColRef(const IMDType *pmdtype, const int32_t type_modifier, uint32_t id, const CName *pname);

  // dtor
  virtual ~CColRef();

  // accessor to type info
  const IMDType *RetrieveType() const { return m_pmdtype; }

  // type modifier
  int32_t TypeModifier() const { return m_type_modifier; }

  // name
  const CName &Name() const { return *m_pname; }

  // id
  uint32_t Id() const { return m_id; }

  // overloaded equality operator
  bool operator==(const CColRef &cr) const { return Equals(m_id, cr.Id()); }

  // static hash functions
  static uint32_t HashValue(const uint32_t &);

  static uint32_t HashValue(const CColRef *colref);

  // equality function for hash table
  static bool Equals(const uint32_t &ulKey, const uint32_t &ulKeyOther) { return ulKey == ulKeyOther; }

  // equality function
  static bool Equals(const CColRef *pcrFirst, const CColRef *pcrSecond) {
    return Equals(pcrFirst->Id(), pcrSecond->Id());
  }

  // extract array of colids from array of colrefs
  static ULongPtrArray *Pdrgpul(CMemoryPool *mp, CColRefArray *colref_array);

  // check if the the array of column references are equal
  static bool Equals(const CColRefArray *pdrgpcr1, const CColRefArray *pdrgpcr2);

  // check if the the array of column reference arrays are equal
  static bool Equals(const CColRef2dArray *pdrgdrgpcr1, const CColRef2dArray *pdrgdrgpcr2);

  // type of column reference (base/computed)
  virtual Ecolreftype Ecrt() const = 0;

  // is column a system column?
  virtual bool IsSystemCol() const = 0;

  // is column a distribution column?
  virtual bool IsDistCol() const = 0;

  // is column a partition column?
  virtual bool IsPartCol() const = 0;

  // print
  IOstream &OsPrint(IOstream &) const;

  // link for hash chain
  SLink m_link;

  // id, serves as hash key
  const uint32_t m_id;

  // invalid key
  static const uint32_t m_ulInvalid;

  void MarkAsUnused() {
    GPOS_ASSERT(m_used != EUsed);
    m_used = EUnused;
  }

  void MarkAsUsed() { m_used = EUsed; }

  void MarkAsUnknown() { m_used = EUnknown; }

  EUsedStatus GetUsage(bool check_system_col = false, bool check_distribution_col = false) const {
    if ((!check_system_col && IsSystemCol()) || (!check_distribution_col && IsDistCol())) {
      return EUsed;
    }

    return m_used;
  }

  IMDId *GetMdidTable() const { return m_mdid_table; };

  void SetMdidTable(IMDId *mdid_table) { m_mdid_table = mdid_table; }

};  // class CColRef

// shorthand for printing
inline IOstream &operator<<(IOstream &os, CColRef &cr) {
  return cr.OsPrint(os);
}

// hash map: CColRef -> uint32_t
using ColRefToUlongMap = CHashMap<CColRef, uint32_t, CColRef::HashValue, gpos::Equals<CColRef>, CleanupNULL<CColRef>,
                                  CleanupDelete<uint32_t>>;

using ColRefToUlongMapArray = CDynamicPtrArray<ColRefToUlongMap, CleanupRelease>;

}  // namespace gpopt

#endif  // !GPOS_CColRef_H

// EOF
