//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008, 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnFactory.h
//
//	@doc:
//		Column reference management; one instance per optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_CColumnFactory_H
#define GPOPT_CColumnFactory_H

#include "gpopt/base/CColRefSet.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/common/CSyncHashtable.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

namespace gpopt {
class CExpression;

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CColumnFactory
//
//	@doc:
//		Singleton factory class used to generate and manage CColRefs in ORCA.
//		The created CColRef objects are maintained in a hash table keyed by
//		Column ID.  CColumnFactory provides various overloaded PcrCreate()
//		methods to create CColRef and a LookupColRef() method to probe the hash
//		table.
//		NB: The class also owns the memory pool in which CColRefs are
//		allocated.
//
//---------------------------------------------------------------------------
class CColumnFactory {
 private:
  // MTS memory pool
  CMemoryPool *m_mp{nullptr};

  // mapping between column id of computed column and a set of used column references
  ColRefToColRefSetMap *m_phmcrcrs{nullptr};

  // id counter
  uint32_t m_aul{0};

  // hash table
  CSyncHashtable<CColRef, uint32_t> m_sht;

  // implementation of factory methods
  CColRef *PcrCreate(const IMDType *pmdtype, int32_t type_modifier, uint32_t id, const CName &name);
  CColRef *PcrCreate(const CColumnDescriptor *pcoldesc, uint32_t id, const CName &name, uint32_t ulOpSource,
                     bool mark_as_used = true, IMDId *mdid_table = nullptr);

 public:
  CColumnFactory(const CColumnFactory &) = delete;

  // ctor
  CColumnFactory();

  // dtor
  ~CColumnFactory();

  // initialize the hash map between computed column and used columns
  void Initialize();

  // create a column reference given only its type and type modifier, used for computed columns
  CColRef *PcrCreate(const IMDType *pmdtype, int32_t type_modifier);

  // create column reference given its type, type modifier, and name
  CColRef *PcrCreate(const IMDType *pmdtype, int32_t type_modifier, const CName &name);

  // create a column reference given its descriptor and name
  CColRef *PcrCreate(const CColumnDescriptor *pcoldescr, const CName &name, uint32_t ulOpSource, bool mark_as_used,
                     IMDId *mdid_table);

  // create a column reference given its type, attno, nullability and name
  CColRef *PcrCreate(const IMDType *pmdtype, int32_t type_modifier, bool mark_as_used, IMDId *mdid_table, int32_t attno,
                     bool is_nullable, uint32_t id, const CName &name, uint32_t ulOpSource, bool isDistCol,
                     uint32_t ulWidth = UINT32_MAX);

  // create a column reference with the same type as passed column reference
  CColRef *PcrCreate(const CColRef *colref) { return PcrCreate(colref->RetrieveType(), colref->TypeModifier()); }

  // add mapping between computed column to its used columns
  void AddComputedToUsedColsMap(CExpression *pexpr);

  // lookup the set of used column references (if any) based on id of computed column
  const CColRefSet *PcrsUsedInComputedCol(const CColRef *pcrComputedCol);

  // create a copy of the given colref
  CColRef *PcrCopy(const CColRef *colref);

  // lookup by id
  CColRef *LookupColRef(uint32_t id);

  // destructor
  void Destroy(CColRef *);

};  // class CColumnFactory
}  // namespace gpopt

#endif  // !GPOPT_CColumnFactory_H

// EOF
