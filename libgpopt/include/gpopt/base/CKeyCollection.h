//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC CORP.
//
//	@filename:
//		CKeyCollection.h
//
//	@doc:
//		Encodes key sets for a relation
//---------------------------------------------------------------------------
#ifndef GPOPT_CKeyCollection_H
#define GPOPT_CKeyCollection_H

#include "gpopt/base/CColRefSet.h"
#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CKeyCollection
//
//	@doc:
//		Captures sets of keys for a relation
//
//---------------------------------------------------------------------------
class CKeyCollection : public CRefCount {
 private:
  // array of key sets
  CColRefSetArray *m_pdrgpcrs;

 public:
  CKeyCollection(const CKeyCollection &) = delete;

  // ctors
  explicit CKeyCollection(CMemoryPool *mp);
  CKeyCollection(CMemoryPool *mp, CColRefSet *pcrs);
  CKeyCollection(CMemoryPool *mp, CColRefArray *colref_array);

  // dtor
  ~CKeyCollection() override;

  // add individual set -- takes ownership
  void Add(CColRefSet *pcrs);

  // check if set forms a key
  bool FKey(const CColRefSet *pcrs, bool fExactMatch = true) const;

  // check if an array of columns constitutes a key
  bool FKey(CMemoryPool *mp, const CColRefArray *colref_array) const;

  // trim off non-key columns
  CColRefArray *PdrgpcrTrim(CMemoryPool *mp, const CColRefArray *colref_array) const;

  // extract a key
  CColRefArray *PdrgpcrKey(CMemoryPool *mp) const;

  // extract a hashable key
  CColRefArray *PdrgpcrHashableKey(CMemoryPool *mp) const;

  // extract key at given position
  CColRefArray *PdrgpcrKey(CMemoryPool *mp, uint32_t ul) const;

  // extract key at given position
  CColRefSet *PcrsKey(CMemoryPool *mp, uint32_t ul) const;

  // number of keys
  uint32_t Keys() const { return m_pdrgpcrs->Size(); }

  // print
  IOstream &OsPrint(IOstream &os) const;

};  // class CKeyCollection

// shorthand for printing
inline IOstream &operator<<(IOstream &os, CKeyCollection &kc) {
  return kc.OsPrint(os);
}

}  // namespace gpopt

#endif  // !GPOPT_CKeyCollection_H

// EOF
