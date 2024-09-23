//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPartKeys.h
//
//	@doc:
//		A collection of partitioning keys for a partitioned table
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartKeys_H
#define GPOPT_CPartKeys_H

#include "gpopt/base/CColRef.h"
#include "gpos/base.h"

namespace gpopt {
using namespace gpos;

// fwd decl
class CColRefSet;
class CPartKeys;

// array of part keys
using CPartKeysArray = CDynamicPtrArray<CPartKeys, CleanupRelease>;

//---------------------------------------------------------------------------
//	@class:
//		CPartKeys
//
//	@doc:
//		A collection of partitioning keys for a partitioned table
//
//---------------------------------------------------------------------------
class CPartKeys : public CRefCount {
 private:
  // partitioning keys
  CColRef2dArray *m_pdrgpdrgpcr;

  // number of levels
  uint32_t m_num_of_part_levels;

 public:
  CPartKeys(const CPartKeys &) = delete;

  // ctor
  explicit CPartKeys(CColRef2dArray *pdrgpdrgpcr);

  // dtor
  ~CPartKeys() override;

  // return key at a given level
  CColRef *PcrKey(uint32_t ulLevel) const;

  // return array of keys
  CColRef2dArray *Pdrgpdrgpcr() const { return m_pdrgpdrgpcr; }

  // number of levels
  uint32_t GetPartitioningLevel() const { return m_num_of_part_levels; }

  // copy part key into the given memory pool
  CPartKeys *PpartkeysCopy(CMemoryPool *mp);

  // check whether the key columns overlap the given column
  bool FOverlap(CColRefSet *pcrs) const;

  // create a new PartKeys object from the current one by remapping the
  // keys using the given hashmap
  CPartKeys *PpartkeysRemap(CMemoryPool *mp, UlongToColRefMap *colref_mapping) const;

  // print
  IOstream &OsPrint(IOstream &os) const;

  // copy array of part keys into given memory pool
  static CPartKeysArray *PdrgppartkeysCopy(CMemoryPool *mp, const CPartKeysArray *pdrgppartkeys);

};  // CPartKeys

// shorthand for printing
inline IOstream &operator<<(IOstream &os, CPartKeys &partkeys) {
  return partkeys.OsPrint(os);
}

}  // namespace gpopt

#endif  // !GPOPT_CPartKeys_H

// EOF
