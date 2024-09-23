//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPartInfo.h
//
//	@doc:
//		Derived partition information at the logical level
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartInfo_H
#define GPOPT_CPartInfo_H

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CPartKeys.h"
#include "gpos/base.h"

// fwd decl
namespace gpmd {
class IMDId;
}

namespace gpopt {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CPartInfo
//
//	@doc:
//		Derived partition information at the logical level
//
//---------------------------------------------------------------------------
class CPartInfo : public CRefCount {
 private:
  //---------------------------------------------------------------------------
  //	@class:
  //		CPartInfoEntry
  //
  //	@doc:
  //		A single entry of the CPartInfo
  //
  //---------------------------------------------------------------------------
  class CPartInfoEntry : public CRefCount {
   private:
    // scan id
    uint32_t m_scan_id;

    // partition table mdid
    IMDId *m_mdid;

    // partition keys
    CPartKeysArray *m_pdrgppartkeys;

   public:
    CPartInfoEntry(const CPartInfoEntry &) = delete;

    // ctor
    CPartInfoEntry(uint32_t scan_id, IMDId *mdid, CPartKeysArray *pdrgppartkeys);

    // dtor
    ~CPartInfoEntry() override;

    // scan id
    virtual uint32_t ScanId() const { return m_scan_id; }

    // create a copy of the current object, and add a set of remapped
    // part keys to this entry, using the existing keys and the given hashmap
    CPartInfoEntry *PpartinfoentryAddRemappedKeys(CMemoryPool *mp, CColRefSet *pcrs, UlongToColRefMap *colref_mapping);

    // mdid of partition table
    virtual IMDId *MDId() const { return m_mdid; }

    // partition keys of partition table
    virtual CPartKeysArray *Pdrgppartkeys() const { return m_pdrgppartkeys; }

    // print function
    IOstream &OsPrint(IOstream &os) const;

    // copy part info entry into given memory pool
    CPartInfoEntry *PpartinfoentryCopy(CMemoryPool *mp) const;

  };  // CPartInfoEntry

  using CPartInfoEntryArray = CDynamicPtrArray<CPartInfoEntry, CleanupRelease>;

  // partition table consumers
  CPartInfoEntryArray *m_pdrgppartentries;

  // private ctor
  explicit CPartInfo(CPartInfoEntryArray *pdrgppartentries);

 public:
  CPartInfo(const CPartInfo &) = delete;

  // ctor
  explicit CPartInfo(CMemoryPool *mp);

  // dtor
  ~CPartInfo() override;

  // number of part table consumers
  uint32_t UlConsumers() const { return m_pdrgppartentries->Size(); }

  // add part table consumer
  void AddPartConsumer(CMemoryPool *mp, uint32_t scan_id, IMDId *mdid, CColRef2dArray *pdrgpdrgpcrPart);

  // scan id of the entry at the given position
  uint32_t ScanId(uint32_t ulPos) const;

  // relation mdid of the entry at the given position
  IMDId *GetRelMdId(uint32_t ulPos) const;

  // part keys of the entry at the given position
  CPartKeysArray *Pdrgppartkeys(uint32_t ulPos) const;

  // check if part info contains given scan id
  bool FContainsScanId(uint32_t scan_id) const;

  // part keys of the entry with the given scan id
  CPartKeysArray *PdrgppartkeysByScanId(uint32_t scan_id) const;

  // return a new part info object with an additional set of remapped keys
  CPartInfo *PpartinfoWithRemappedKeys(CMemoryPool *mp, CColRefArray *pdrgpcrSrc, CColRefArray *pdrgpcrDest) const;

  // print
  IOstream &OsPrint(IOstream &) const;

  // combine two part info objects
  static CPartInfo *PpartinfoCombine(CMemoryPool *mp, CPartInfo *ppartinfoFst, CPartInfo *ppartinfoSnd);

};  // CPartInfo

// shorthand for printing
inline IOstream &operator<<(IOstream &os, CPartInfo &partinfo) {
  return partinfo.OsPrint(os);
}
}  // namespace gpopt

#endif  // !GPOPT_CPartInfo_H

// EOF
