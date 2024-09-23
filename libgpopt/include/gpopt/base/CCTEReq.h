//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CCTEReq.h
//
//	@doc:
//		CTE requirements. Each entry has a CTE id, whether it is a producer or
//		a consumer, whether it is required or optional (a required CTE has to
//		be in the derived CTE map of that subtree, while an optional CTE may or
//		may not be there). If the CTE entry represents a consumer, then the
//		plan properties of the corresponding producer are also part of that entry
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEReq_H
#define GPOPT_CCTEReq_H

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CDrvdPropPlan.h"
#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"
#include "gpos/common/CRefCount.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CCTEReq
//
//	@doc:
//		CTE requirements
//
//---------------------------------------------------------------------------
class CCTEReq : public CRefCount {
 private:
  //---------------------------------------------------------------------------
  //	@class:
  //		CCTEReqEntry
  //
  //	@doc:
  //		A single entry in the CTE requirement
  //
  //---------------------------------------------------------------------------
  class CCTEReqEntry : public CRefCount {
   private:
    // cte id
    uint32_t m_id;

    // cte type
    CCTEMap::ECteType m_ect;

    // is it required or optional
    bool m_fRequired;

    // plan properties of corresponding producer
    CDrvdPropPlan *m_pdpplan;

   public:
    CCTEReqEntry(const CCTEReqEntry &) = delete;

    // ctor
    CCTEReqEntry(uint32_t id, CCTEMap::ECteType ect, bool fRequired, CDrvdPropPlan *pdpplan);

    // dtor
    ~CCTEReqEntry() override;

    // cte id
    uint32_t Id() const { return m_id; }

    // cte type
    CCTEMap::ECteType Ect() const { return m_ect; }

    // required flag
    bool FRequired() const { return m_fRequired; }

    // plan properties
    CDrvdPropPlan *PdpplanProducer() const { return m_pdpplan; }

    // hash function
    uint32_t HashValue() const;

    // equality function
    bool Equals(CCTEReqEntry *pcre) const;

    // print function
    IOstream &OsPrint(IOstream &os) const;

  };  // class CCTEReqEntry

  // map CTE id to CTE Req entry
  using UlongToCTEReqEntryMap = CHashMap<uint32_t, CCTEReqEntry, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                         CleanupDelete<uint32_t>, CleanupRelease<CCTEReqEntry>>;

  // map iterator
  using UlongToCTEReqEntryMapIter =
      CHashMapIter<uint32_t, CCTEReqEntry, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>, CleanupDelete<uint32_t>,
                   CleanupRelease<CCTEReqEntry>>;

  // memory pool
  CMemoryPool *m_mp;

  // cte map
  UlongToCTEReqEntryMap *m_phmcter;

  // required cte ids (not optional)
  ULongPtrArray *m_pdrgpulRequired;

  // lookup info for given cte id
  CCTEReqEntry *PcreLookup(uint32_t ulCteId) const;

 public:
  CCTEReq(const CCTEReq &) = delete;

  // ctor
  explicit CCTEReq(CMemoryPool *mp);

  // dtor
  ~CCTEReq() override;

  // required cte ids
  ULongPtrArray *PdrgpulRequired() const { return m_pdrgpulRequired; }

  // return the CTE type associated with the given ID in the requirements
  CCTEMap::ECteType Ect(const uint32_t id) const;

  // insert a new entry, no entry with the same id can already exist
  void Insert(uint32_t ulCteId, CCTEMap::ECteType ect, bool fRequired, CDrvdPropPlan *pdpplan);

  // insert a new consumer entry with the given id. The plan properties are
  // taken from the given context
  void InsertConsumer(uint32_t id, CDrvdPropArray *pdrgpdpCtxt);

  // check if two cte requirements are equal
  bool Equals(const CCTEReq *pcter) const {
    GPOS_ASSERT(nullptr != pcter);
    return (m_phmcter->Size() == pcter->m_phmcter->Size()) && this->FSubset(pcter);
  }

  // check if current requirement is a subset of the given one
  bool FSubset(const CCTEReq *pcter) const;

  // check if the given CTE is in the requirements
  bool FContainsRequirement(const uint32_t id, const CCTEMap::ECteType ect) const;

  // hash function
  uint32_t HashValue() const;

  // returns a new requirement containing unresolved CTE requirements given a derived CTE map
  CCTEReq *PcterUnresolved(CMemoryPool *mp, CCTEMap *pcm);

  // unresolved CTE requirements given a derived CTE map for a sequence
  // operator
  CCTEReq *PcterUnresolvedSequence(CMemoryPool *mp, CCTEMap *pcm, CDrvdPropArray *pdrgpdpCtxt);

  // create a copy of the current requirement where all the entries are marked optional
  CCTEReq *PcterAllOptional(CMemoryPool *mp);

  // lookup plan properties for given cte id
  CDrvdPropPlan *Pdpplan(uint32_t ulCteId) const;

  // print function
  IOstream &OsPrint(IOstream &os) const;

};  // class CCTEMap

// shorthand for printing
IOstream &operator<<(IOstream &os, CCTEReq &cter);

}  // namespace gpopt

#endif  // !GPOPT_CCTEMap_H

// EOF
