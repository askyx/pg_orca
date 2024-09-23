//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCTEInfo.h
//
//	@doc:
//		Information about CTEs in a query
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEInfo_H
#define GPOPT_CCTEInfo_H

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/operators/CExpression.h"
#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CStack.h"

namespace gpopt {
// fwd declarations
class CLogicalCTEConsumer;

//---------------------------------------------------------------------------
//	@class:
//		CCTEInfo
//
//	@doc:
//		Global information about common table expressions (CTEs) including:
//		- the expression tree that defines each CTE
//		- the number of consumers created by the optimizer
//		- a mapping from consumer columns to producer columns
//
//---------------------------------------------------------------------------
class CCTEInfo : public CRefCount {
 private:
  //-------------------------------------------------------------------
  //	@struct:
  //		SConsumerCounter
  //
  //	@doc:
  //		Representation of the number of consumers of a given CTE inside
  // 		a specific context (e.g. inside the main query, inside another CTE, etc.)
  //
  //-------------------------------------------------------------------
  struct SConsumerCounter {
   private:
    // consumer ID
    uint32_t m_ulCTEId;

    // number of occurrences
    uint32_t m_ulCount;

   public:
    // ctor
    explicit SConsumerCounter(uint32_t ulCTEId) : m_ulCTEId(ulCTEId), m_ulCount(1) {}

    // consumer ID
    uint32_t UlCTEId() const { return m_ulCTEId; }

    // number of consumers
    uint32_t UlCount() const { return m_ulCount; }

    // increment number of consumers
    void Increment() { m_ulCount++; }
  };

  // hash map mapping uint32_t -> SConsumerCounter
  using UlongToConsumerCounterMap =
      CHashMap<uint32_t, SConsumerCounter, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>, CleanupDelete<uint32_t>,
               CleanupDelete<SConsumerCounter>>;

  // map iterator
  using UlongToConsumerCounterMapIter =
      CHashMapIter<uint32_t, SConsumerCounter, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                   CleanupDelete<uint32_t>, CleanupDelete<SConsumerCounter>>;

  // hash map mapping uint32_t -> UlongToConsumerCounterMap: maps from CTE producer ID to all consumers inside this CTE
  using UlongToProducerConsumerMap =
      CHashMap<uint32_t, UlongToConsumerCounterMap, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
               CleanupDelete<uint32_t>, CleanupRelease<UlongToConsumerCounterMap>>;

  //-------------------------------------------------------------------
  //	@struct:
  //		CCTEInfoEntry
  //
  //	@doc:
  //		A single entry for CTEInfo, representing a single CTE producer
  //
  //-------------------------------------------------------------------
  class CCTEInfoEntry : public CRefCount {
   private:
    // memory pool
    CMemoryPool *m_mp;

    // logical producer expression
    CExpression *m_pexprCTEProducer;

    // map columns of all created consumers of current CTE to their positions in consumer output
    ColRefToUlongMap *m_phmcrulConsumers;

    // is this CTE used
    bool m_fUsed;

    // does CTE have outer references outside CTE? If so, force inlining
    bool m_hasOuterReferences;

   public:
    // ctors
    CCTEInfoEntry(CMemoryPool *mp, CExpression *pexprCTEProducer);
    CCTEInfoEntry(CMemoryPool *mp, CExpression *pexprCTEProducer, bool fUsed);

    // dtor
    ~CCTEInfoEntry() override;

    // CTE expression
    CExpression *Pexpr() const { return m_pexprCTEProducer; }

    // is this CTE used?
    bool FUsed() const { return m_fUsed; }

    // CTE id
    uint32_t UlCTEId() const;

    // mark CTE as unused
    void MarkUnused() { m_fUsed = false; }

    // mark CTE as used
    void MarkUsed() { m_fUsed = true; }

    // add given columns to consumers column map
    void AddConsumerCols(CColRefArray *colref_array);

    // return position of given consumer column in consumer output
    uint32_t UlConsumerColPos(CColRef *colref);

    // check if CTE entry has outer reference
    bool HasOuterReferences() const { return m_hasOuterReferences; }

    // mark cte entry as containing an outer reference
    void SetHasOuterReferences() { m_hasOuterReferences = true; }

  };  // class CCTEInfoEntry

  // hash maps mapping uint32_t -> CCTEInfoEntry
  using UlongToCTEInfoEntryMap = CHashMap<uint32_t, CCTEInfoEntry, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                                          CleanupDelete<uint32_t>, CleanupRelease<CCTEInfoEntry>>;

  // map iterator
  using UlongToCTEInfoEntryMapIter =
      CHashMapIter<uint32_t, CCTEInfoEntry, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>, CleanupDelete<uint32_t>,
                   CleanupRelease<CCTEInfoEntry>>;

  // memory pool
  CMemoryPool *m_mp;

  // mapping from cte producer id -> cte info entry
  UlongToCTEInfoEntryMap *m_phmulcteinfoentry;

  // next available CTE Id
  uint32_t m_ulNextCTEId;

  // whether or not to inline CTE consumers
  bool m_fEnableInlining;

  // consumers inside each cte/main query
  UlongToProducerConsumerMap *m_phmulprodconsmap;

  // initialize default statistics for a given CTE Producer
  void InitDefaultStats(CExpression *pexprCTEProducer);

  // preprocess CTE producer expression
  CExpression *PexprPreprocessCTEProducer(const CExpression *pexprCTEProducer);

  // number of consumers of given CTE inside a given parent
  uint32_t UlConsumersInParent(uint32_t ulConsumerId, uint32_t ulParentId) const;

  // find all CTE consumers inside given parent, and push them to the given stack
  void FindConsumersInParent(uint32_t ulParentId, CBitSet *pbsUnusedConsumers, CStack<uint32_t> *pstack);

 public:
  CCTEInfo(const CCTEInfo &) = delete;

  // ctor
  explicit CCTEInfo(CMemoryPool *mp);

  // dtor
  ~CCTEInfo() override;

  // logical cte producer with given id
  CExpression *PexprCTEProducer(uint32_t ulCTEId) const;

  // number of CTE consumers of given CTE
  uint32_t UlConsumers(uint32_t ulCTEId) const;

  // check if given CTE is used
  bool FUsed(uint32_t ulCTEId) const;

  // check if given CTE has outer reference
  bool HasOuterReferences(uint32_t ulCTEId) const;

  // mark given cte as containing an outer reference
  void SetHasOuterReferences(uint32_t ulCTEId);

  // increment number of CTE consumers
  void IncrementConsumers(uint32_t ulConsumerId, uint32_t ulParentCTEId = UINT32_MAX);

  // add cte producer to hashmap
  void AddCTEProducer(CExpression *pexprCTEProducer);

  // replace cte producer with given expression
  void ReplaceCTEProducer(CExpression *pexprCTEProducer);

  // next available CTE id
  uint32_t next_id() { return m_ulNextCTEId++; }

  // derive the statistics on the CTE producer
  void DeriveProducerStats(CLogicalCTEConsumer *popConsumer,  // CTE Consumer operator
                           CColRefSet *pcrsStat               // required stat columns on the CTE Consumer
  );

  // return a CTE requirement with all the producers as optional
  CCTEReq *PcterProducers(CMemoryPool *mp) const;

  // return an array of all stored CTE expressions
  CExpressionArray *PdrgPexpr(CMemoryPool *mp) const;

  // disable CTE inlining
  void DisableInlining() { m_fEnableInlining = false; }

  // whether or not to inline CTE consumers
  bool FEnableInlining() const { return m_fEnableInlining; }

  // mark unused CTEs
  void MarkUnusedCTEs();

  // walk the producer expressions and add the mapping between computed column
  // and their corresponding used column(s)
  void MapComputedToUsedCols(CColumnFactory *col_factory) const;

  // add given columns to consumers column map
  void AddConsumerCols(uint32_t ulCTEId, CColRefArray *colref_array);

  // return position of given consumer column in consumer output
  uint32_t UlConsumerColPos(uint32_t ulCTEId, CColRef *colref);

  // return a map from Id's of consumer columns in the given column set to their corresponding producer columns
  UlongToColRefMap *PhmulcrConsumerToProducer(CMemoryPool *mp, uint32_t ulCTEId, CColRefSet *pcrs,
                                              CColRefArray *pdrgpcrProducer);

};  // CCTEInfo
}  // namespace gpopt

#endif  // !GPOPT_CCTEInfo_H

// EOF
