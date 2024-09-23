//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPartitionPropagationSpec.h
//
//	@doc:
//		Partition Propagation spec in required properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartitionPropagationSpec_H
#define GPOPT_CPartitionPropagationSpec_H

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CPropSpec.h"
#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CPartitionPropagationSpec
//
//	@doc:
//		Partition Propagation specification
//
//---------------------------------------------------------------------------
class CPartitionPropagationSpec : public CPropSpec {
 public:
  enum EPartPropSpecInfoType { EpptPropagator, EpptConsumer, EpptSentinel };

 private:
  struct SPartPropSpecInfo : public CRefCount {
    // scan id of the DynamicScan
    uint32_t m_scan_id;

    // info type: consumer or propagator
    EPartPropSpecInfoType m_type;

    // relation id of the DynamicScan
    IMDId *m_root_rel_mdid;

    //  partition selector ids to use (reqd only)
    CBitSet *m_selector_ids = nullptr;

    // filter expressions to generate partition pruning data in the translator (reqd only)
    CExpression *m_filter_expr = nullptr;

    SPartPropSpecInfo(uint32_t scan_id, EPartPropSpecInfoType type, IMDId *rool_rel_mdid)
        : m_scan_id(scan_id), m_type(type), m_root_rel_mdid(rool_rel_mdid) {
      GPOS_ASSERT(m_root_rel_mdid != nullptr);

      CMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
      m_selector_ids = GPOS_NEW(mp) CBitSet(mp);
    }

    ~SPartPropSpecInfo() override {
      m_root_rel_mdid->Release();
      CRefCount::SafeRelease(m_selector_ids);
      CRefCount::SafeRelease(m_filter_expr);
    }

    // hash function
    uint32_t HashValue() const {
      uint32_t ulHash = m_root_rel_mdid->HashValue();

      ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<uint32_t>(&m_scan_id));
      if (m_selector_ids) {
        ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CBitSet>(m_selector_ids));
      }
      if (m_filter_expr) {
        ulHash = gpos::CombineHashes(ulHash, CExpression::HashValue(m_filter_expr));
      }

      return ulHash;
    }

    IOstream &OsPrint(IOstream &os) const;

    // used for determining equality in memo (e.g in optimization contexts)
    bool Equals(const SPartPropSpecInfo *) const;

    bool FSatisfies(const SPartPropSpecInfo *) const;

    // used for sorting SPartPropSpecInfo in an array
    static int32_t CmpFunc(const void *val1, const void *val2);
  };

  // partition required/derived info, sorted by scanid
  using UlongToSPartPropSpecInfoMap =
      CHashMap<uint32_t, SPartPropSpecInfo, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>, CleanupDelete<uint32_t>,
               CleanupRelease<SPartPropSpecInfo>>;

  using UlongToSPartPropSpecInfoMapIter =
      CHashMapIter<uint32_t, SPartPropSpecInfo, gpos::HashValue<uint32_t>, gpos::Equals<uint32_t>,
                   CleanupDelete<uint32_t>, CleanupRelease<SPartPropSpecInfo>>;

  UlongToSPartPropSpecInfoMap *m_part_prop_spec_infos = nullptr;

  // Present scanids (for easy lookup)
  CBitSet *m_scan_ids = nullptr;

 public:
  CPartitionPropagationSpec(const CPartitionPropagationSpec &) = delete;

  // ctor
  CPartitionPropagationSpec(CMemoryPool *mp);

  // dtor
  ~CPartitionPropagationSpec() override;

  // append enforcers to dynamic array for the given plan properties
  void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
                       CExpression *pexpr) override;

  // hash function
  uint32_t HashValue() const override {
    uint32_t ulHash = 0;

    UlongToSPartPropSpecInfoMapIter hmulpi(m_part_prop_spec_infos);
    while (hmulpi.Advance()) {
      const SPartPropSpecInfo *info = hmulpi.Value();
      ulHash = gpos::CombineHashes(ulHash, info->HashValue());
    }

    return ulHash;
  }

  // extract columns used by the partition propagation spec
  CColRefSet *PcrsUsed(CMemoryPool *mp) const override {
    // return an empty set
    return GPOS_NEW(mp) CColRefSet(mp);
  }

  // property type
  EPropSpecType Epst() const override { return EpstPartPropagation; }

  bool Contains(uint32_t scan_id) const { return m_scan_ids->Get(scan_id); }

  bool ContainsAnyConsumers() const;

  // equality check to determine compatibility of derived & required properties
  bool Equals(const CPartitionPropagationSpec *ppps) const;

  // satisfies function
  bool FSatisfies(const CPartitionPropagationSpec *pps_reqd) const;

  // Check if there is an unsupported part prop spec between two properties
  bool IsUnsupportedPartSelector(const CPartitionPropagationSpec *pps_reqd) const;

  SPartPropSpecInfo *FindPartPropSpecInfo(uint32_t scan_id) const;

  void Insert(uint32_t scan_id, EPartPropSpecInfoType type, IMDId *rool_rel_mdid, CBitSet *selector_ids,
              CExpression *expr);

  void Insert(SPartPropSpecInfo *other);

  void InsertAll(CPartitionPropagationSpec *pps);

  void InsertAllowedConsumers(CPartitionPropagationSpec *pps, CBitSet *allowed_scan_ids);

  void InsertAllExcept(CPartitionPropagationSpec *pps, uint32_t scan_id);

  const CBitSet *SelectorIds(uint32_t scan_id) const;

  // is partition propagation required
  bool FPartPropagationReqd() const { return true; }

  // print
  IOstream &OsPrint(IOstream &os) const override;

  void InsertAllResolve(CPartitionPropagationSpec *pSpec);
};  // class CPartitionPropagationSpec

}  // namespace gpopt

#endif  // !GPOPT_CPartitionPropagationSpec_H

// EOF
