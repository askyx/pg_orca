//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIndexGet.h
//
//	@doc:
//		Basic index accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIndexGet_H
#define GPOPT_CLogicalIndexGet_H

#include "gpopt/base/COrderSpec.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CLogical.h"
#include "gpos/base.h"

namespace gpopt {
// fwd declarations
class CName;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalIndexGet
//
//	@doc:
//		Basic index accessor
//
//---------------------------------------------------------------------------
class CLogicalIndexGet : public CLogical {
 private:
  // index descriptor
  CIndexDescriptor *m_pindexdesc;

  // table descriptor
  CTableDescriptor *m_ptabdesc;

  // origin operator id -- UINT32_MAX if operator was not generated via a transformation
  uint32_t m_ulOriginOpId;

  // alias for table
  const CName *m_pnameAlias;

  // output columns
  CColRefArray *m_pdrgpcrOutput;

  // set representation of output columns
  CColRefSet *m_pcrsOutput;

  // order spec
  COrderSpec *m_pos;

  // distribution columns (empty for coordinator only tables)
  CColRefSet *m_pcrsDist;

  // Number of predicate not applicable on the index
  uint32_t m_ulUnindexedPredColCount;

  // index scan direction
  EIndexScanDirection m_scan_direction;

 public:
  CLogicalIndexGet(const CLogicalIndexGet &) = delete;

  // ctors
  explicit CLogicalIndexGet(CMemoryPool *mp);

  CLogicalIndexGet(CMemoryPool *mp, const IMDIndex *pmdindex, CTableDescriptor *ptabdesc, uint32_t ulOriginOpId,
                   const CName *pnameAlias, CColRefArray *pdrgpcrOutput, uint32_t ulUnindexedPredColCount,
                   EIndexScanDirection scan_direction);

  // dtor
  ~CLogicalIndexGet() override;

  // ident accessors
  EOperatorId Eopid() const override { return EopLogicalIndexGet; }

  // return a string for operator name
  const char *SzId() const override { return "CLogicalIndexGet"; }

  // distribution columns
  virtual const CColRefSet *PcrsDist() const { return m_pcrsDist; }

  // array of output columns
  CColRefArray *PdrgpcrOutput() const { return m_pdrgpcrOutput; }

  // origin operator id -- UINT32_MAX if operator was not generated via a transformation
  uint32_t UlOriginOpId() const { return m_ulOriginOpId; }

  // index name
  const CName &Name() const { return m_pindexdesc->Name(); }

  // table alias name
  const CName &NameAlias() const { return *m_pnameAlias; }

  // index descriptor
  CIndexDescriptor *Pindexdesc() const { return m_pindexdesc; }

  // table descriptor
  CTableDescriptor *Ptabdesc() const { return m_ptabdesc; }

  // order spec
  COrderSpec *Pos() const { return m_pos; }

  // number of predicate not applicable on the index
  uint32_t ResidualPredicateSize() const { return m_ulUnindexedPredColCount; }

  // index scan direction is only used for B-tree indices.
  EIndexScanDirection ScanDirection() const { return m_scan_direction; }

  // operator specific hash function
  uint32_t HashValue() const override;

  // match function
  bool Matches(COperator *pop) const override;

  // sensitivity to order of inputs
  bool FInputOrderSensitive() const override;

  // return a copy of the operator with remapped columns
  COperator *PopCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, bool must_exist) override;

  //-------------------------------------------------------------------------------------
  // Derived Relational Properties
  //-------------------------------------------------------------------------------------

  // derive output columns
  CColRefSet *DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl) override;

  // derive outer references
  CColRefSet *DeriveOuterReferences(CMemoryPool *mp, CExpressionHandle &exprhdl) override;

  // derive partition consumer info
  CPartInfo *DerivePartitionInfo(CMemoryPool *mp,
                                 CExpressionHandle &  // exprhdl
  ) const override {
    return GPOS_NEW(mp) CPartInfo(mp);
  }

  // derive constraint property
  CPropConstraint *DerivePropertyConstraint(CMemoryPool *mp,
                                            CExpressionHandle &  // exprhdl
  ) const override {
    return PpcDeriveConstraintFromTable(mp, m_ptabdesc, m_pdrgpcrOutput);
  }

  // derive key collections
  CKeyCollection *DeriveKeyCollection(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

  // derive join depth
  uint32_t DeriveJoinDepth(CMemoryPool *,       // mp
                           CExpressionHandle &  // exprhdl
  ) const override {
    return 1;
  }

  //-------------------------------------------------------------------------------------
  // Required Relational Properties
  //-------------------------------------------------------------------------------------

  // compute required stat columns of the n-th child
  CColRefSet *PcrsStat(CMemoryPool *mp,
                       CExpressionHandle &,  // exprhdl
                       CColRefSet *,         // pcrsInput
                       uint32_t              // child_index
  ) const override {
    // TODO:  March 26 2012; statistics derivation for indexes
    return GPOS_NEW(mp) CColRefSet(mp);
  }

  // derive statistics
  IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl, IStatisticsArray *stats_ctxt) const override;

  CTableDescriptorHashSet *DeriveTableDescriptor(CMemoryPool *mp,
                                                 CExpressionHandle &exprhdl GPOS_UNUSED) const override {
    CTableDescriptorHashSet *result = GPOS_NEW(mp) CTableDescriptorHashSet(mp);
    if (result->Insert(m_ptabdesc)) {
      m_ptabdesc->AddRef();
    }
    return result;
  }

  //-------------------------------------------------------------------------------------
  // Transformations
  //-------------------------------------------------------------------------------------

  // candidate set of xforms
  CXformSet *PxfsCandidates(CMemoryPool *mp) const override;

  // stat promise
  EStatPromise Esp(CExpressionHandle &) const override { return CLogical::EspLow; }

  //-------------------------------------------------------------------------------------
  // conversion function
  //-------------------------------------------------------------------------------------

  static CLogicalIndexGet *PopConvert(COperator *pop) {
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(EopLogicalIndexGet == pop->Eopid());

    return dynamic_cast<CLogicalIndexGet *>(pop);
  }

  // debug print
  IOstream &OsPrint(IOstream &) const override;

};  // class CLogicalIndexGet

}  // namespace gpopt

#endif  // !GPOPT_CLogicalIndexGet_H

// EOF
