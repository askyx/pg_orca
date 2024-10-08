//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CWindowFrame.h
//
//	@doc:
//		Description of window frame
//---------------------------------------------------------------------------
#ifndef GPOPT_CWindowFrame_H
#define GPOPT_CWindowFrame_H

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CPropSpec.h"
#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

namespace gpopt {
// type definition of corresponding dynamic pointer array
class CWindowFrame;
using CWindowFrameArray = CDynamicPtrArray<CWindowFrame, CleanupRelease>;

using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CWindowFrame
//
//	@doc:
//		Description of window frame
//
//---------------------------------------------------------------------------
class CWindowFrame : public CRefCount {
 public:
  // specification method
  enum EFrameSpec {
    EfsRows,    // frame is specified using Rows construct
    EfsRange,   // frame is specified using Range construct
    EfsGroups,  // frame is specified using Groups construct

    EfsSentinel
  };

  // types of frame boundary
  enum EFrameBoundary {
    EfbUnboundedPreceding,       // boundary is unlimited preceding current row
    EfbBoundedPreceding,         // boundary is limited preceding current row
    EfbCurrentRow,               // boundary is set to current row
    EfbUnboundedFollowing,       // boundary is unlimited following current row
    EfbBoundedFollowing,         // boundary is limited following current row
    EfbDelayedBoundedPreceding,  // boundary is delayed preceding current row
    EfbDelayedBoundedFollowing,  // boundary is delayed following current row

    EfbSentinel
  };

  // possible exclusion strategies
  enum EFrameExclusionStrategy {
    EfesNone,            // no exclusion
    EfesNulls,           // exclude nulls
    EfesCurrentRow,      // exclude current row
    EfseMatchingOthers,  // exclude other rows matching current row
    EfesTies,            // exclude other matching rows and current row

    EfesSentinel
  };

 private:
  // specification method
  const EFrameSpec m_efs{EfsRange};

  // type of leading edge
  const EFrameBoundary m_efbLeading{EfbUnboundedPreceding};

  // type of trailing edge
  const EFrameBoundary m_efbTrailing{EfbCurrentRow};

  // scalar value of leading edge, memory owned by this class
  CExpression *m_pexprLeading{nullptr};

  // scalar value of trailing edge, memory owned by this class
  CExpression *m_pexprTrailing{nullptr};

  // exclusion strategy
  const EFrameExclusionStrategy m_efes{EfesNone};

  // columns used by frame edges
  CColRefSet *m_pcrsUsed{nullptr};

  // singelton empty frame -- used with any unspecified window function frame
  static const CWindowFrame m_wfEmpty;

  // in_range function for startOffset
  OID m_start_in_range_func;

  // in_range function for endOffset
  OID m_end_in_range_func;

  // collation for in_range tests
  OID m_in_range_coll;

  // use ASC sort order for in_range tests
  bool m_in_range_asc;

  // nulls sort first for in_range tests
  bool m_in_range_nulls_first;

  // private dummy ctor used to create empty frame
  CWindowFrame();

 public:
  CWindowFrame(const CWindowFrame &) = delete;

  // ctor
  CWindowFrame(CMemoryPool *mp, EFrameSpec efs, EFrameBoundary efbLeading, EFrameBoundary efbTrailing,
               CExpression *pexprLeading, CExpression *pexprTrailing, EFrameExclusionStrategy efes,
               OID start_in_range_func, OID end_in_range_func, OID in_range_coll, bool in_range_asc,
               bool in_range_nulls_first);

  // dtor
  ~CWindowFrame() override;

  // specification
  EFrameSpec Efs() const { return m_efs; }

  // type of leading edge
  EFrameBoundary EfbLeading() const { return m_efbLeading; }

  // type of trailing edge
  EFrameBoundary EfbTrailing() const { return m_efbTrailing; }

  // scalar value of leading edge
  CExpression *PexprLeading() const { return m_pexprLeading; }

  // scalar value of trailing edge
  CExpression *PexprTrailing() const { return m_pexprTrailing; }

  // exclusion strategy
  EFrameExclusionStrategy Efes() const { return m_efes; }

  // matching function
  bool Matches(const CWindowFrame *pwf) const;

  // hash function
  virtual uint32_t HashValue() const;

  // return a copy of the window frame with remapped columns
  virtual CWindowFrame *PwfCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, bool must_exist);

  // return columns used by frame edges
  CColRefSet *PcrsUsed() const { return m_pcrsUsed; }

  // print
  IOstream &OsPrint(IOstream &os) const;

  // matching function over frame arrays
  static bool Equals(const CWindowFrameArray *pdrgpwfFirst, const CWindowFrameArray *pdrgpwfSecond);

  // combine hash values of a maximum number of entries
  static uint32_t HashValue(const CWindowFrameArray *pdrgpwfFirst, uint32_t ulMaxSize);

  // print array of window frame objects
  static IOstream &OsPrint(IOstream &os, const CWindowFrameArray *pdrgpwf);

  // check if a given window frame is empty
  static bool IsEmpty(CWindowFrame *pwf) { return pwf == &m_wfEmpty; }

  // return pointer to singleton empty window frame
  static const CWindowFrame *PwfEmpty() { return &m_wfEmpty; }

  OID StartInRangeFunc() const { return m_start_in_range_func; }

  OID EndInRangeFunc() const { return m_end_in_range_func; }

  OID InRangeColl() const { return m_in_range_coll; }

  bool InRangeAsc() const { return m_in_range_asc; }

  bool InRangeNullsFirst() const { return m_in_range_nulls_first; }

};  // class CWindowFrame

}  // namespace gpopt

#endif  // !GPOPT_CWindowFrame_H

// EOF
