//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalAgg.h
//
//	@doc:
//		Class for representing DXL aggregate operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalAgg_H
#define GPDXL_CDXLPhysicalAgg_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl {
// indices of group by elements in the children array
enum Edxlagg { EdxlaggIndexProjList = 0, EdxlaggIndexFilter, EdxlaggIndexChild, EdxlaggIndexSentinel };

enum EdxlAggStrategy { EdxlaggstrategyPlain, EdxlaggstrategySorted, EdxlaggstrategyHashed, EdxlaggstrategySentinel };

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalAgg
//
//	@doc:
//		Class for representing DXL aggregate operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalAgg : public CDXLPhysical {
 private:
  // private copy ctor
  CDXLPhysicalAgg(const CDXLPhysicalAgg &);

  // grouping column ids
  ULongPtrArray *m_grouping_colids_array;

  EdxlAggStrategy m_dxl_agg_strategy;

  // is it safe to stream the local hash aggregate
  bool m_stream_safe;

 public:
  // ctor
  CDXLPhysicalAgg(CMemoryPool *mp, EdxlAggStrategy dxl_agg_strategy, bool stream_safe);

  // dtor
  ~CDXLPhysicalAgg() override;

  // accessors
  Edxlopid GetDXLOperator() const override;
  EdxlAggStrategy GetAggStrategy() const;

  const CWStringConst *GetOpNameStr() const override;
  const CWStringConst *GetAggStrategyNameStr() const;
  const CWStringConst *PstrAggLevel() const;
  const ULongPtrArray *GetGroupingColidArray() const;

  // set grouping column indices
  void SetGroupingCols(ULongPtrArray *);

  // is aggregate a hash aggregate that it safe to stream
  bool IsStreamSafe() const { return (EdxlaggstrategyHashed == m_dxl_agg_strategy) && m_stream_safe; }

  // serialize operator in DXL format

  // conversion function
  static CDXLPhysicalAgg *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopPhysicalAgg == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLPhysicalAgg *>(dxl_op);
  }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *, bool validate_children) const override;
#endif  // GPOS_DEBUG
};
}  // namespace gpdxl
#endif  // !GPDXL_CDXLPhysicalAgg_H

// EOF
