//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEAnchor.h
//
//	@doc:
//		Class for representing DXL logical CTE anchors
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEAnchor_H
#define GPDXL_CDXLLogicalCTEAnchor_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl {
//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalCTEAnchor
//
//	@doc:
//		Class for representing DXL logical CTE producers
//
//---------------------------------------------------------------------------
class CDXLLogicalCTEAnchor : public CDXLLogical {
 private:
  // cte id
  uint32_t m_id;

 public:
  CDXLLogicalCTEAnchor(CDXLLogicalCTEAnchor &) = delete;

  // ctor
  CDXLLogicalCTEAnchor(CMemoryPool *mp, uint32_t id);

  // operator type
  Edxlopid GetDXLOperator() const override;

  // operator name
  const CWStringConst *GetOpNameStr() const override;

  // cte identifier
  uint32_t Id() const { return m_id; }

  // serialize operator in DXL format

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *, bool validate_children) const override;
#endif  // GPOS_DEBUG

  // conversion function
  static CDXLLogicalCTEAnchor *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopLogicalCTEAnchor == dxl_op->GetDXLOperator());
    return dynamic_cast<CDXLLogicalCTEAnchor *>(dxl_op);
  }
};
}  // namespace gpdxl
#endif  // !GPDXL_CDXLLogicalCTEAnchor_H

// EOF
