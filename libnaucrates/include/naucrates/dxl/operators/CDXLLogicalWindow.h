//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalWindow.h
//
//	@doc:
//		Class for representing DXL logical window operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalWindow_H
#define GPDXL_CDXLLogicalWindow_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLWindowSpec.h"

namespace gpdxl {
//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalWindow
//
//	@doc:
//		Class for representing DXL logical window operators
//
//---------------------------------------------------------------------------
class CDXLLogicalWindow : public CDXLLogical {
 private:
  // array of window specifications
  CDXLWindowSpecArray *m_window_spec_array;

  // private copy ctor
  CDXLLogicalWindow(CDXLLogicalWindow &);

 public:
  // ctor
  CDXLLogicalWindow(CMemoryPool *mp, CDXLWindowSpecArray *pdrgpdxlwinspec);

  // dtor
  ~CDXLLogicalWindow() override;

  // accessors
  Edxlopid GetDXLOperator() const override;
  const CWStringConst *GetOpNameStr() const override;

  // number of window specs
  uint32_t NumOfWindowSpecs() const;

  // return the window key at a given position
  CDXLWindowSpec *GetWindowKeyAt(uint32_t idx) const;

  // serialize operator in DXL format

  // conversion function
  static CDXLLogicalWindow *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopLogicalWindow == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLLogicalWindow *>(dxl_op);
  }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *, bool validate_children) const override;
#endif  // GPOS_DEBUG
};
}  // namespace gpdxl
#endif  // !GPDXL_CDXLLogicalWindow_H

// EOF
