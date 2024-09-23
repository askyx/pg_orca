//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarWindowRef.h
//
//	@doc:
//		Class for representing DXL scalar WindowRef
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarWindowRef_H
#define GPDXL_CDXLScalarWindowRef_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl {
using namespace gpos;
using namespace gpmd;

// stage of the evaluation of the window function
enum EdxlWinStage { EdxlwinstageImmediate = 0, EdxlwinstagePreliminary, EdxlwinstageRowKey, EdxlwinstageSentinel };

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarWindowRef
//
//	@doc:
//		Class for representing DXL scalar WindowRef
//
//---------------------------------------------------------------------------
class CDXLScalarWindowRef : public CDXLScalar {
 private:
  // catalog id of the function
  IMDId *m_func_mdid;

  // return type
  IMDId *m_return_type_mdid;

  // denotes whether it's agg(DISTINCT ...)
  bool m_is_distinct;

  // is argument list really '*' //
  bool m_is_star_arg;

  // is function a simple aggregate? //
  bool m_is_simple_agg;

  // denotes the win stage
  EdxlWinStage m_dxl_win_stage;

  // position the window specification in a parent window operator
  uint32_t m_win_spec_pos;

 public:
  CDXLScalarWindowRef(const CDXLScalarWindowRef &) = delete;

  // ctor
  CDXLScalarWindowRef(CMemoryPool *mp, IMDId *pmdidWinfunc, IMDId *mdid_return_type, bool is_distinct, bool is_star_arg,
                      bool is_simple_agg, EdxlWinStage dxl_win_stage, uint32_t ulWinspecPosition);

  // dtor
  ~CDXLScalarWindowRef() override;

  // ident accessors
  Edxlopid GetDXLOperator() const override;

  // name of the DXL operator
  const CWStringConst *GetOpNameStr() const override;

  // catalog id of the function
  IMDId *FuncMdId() const { return m_func_mdid; }

  // return type of the function
  IMDId *ReturnTypeMdId() const { return m_return_type_mdid; }

  // window stage
  EdxlWinStage GetDxlWinStage() const { return m_dxl_win_stage; }

  // denotes whether it's agg(DISTINCT ...)
  bool IsDistinct() const { return m_is_distinct; }

  bool IsStarArg() const { return m_is_star_arg; }

  bool IsSimpleAgg() const { return m_is_simple_agg; }

  // position the window specification in a parent window operator
  uint32_t GetWindSpecPos() const { return m_win_spec_pos; }

  // set window spec position
  void SetWinSpecPos(uint32_t win_spec_pos) { m_win_spec_pos = win_spec_pos; }

  // string representation of win stage
  const CWStringConst *GetWindStageStr() const;

  // serialize operator in DXL format

  // conversion function
  static CDXLScalarWindowRef *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopScalarWindowRef == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLScalarWindowRef *>(dxl_op);
  }

  // does the operator return a boolean result
  bool HasBoolResult(CMDAccessor *md_accessor) const override;

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *dxlnode, bool validate_children) const override;
#endif  // GPOS_DEBUG
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLScalarWindowRef_H

// EOF
