//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarFuncExpr.h
//
//	@doc:
//		Class for representing DXL scalar FuncExpr
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarFuncExpr_H
#define GPDXL_CDXLScalarFuncExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl {
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarFuncExpr
//
//	@doc:
//		Class for representing DXL scalar FuncExpr
//
//---------------------------------------------------------------------------
class CDXLScalarFuncExpr : public CDXLScalar {
 private:
  // catalog id of the function
  IMDId *m_func_mdid;

  // return type
  IMDId *m_return_type_mdid;

  const int32_t m_return_type_modifier;

  // does the func return a set
  bool m_returns_set;

  //  true if in the function, variadic arguments have been
  //  combined into an array last argument
  bool m_funcvariadic;

 public:
  CDXLScalarFuncExpr(const CDXLScalarFuncExpr &) = delete;

  // ctor
  CDXLScalarFuncExpr(CMemoryPool *mp, IMDId *mdid_func, IMDId *mdid_return_type, int32_t return_type_modifier,
                     bool returns_set, bool funcvariadic);

  // dtor
  ~CDXLScalarFuncExpr() override;

  // ident accessors
  Edxlopid GetDXLOperator() const override;

  // name of the DXL operator
  const CWStringConst *GetOpNameStr() const override;

  // function id
  IMDId *FuncMdId() const;

  // return type
  IMDId *ReturnTypeMdId() const;

  int32_t TypeModifier() const;

  // does function return a set
  bool ReturnsSet() const;

  // Is the variadic flag set
  bool IsFuncVariadic() const;

  // serialize operator in DXL format

  // conversion function
  static CDXLScalarFuncExpr *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopScalarFuncExpr == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLScalarFuncExpr *>(dxl_op);
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

#endif  // !GPDXL_CDXLScalarFuncExpr_H

// EOF
