//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarProjElem.h
//
//	@doc:
//		Class for representing DXL projection lists.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarProjElem_H
#define GPDXL_CDXLScalarProjElem_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl {
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarProjElem
//
//	@doc:
//		Container for projection list elements, storing the expression and the alias
//
//---------------------------------------------------------------------------
class CDXLScalarProjElem : public CDXLScalar {
 private:
  // id of column defined by this project element:
  // for computed columns this is a new id, for colrefs: id of the original column
  uint32_t m_id;

  // alias
  const CMDName *m_mdname;

 public:
  CDXLScalarProjElem(CDXLScalarProjElem &) = delete;

  // ctor/dtor
  CDXLScalarProjElem(CMemoryPool *mp, uint32_t id, const CMDName *mdname);

  ~CDXLScalarProjElem() override;

  // ident accessors
  Edxlopid GetDXLOperator() const override;

  // name of the operator
  const CWStringConst *GetOpNameStr() const override;

  // id of the proj element
  uint32_t Id() const;

  // alias of the proj elem
  const CMDName *GetMdNameAlias() const;

  // serialize operator in DXL format

  // check if given column is defined by operator
  bool IsColDefined(uint32_t colid) const override { return (Id() == colid); }

  // conversion function
  static CDXLScalarProjElem *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopScalarProjectElem == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLScalarProjElem *>(dxl_op);
  }

  // does the operator return a boolean result
  bool HasBoolResult(CMDAccessor *  // md_accessor
  ) const override {
    GPOS_ASSERT(!"Invalid function call on a container operator");
    return false;
  }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure
  void AssertValid(const CDXLNode *dxlnode, bool validate_children) const override;
#endif  // GPOS_DEBUG
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLScalarProjElem_H

// EOF
