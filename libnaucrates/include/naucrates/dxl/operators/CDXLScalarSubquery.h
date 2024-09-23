//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubquery.h
//
//	@doc:
//		Class for representing subqueries computing scalar values
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSubquery_H
#define GPDXL_CDXLScalarSubquery_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl {
//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarSubquery
//
//	@doc:
//		Class for representing subqueries computing scalar values
//
//---------------------------------------------------------------------------
class CDXLScalarSubquery : public CDXLScalar {
 private:
  // id of column computed by the subquery
  uint32_t m_colid;

 public:
  CDXLScalarSubquery(CDXLScalarSubquery &) = delete;

  // ctor/dtor
  CDXLScalarSubquery(CMemoryPool *mp, uint32_t colid);

  ~CDXLScalarSubquery() override;

  // ident accessors
  Edxlopid GetDXLOperator() const override;

  // colid of subquery column
  uint32_t GetColId() const { return m_colid; }

  // name of the operator
  const CWStringConst *GetOpNameStr() const override;

  // serialize operator in DXL format

  // conversion function
  static CDXLScalarSubquery *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopScalarSubquery == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLScalarSubquery *>(dxl_op);
  }

  // does the operator return a boolean result
  bool HasBoolResult(CMDAccessor *  // md_accessor
  ) const override {
    return true;
  }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *dxlnode, bool validate_children) const override;
#endif  // GPOS_DEBUG
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLScalarSubquery_H

// EOF
