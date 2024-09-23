//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarArrayRef.h
//
//	@doc:
//		Class for representing DXL scalar arrayrefs
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarArrayRef_H
#define GPDXL_CDXLScalarArrayRef_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl {
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarArrayRef
//
//	@doc:
//		Class for representing DXL scalar arrayrefs
//
//---------------------------------------------------------------------------
class CDXLScalarArrayRef : public CDXLScalar {
 private:
  // base element type id
  IMDId *m_elem_type_mdid;

  // element type modifier
  int32_t m_type_modifier;

  // array type id
  IMDId *m_array_type_mdid;

  // return type id
  IMDId *m_return_type_mdid;

 public:
  CDXLScalarArrayRef(const CDXLScalarArrayRef &) = delete;

  // ctor
  CDXLScalarArrayRef(CMemoryPool *mp, IMDId *elem_type_mdid, int32_t type_modifier, IMDId *array_type_mdid,
                     IMDId *return_type_mdid);

  // dtor
  ~CDXLScalarArrayRef() override;

  // ident accessors
  Edxlopid GetDXLOperator() const override;

  // operator name
  const CWStringConst *GetOpNameStr() const override;

  // element type id
  IMDId *ElementTypeMDid() const { return m_elem_type_mdid; }

  // element type modifier
  int32_t TypeModifier() const;

  // array type id
  IMDId *ArrayTypeMDid() const { return m_array_type_mdid; }

  // return type id
  IMDId *ReturnTypeMDid() const { return m_return_type_mdid; }

  // serialize operator in DXL format

  // does the operator return a boolean result
  bool HasBoolResult(CMDAccessor *md_accessor) const override;

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *dxlnode, bool validate_children) const override;
#endif  // GPOS_DEBUG

  // conversion function
  static CDXLScalarArrayRef *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopScalarArrayRef == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLScalarArrayRef *>(dxl_op);
  }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLScalarArrayRef_H

// EOF
