//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSortCol.h
//
//	@doc:
//		Class for representing sorting column info in DXL Sort and Motion nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSortCol_H
#define GPDXL_CDXLScalarSortCol_H

#include "gpos/base.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl {
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarSortCol
//
//	@doc:
//		Sorting column info in DXL Sort and Motion nodes
//
//---------------------------------------------------------------------------
class CDXLScalarSortCol : public CDXLScalar {
 private:
  // id of the sorting column
  uint32_t m_colid;

  // catalog Oid of the sorting operator
  IMDId *m_mdid_sort_op;

  // name of sorting operator
  CWStringConst *m_sort_op_name_str;

  // sort nulls before other values
  bool m_must_sort_nulls_first;

 public:
  CDXLScalarSortCol(CDXLScalarSortCol &) = delete;

  // ctor/dtor
  CDXLScalarSortCol(CMemoryPool *mp, uint32_t colid, IMDId *sort_op_id, CWStringConst *pstrTypeName,
                    bool fSortNullsFirst);

  ~CDXLScalarSortCol() override;

  // ident accessors
  Edxlopid GetDXLOperator() const override;

  // name of the operator
  const CWStringConst *GetOpNameStr() const override;

  // Id of the sorting column
  uint32_t GetColId() const;

  // mdid of the sorting operator
  IMDId *GetMdIdSortOp() const;

  // whether nulls are sorted before other values
  bool IsSortedNullsFirst() const;

  // serialize operator in DXL format

  // conversion function
  static CDXLScalarSortCol *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopScalarSortCol == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLScalarSortCol *>(dxl_op);
  }

  // does the operator return a boolean result
  bool HasBoolResult(CMDAccessor *  // md_accessor
  ) const override {
    GPOS_ASSERT(!"Invalid function call for this operator");
    return false;
  }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *dxlnode, bool validate_children) const override;
#endif  // GPOS_DEBUG
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLScalarSortCol_H

// EOF
