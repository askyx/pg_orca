//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalDelete.h
//
//	@doc:
//		Class for representing logical delete operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalDelete_H
#define GPDXL_CDXLLogicalDelete_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl {
using namespace gpmd;

// fwd decl
class CDXLTableDescr;

//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalDelete
//
//	@doc:
//		Class for representing logical delete operator
//
//---------------------------------------------------------------------------
class CDXLLogicalDelete : public CDXLLogical {
 private:
  // target table descriptor
  CDXLTableDescr *m_dxl_table_descr;

  // ctid column id
  uint32_t m_ctid_colid;

  // segmentId column id
  uint32_t m_segid_colid;

  // list of deletion column ids
  ULongPtrArray *m_deletion_colid_array;

 public:
  CDXLLogicalDelete(const CDXLLogicalDelete &) = delete;

  // ctor
  CDXLLogicalDelete(CMemoryPool *mp, CDXLTableDescr *table_descr, uint32_t ctid_colid, uint32_t segid_colid,
                    ULongPtrArray *delete_colid_array);

  // dtor
  ~CDXLLogicalDelete() override;

  // operator type
  Edxlopid GetDXLOperator() const override;

  // operator name
  const CWStringConst *GetOpNameStr() const override;

  // target table descriptor
  CDXLTableDescr *GetDXLTableDescr() const { return m_dxl_table_descr; }

  // ctid column
  uint32_t GetCtIdColId() const { return m_ctid_colid; }

  // segment id column
  uint32_t GetSegmentIdColId() const { return m_segid_colid; }

  // deletion column ids
  ULongPtrArray *GetDeletionColIdArray() const { return m_deletion_colid_array; }

#ifdef GPOS_DEBUG
  // checks whether the operator has valid structure, i.e. number and
  // types of child nodes
  void AssertValid(const CDXLNode *node, bool validate_children) const override;
#endif  // GPOS_DEBUG

  // serialize operator in DXL format

  // conversion function
  static CDXLLogicalDelete *Cast(CDXLOperator *dxl_op) {
    GPOS_ASSERT(nullptr != dxl_op);
    GPOS_ASSERT(EdxlopLogicalDelete == dxl_op->GetDXLOperator());

    return dynamic_cast<CDXLLogicalDelete *>(dxl_op);
  }
};
}  // namespace gpdxl

#endif  // !GPDXL_CDXLLogicalDelete_H

// EOF
